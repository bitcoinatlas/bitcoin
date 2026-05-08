import type { Codec } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { Uint8ArrayView } from "~/lib/Uint8ArrayView.ts";
import { Uint8ArrayMap } from "../Uint8ArrayMap.ts";
import { readFileFull, writeFileFull } from "../utils/fs.ts";
import type { Transaction, Transactionable } from "./Store.ts";

export interface FixedKVStoreOptions {
	blockSize?: number;
	blockCacheSize?: number;
}

interface BlockMetadata {
	startKey: Uint8Array;
	endKey: Uint8Array;
	offset: number;
	size: number;
	entryCount: number;
}

interface SSTMetadata {
	blocks: BlockMetadata[];
	bloomFilter: Uint8Array;
	totalEntries: number;
	fileSize: number;
}

export class FixedKVStoreTransaction<K, V> implements Transaction {
	private readonly staged: Uint8ArrayMap<Uint8Array>;
	private committed = false;

	constructor(private readonly store: FixedKVStore<K, V>) {
		// 20_000 buckets — keeps chains short for typical batch sizes up to ~100k
		this.staged = new Uint8ArrayMap(20_000);
	}

	set(key: K, value: V): void {
		const k = this.store.encodeKey(key);
		const v = this.store.encodeValue(value);
		this.staged.set(k, v);
	}

	async get(key: K): Promise<V | undefined> {
		const k = this.store.encodeKey(key);
		const staged = this.staged.get(k);
		if (staged !== undefined) return this.store.decodeValue(staged);
		return this.store.get(key);
	}

	async getMany(keys: K[]): Promise<(V | undefined)[]> {
		const results: (V | undefined)[] = new Array(keys.length);
		const passthrough: number[] = [];

		for (let i = 0; i < keys.length; i++) {
			const k = this.store.encodeKey(keys[i]!);
			const staged = this.staged.get(k);
			if (staged !== undefined) {
				results[i] = this.store.decodeValue(staged);
			} else {
				passthrough.push(i);
			}
		}

		if (passthrough.length > 0) {
			const passthroughKeys = passthrough.map((i) => keys[i]!);
			const fromStore = await this.store.getMany(passthroughKeys);
			for (let i = 0; i < passthrough.length; i++) {
				results[passthrough[i]!] = fromStore[i];
			}
		}

		return results;
	}

	async commit(): Promise<void> {
		if (this.committed) throw new Error("Transaction already committed");
		await this.store.writeWal(this.staged);
		this.committed = true;
	}

	rollback(): void {
		this.staged.clear();
		this.store.releaseTransaction();
	}
}

/**
 * LSM-based store for fixed-size KV pairs. Mutations via transactions only.
 * Crash safety: commit() writes WAL atomically; finalize() flushes WAL→SST then
 * deletes it. If power dies mid-finalize, WAL replays on next open.
 */
export class FixedKVStore<K, V> implements Transactionable {
	private readonly filepath: string;
	private readonly walPath: string;
	private readonly walTmpPath: string;
	private file: Deno.FsFile | undefined;
	readonly keyCodec: Codec<K>;
	readonly valueCodec: Codec<V>;

	private readonly blockSize: number;
	private readonly maxCacheSize: number;

	private sstFiles: SSTMetadata[] = [];
	private fileOffset = 0;

	private blockCache = new Map<number, { data: Uint8Array }>();

	private activeTx: FixedKVStoreTransaction<K, V> | null = null;

	constructor(
		filepath: string,
		codecs: readonly [Codec<K>, Codec<V>],
		options: FixedKVStoreOptions = {},
	) {
		[this.keyCodec, this.valueCodec] = codecs;

		if (this.keyCodec.stride < 0) throw new Error("Key codec must have fixed stride (>= 0)");
		if (this.valueCodec.stride < 0) throw new Error("Value codec must have fixed stride (>= 0)");

		this.filepath = filepath;
		this.walPath = filepath + ".wal";
		this.walTmpPath = filepath + ".wal.tmp";
		this.blockSize = options.blockSize ?? 65536;
		this.maxCacheSize = options.blockCacheSize ?? 1000;
	}

	transaction(): FixedKVStoreTransaction<K, V> {
		if (this.activeTx !== null) throw new Error("A transaction is already open");
		this.activeTx = new FixedKVStoreTransaction(this);
		return this.activeTx;
	}

	// Flush WAL→SST then delete WAL. Idempotent. WAL survives crashes → safe to replay.
	async finalize(): Promise<void> {
		await this.ensureFile();
		this.activeTx = null;

		if (!await exists(this.walPath)) return;

		const staged = await this.readWal();
		if (staged.size > 0) await this.flushEntries(staged);
		await this.deleteWal();
	}

	async prepare(): Promise<void> {
		await this.ensureFile();
	}

	async get(key: K): Promise<V | undefined> {
		await this.ensureFile();
		return this.getByBytes(this.encodeKey(key));
	}

	async getMany(keys: K[]): Promise<(V | undefined)[]> {
		await this.ensureFile();
		return this.getManyByBytes(keys.map((k) => this.encodeKey(k)));
	}

	encodeKey(key: K): Uint8Array {
		return this.keyCodec.encode(key);
	}
	encodeValue(value: V): Uint8Array {
		return this.valueCodec.encode(value);
	}
	decodeValue(bytes: Uint8Array): V {
		return this.valueCodec.decode(bytes)[0];
	}
	releaseTransaction(): void {
		this.activeTx = null;
	}

	// WAL: write atomically via tmp→rename. Crash before rename = no .wal = no corruption.
	async writeWal(staged: Uint8ArrayMap<Uint8Array>): Promise<void> {
		const entries = staged.entries().toArray();

		// 4 bytes count + N * (keyStride + valueStride)
		const entrySize = this.keyCodec.stride + this.valueCodec.stride;
		const buf = new Uint8Array(4 + entries.length * entrySize);
		const view = new DataView(buf.buffer);
		view.setUint32(0, entries.length, true);

		let pos = 4;
		for (const [k, v] of entries) {
			buf.set(k, pos);
			pos += this.keyCodec.stride;
			buf.set(v, pos);
			pos += this.valueCodec.stride;
		}

		await Deno.writeFile(this.walTmpPath, buf);
		await Deno.rename(this.walTmpPath, this.walPath);
	}

	private async readWal(): Promise<Uint8ArrayMap<Uint8Array>> {
		const buf = await Deno.readFile(this.walPath);
		const view = new DataView(buf.buffer);
		const count = view.getUint32(0, true);

		const staged = new Uint8ArrayMap<Uint8Array>(count * 2);
		let pos = 4;
		for (let i = 0; i < count; i++) {
			const k = new Uint8Array(buf.subarray(pos, pos + this.keyCodec.stride));
			pos += this.keyCodec.stride;
			const v = new Uint8Array(buf.subarray(pos, pos + this.valueCodec.stride));
			pos += this.valueCodec.stride;
			staged.set(k, v);
		}
		return staged;
	}

	private async deleteWal(): Promise<void> {
		await Deno.remove(this.walPath).catch(() => {});
	}

	private async flushEntries(staged: Uint8ArrayMap<Uint8Array>): Promise<void> {
		const file = await this.ensureFile();

		const entries: Array<[Uint8Array, Uint8Array]> = staged.entries().toArray();
		if (entries.length === 0) return;

		entries.sort(([a], [b]) => this.compareKeys(a, b));

		const bloomFilter = this.buildBloomFilter(entries);
		const sstDataStart = this.fileOffset;

		const blocks: BlockMetadata[] = [];
		let dataOffset = sstDataStart;
		let blockBufferPos = 0;
		let blockEntryCount = 0;
		let blockStartKey: Uint8Array | null = null;

		// SST entry layout: [key(K)][value(V)]
		const entrySize = this.keyCodec.stride + this.valueCodec.stride;
		const blockBuffer = new Uint8Array(this.blockSize);

		for (let i = 0; i < entries.length; i++) {
			const [key, value] = entries[i]!;

			if (blockStartKey === null) blockStartKey = key;

			blockBuffer.set(key, blockBufferPos);
			blockBuffer.set(value, blockBufferPos + this.keyCodec.stride);
			blockBufferPos += entrySize;
			blockEntryCount++;

			const flush = blockBufferPos + entrySize > this.blockSize || i === entries.length - 1;

			if (flush) {
				const slice = blockBuffer.subarray(0, blockBufferPos);
				await file.seek(dataOffset, Deno.SeekMode.Start);
				await writeFileFull(file, slice);

				blocks.push({
					startKey: new Uint8Array(blockStartKey),
					endKey: new Uint8Array(key),
					offset: dataOffset - sstDataStart,
					size: blockBufferPos,
					entryCount: blockEntryCount,
				});

				dataOffset += blockBufferPos;
				blockBufferPos = 0;
				blockEntryCount = 0;
				blockStartKey = null;
			}
		}

		const metadata: SSTMetadata = {
			blocks,
			bloomFilter,
			totalEntries: entries.length,
			fileSize: dataOffset - sstDataStart,
		};

		const metadataBytes = this.encodeMetadata(metadata);
		await file.seek(dataOffset, Deno.SeekMode.Start);
		await writeFileFull(file, metadataBytes);

		this.fileOffset = dataOffset + metadataBytes.length;

		for (const block of metadata.blocks) {
			block.offset = sstDataStart + block.offset;
		}

		this.sstFiles.push(metadata);
	}

	private async getByBytes(keyBytes: Uint8Array): Promise<V | undefined> {
		const sstIndex = this.findSst(keyBytes);
		if (sstIndex === -1) return undefined;

		const sst = this.sstFiles[sstIndex]!;
		if (!this.mightContain(sst.bloomFilter, keyBytes)) return undefined;

		const blockIdx = this.findBlock(sst.blocks, keyBytes);
		if (blockIdx === -1) return undefined;

		const blockData = await this.readBlock(sstIndex, blockIdx);
		const valueBytes = this.binarySearchInBlock(blockData, keyBytes, sst.blocks[blockIdx]!.entryCount);
		if (valueBytes === undefined) return undefined;
		return this.valueCodec.decode(valueBytes)[0];
	}

	private async getManyByBytes(keyBytes: Uint8Array[]): Promise<(V | undefined)[]> {
		await this.ensureFile();
		const results: (V | undefined)[] = new Array(keyBytes.length).fill(undefined);
		const pendingByBlock = new Map<number, Array<{ keyIndex: number; key: Uint8Array }>>();

		for (let ki = 0; ki < keyBytes.length; ki++) {
			const key = keyBytes[ki]!;
			const sstIndex = this.findSst(key);
			if (sstIndex === -1) continue;

			const sst = this.sstFiles[sstIndex]!;
			if (!this.mightContain(sst.bloomFilter, key)) continue;

			const blockIdx = this.findBlock(sst.blocks, key);
			if (blockIdx === -1) continue;

			const cacheKey = (sstIndex << 20) | blockIdx;
			if (!pendingByBlock.has(cacheKey)) pendingByBlock.set(cacheKey, []);
			pendingByBlock.get(cacheKey)!.push({ keyIndex: ki, key });
		}

		for (const [cacheKey, pending] of pendingByBlock) {
			const si = cacheKey >>> 20;
			const blockIdx = cacheKey & 0xFFFFF;
			const block = this.sstFiles[si]!.blocks[blockIdx]!;
			const blockData = await this.readBlock(si, blockIdx);

			for (const { keyIndex, key } of pending) {
				const valueBytes = this.binarySearchInBlock(blockData, key, block.entryCount);
				if (valueBytes !== undefined) {
					results[keyIndex] = this.valueCodec.decode(valueBytes)[0];
				}
			}
		}

		return results;
	}

	private async readBlock(sstIndex: number, blockIdx: number): Promise<Uint8Array> {
		const cacheKey = (sstIndex << 20) | blockIdx;
		const cached = this.blockCache.get(cacheKey);
		if (cached) {
			// Promote to MRU by reinserting
			this.blockCache.delete(cacheKey);
			this.blockCache.set(cacheKey, cached);
			return cached.data;
		}

		const file = await this.ensureFile();
		const block = this.sstFiles[sstIndex]!.blocks[blockIdx]!;
		await file.seek(block.offset, Deno.SeekMode.Start);
		const data = new Uint8Array(block.size);
		await readFileFull(file, data);
		this.addToCache(cacheKey, data);
		return data;
	}

	private preparePromise: Promise<Deno.FsFile> | null = null;
	private async ensureFile(): Promise<Deno.FsFile> {
		if (this.file) return this.file;
		if (this.preparePromise) return this.preparePromise;

		return this.preparePromise = (async () => {
			try {
				this.file = await Deno.open(this.filepath, { create: true, read: true, write: true });
				const stat = await this.file.stat();
				if (stat.size > 0) await this.loadExistingData();
				return this.file;
			} catch (err) {
				this.preparePromise = null;
				throw err;
			}
		})();
	}

	async close(): Promise<void> {
		this.file?.close();
		this.file = undefined;
		this.preparePromise = null;
	}

	private async loadExistingData(): Promise<void> {
		const stat = await this.file!.stat();
		if (stat.size === 0) {
			this.fileOffset = 0;
			return;
		}

		const fileData = new Uint8Array(stat.size);
		await this.file!.seek(0, Deno.SeekMode.Start);
		await readFileFull(this.file!, fileData);

		// Layout: [SST data][SST metadata]... repeated. Metadata starts with
		// [u32 totalSize][u32 magic]. Scan byte-by-byte until magic found, then jump.
		let pos = 0;
		while (pos + 8 <= stat.size) {
			const view = new DataView(fileData.buffer, pos, 8);
			const metadataSize = view.getUint32(0, true);
			const magic = view.getUint32(4, true);

			if (magic === 0x524F434B && metadataSize > 8 && pos + metadataSize <= stat.size) {
				const sst = this.decodeMetadata(fileData.subarray(pos, pos + metadataSize));
				const dataStart = pos - sst.fileSize;
				for (const block of sst.blocks) block.offset = dataStart + block.offset;
				this.sstFiles.push(sst);
				pos += metadataSize;
			} else {
				pos++;
			}
		}

		this.fileOffset = stat.size;
	}

	private findSst(keyBytes: Uint8Array): number {
		for (let i = this.sstFiles.length - 1; i >= 0; i--) {
			const sst = this.sstFiles[i]!;
			const startKey = sst.blocks[0]!.startKey;
			const endKey = sst.blocks[sst.blocks.length - 1]!.endKey;
			if (this.compareKeys(keyBytes, startKey) >= 0 && this.compareKeys(keyBytes, endKey) <= 0) return i;
		}
		return -1;
	}

	private binarySearchInBlock(data: Uint8Array, searchKey: Uint8Array, entryCount: number): Uint8Array | undefined {
		// Entry layout: [key][value]
		const entrySize = this.keyCodec.stride + this.valueCodec.stride;
		let left = 0, right = entryCount - 1;
		while (left <= right) {
			const mid = (left + right) >> 1;
			const offset = mid * entrySize;
			const key = data.subarray(offset, offset + this.keyCodec.stride);
			const cmp = this.compareKeys(key, searchKey);
			if (cmp === 0) {
				return data.subarray(offset + this.keyCodec.stride, offset + entrySize);
			} else if (cmp < 0) left = mid + 1;
			else right = mid - 1;
		}
		return undefined;
	}

	private findBlock(blocks: BlockMetadata[], key: Uint8Array): number {
		let left = 0, right = blocks.length - 1;
		while (left <= right) {
			const mid = (left + right) >> 1;
			const block = blocks[mid]!;
			if (this.compareKeys(key, block.startKey) < 0) right = mid - 1;
			else if (this.compareKeys(key, block.endKey) > 0) left = mid + 1;
			else return mid;
		}
		return -1;
	}

	private addToCache(key: number, data: Uint8Array): void {
		if (this.blockCache.size >= this.maxCacheSize) {
			// Map insertion order = LRU: first key is oldest
			this.blockCache.delete(this.blockCache.keys().next().value!);
		}
		this.blockCache.set(key, { data: new Uint8Array(data) });
	}

	private buildBloomFilter(entries: Array<[Uint8Array, Uint8Array]>): Uint8Array {
		const bits = entries.length * 8;
		const filter = new Uint8Array(Math.ceil(bits / 8));
		for (const [key] of entries) {
			const h1 = this.hashKey(key, 0), h2 = this.hashKey(key, 1);
			filter[Math.floor((h1 % bits) / 8)]! |= 1 << ((h1 % bits) % 8);
			filter[Math.floor((h2 % bits) / 8)]! |= 1 << ((h2 % bits) % 8);
		}
		return filter;
	}

	private mightContain(filter: Uint8Array, key: Uint8Array): boolean {
		const bits = filter.length * 8;
		const h1 = this.hashKey(key, 0), h2 = this.hashKey(key, 1);
		return (filter[Math.floor((h1 % bits) / 8)]! & (1 << ((h1 % bits) % 8))) !== 0 &&
			(filter[Math.floor((h2 % bits) / 8)]! & (1 << ((h2 % bits) % 8))) !== 0;
	}

	private encodeMetadata(metadata: SSTMetadata): Uint8Array {
		const blockFieldSize = 8 + this.keyCodec.stride * 2 + 8 + 4;
		const totalSize = 8 + 4 + 4 + metadata.blocks.length * blockFieldSize + metadata.bloomFilter.length + 4;
		const buffer = new Uint8Array(totalSize);
		const view = new DataView(buffer.buffer);
		let pos = 0;

		view.setUint32(pos, totalSize, true);
		pos += 4;
		view.setUint32(pos, 0x524F434B, true);
		pos += 4;
		view.setUint32(pos, metadata.bloomFilter.length, true);
		pos += 4;
		buffer.set(metadata.bloomFilter, pos);
		pos += metadata.bloomFilter.length;
		view.setUint32(pos, metadata.totalEntries, true);
		pos += 4;
		view.setUint32(pos, metadata.fileSize, true);
		pos += 4;
		view.setUint32(pos, metadata.blocks.length, true);
		pos += 4;

		for (const block of metadata.blocks) {
			buffer.set(block.startKey, pos);
			pos += this.keyCodec.stride;
			buffer.set(block.endKey, pos);
			pos += this.keyCodec.stride;
			view.setBigUint64(pos, BigInt(block.offset), true);
			pos += 8;
			view.setUint32(pos, block.size, true);
			pos += 4;
			view.setUint32(pos, block.entryCount, true);
			pos += 4;
		}

		return buffer;
	}

	private decodeMetadata(data: Uint8Array): SSTMetadata {
		const view = new Uint8ArrayView(data);
		let pos = 8;

		const bloomSize = view.getUint32(pos, true);
		pos += 4;
		const bloomFilter = new Uint8Array(data.subarray(pos, pos + bloomSize));
		pos += bloomSize;
		const totalEntries = view.getUint32(pos, true);
		pos += 4;
		const fileSize = view.getUint32(pos, true);
		pos += 4;
		const blockCount = view.getUint32(pos, true);
		pos += 4;

		const blocks: BlockMetadata[] = [];
		for (let i = 0; i < blockCount; i++) {
			const startKey = new Uint8Array(data.subarray(pos, pos + this.keyCodec.stride));
			pos += this.keyCodec.stride;
			const endKey = new Uint8Array(data.subarray(pos, pos + this.keyCodec.stride));
			pos += this.keyCodec.stride;
			const offset = Number(view.getBigUint64(pos, true));
			pos += 8;
			const size = view.getUint32(pos, true);
			pos += 4;
			const entryCount = view.getUint32(pos, true);
			pos += 4;
			blocks.push({ startKey, endKey, offset, size, entryCount });
		}

		return { blocks, bloomFilter, totalEntries, fileSize };
	}

	private hashKey(key: Uint8Array, seed: number): number {
		let hash = seed;
		for (let i = 0; i < 4 && i < key.length; i++) hash = (hash * 31 + key[i]!) | 0;
		return Math.abs(hash);
	}

	private compareKeys(a: Uint8Array, b: Uint8Array): number {
		for (let i = 0; i < a.length; i++) {
			if (a[i]! < b[i]!) return -1;
			if (a[i]! > b[i]!) return 1;
		}
		return 0;
	}
}
