import type { Codec } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { Uint8ArrayView } from "~/lib/Uint8ArrayView.ts";
import { Uint8ArrayMap } from "../Uint8ArrayMap.ts";
import { readFileFull, writeFileFull } from "../utils/fs.ts";
import type { Transaction, Transactionable } from "./Transaction.ts";

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

interface CachedBlock {
	data: Uint8Array;
	lastAccess: number;
}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/**
 * FixedKVStore — LSM-based store for fixed-size KV pairs.
 *
 * Mutations only through transactions. Reads available directly on the store
 * (SST + block cache) or through a transaction (staged ops visible too).
 *
 * Crash safety:
 *   commit()   — writes WAL atomically (temp file + rename). No data mutation yet.
 *   finalize() — flushes WAL entries to SST, then deletes WAL.
 *                If power dies mid-finalize, WAL still exists → replayed on next start.
 *                SST supports overwrites so replaying is always safe.
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
	private sstRanges: Array<{ startKey: Uint8Array; endKey: Uint8Array; sstIndex: number }> = [];
	private fileOffset = 0;

	private blockCache: Map<number, CachedBlock> = new Map();
	private accessCounter = 0;

	private readonly blockBuffer: Uint8Array;

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
		this.blockBuffer = new Uint8Array(this.blockSize);
	}

	// -------------------------------------------------------------------------
	// Transactionable
	// -------------------------------------------------------------------------

	transaction(): FixedKVStoreTransaction<K, V> {
		if (this.activeTx !== null) throw new Error("A transaction is already open");
		this.activeTx = new FixedKVStoreTransaction(this);
		return this.activeTx;
	}

	/**
	 * Apply any pending WAL to SST, then delete the WAL. Idempotent.
	 * Always reads from disk — no in-memory shortcut.
	 * If power dies mid-flush, WAL still exists → safe to replay on next start.
	 */
	async finalize(): Promise<void> {
		await this.ensureFile();
		this.activeTx = null;

		if (!await exists(this.walPath)) return;

		const staged = await this.readWal();
		if (staged.size > 0) await this.flushEntries(staged);
		await this.deleteWal();
	}

	// -------------------------------------------------------------------------
	// Reads (public)
	// -------------------------------------------------------------------------

	async prepare(): Promise<void> {
		await this.ensureFile();
	}

	async get(key: K): Promise<V | undefined> {
		await this.ensureFile();
		const keyBytes = this.encodeKey(key);
		return this.getByBytes(keyBytes);
	}

	async getMany(keys: K[]): Promise<(V | undefined)[]> {
		await this.ensureFile();
		const keyBytes = keys.map((k) => this.encodeKey(k));
		return this.getManyByBytes(keyBytes);
	}

	// -------------------------------------------------------------------------
	// Internal: codec helpers (used by transaction)
	// -------------------------------------------------------------------------

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

	// -------------------------------------------------------------------------
	// Internal: WAL
	// -------------------------------------------------------------------------

	/**
	 * Write staged entries to WAL atomically:
	 *   1. Write full buffer to a .wal.tmp file
	 *   2. Rename to .wal
	 * If power dies during step 1, .wal never exists → no corruption.
	 */
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

		// Write to temp first, then atomically rename to final WAL path
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

	// -------------------------------------------------------------------------
	// Internal: SST flush
	// -------------------------------------------------------------------------

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

		for (let i = 0; i < entries.length; i++) {
			const [key, value] = entries[i]!;

			if (blockStartKey === null) blockStartKey = key;

			this.blockBuffer.set(key, blockBufferPos);
			this.blockBuffer.set(value, blockBufferPos + this.keyCodec.stride);
			blockBufferPos += entrySize;
			blockEntryCount++;

			const flush = blockBufferPos + entrySize > this.blockSize || i === entries.length - 1;

			if (flush) {
				const slice = this.blockBuffer.subarray(0, blockBufferPos);
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
		this.sstRanges.push({
			startKey: blocks[0]!.startKey,
			endKey: blocks[blocks.length - 1]!.endKey,
			sstIndex: this.sstFiles.length - 1,
		});
	}

	// -------------------------------------------------------------------------
	// Internal: reads
	// -------------------------------------------------------------------------

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
		const file = await this.ensureFile();
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
			const sst = this.sstFiles[si]!;
			const block = sst.blocks[blockIdx]!;

			let blockData: Uint8Array;
			const cached = this.blockCache.get(cacheKey);
			if (cached) {
				blockData = cached.data;
				cached.lastAccess = ++this.accessCounter;
			} else {
				await file.seek(block.offset, Deno.SeekMode.Start);
				blockData = new Uint8Array(block.size);
				await readFileFull(this.file!, blockData);
				this.addToCache(cacheKey, blockData);
			}

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
			cached.lastAccess = ++this.accessCounter;
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

	// -------------------------------------------------------------------------
	// Internal: file init
	// -------------------------------------------------------------------------

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

	// -------------------------------------------------------------------------
	// Internal: load from disk
	// -------------------------------------------------------------------------

	private async loadExistingData(): Promise<void> {
		const stat = await this.file!.stat();
		if (stat.size === 0) { this.fileOffset = 0; return; }

		const fileData = new Uint8Array(stat.size);
		await this.file!.seek(0, Deno.SeekMode.Start);
		await readFileFull(this.file!, fileData);

		const magicValue = 0x524F434B;
		const sstInfos: Array<{ dataStart: number; metadataStart: number; metadataSize: number; fileSize: number }> = [];
		let searchPos = 4;

		while (searchPos <= stat.size) {
			if (searchPos + 4 > stat.size) break;
			const view = new DataView(fileData.buffer, searchPos, 4);
			if (view.getUint32(0, true) === magicValue) {
				const metadataStart = searchPos - 4;
				if (metadataStart >= 0) {
					const metadataSizeView = new DataView(fileData.buffer, metadataStart, 4);
					const metadataSize = metadataSizeView.getUint32(0, true);
					if (metadataStart + metadataSize <= stat.size && metadataSize > 8) {
						const sst = this.decodeMetadata(fileData.subarray(metadataStart, metadataStart + metadataSize));
						const dataStart = sstInfos.length === 0
							? 0
							: (() => { const p = sstInfos[sstInfos.length - 1]!; return p.dataStart + p.fileSize + p.metadataSize; })();
						sstInfos.push({ dataStart, metadataStart, metadataSize, fileSize: sst.fileSize });
					}
				}
			}
			searchPos++;
		}

		for (const info of sstInfos) {
			const sst = this.decodeMetadata(fileData.subarray(info.metadataStart, info.metadataStart + info.metadataSize));
			for (const block of sst.blocks) block.offset = info.dataStart + block.offset;
			const sstIndex = this.sstFiles.length;
			this.sstFiles.push(sst);
			this.sstRanges.push({ startKey: sst.blocks[0]!.startKey, endKey: sst.blocks[sst.blocks.length - 1]!.endKey, sstIndex });
		}

		this.fileOffset = stat.size;
	}

	// -------------------------------------------------------------------------
	// Internal: SST helpers
	// -------------------------------------------------------------------------

	private findSst(keyBytes: Uint8Array): number {
		for (let i = this.sstFiles.length - 1; i >= 0; i--) {
			const sst = this.sstFiles[i]!;
			const startKey = sst.blocks[0]!.startKey;
			const endKey = sst.blocks[sst.blocks.length - 1]!.endKey;
			if (this.compareKeys(keyBytes, startKey) >= 0 && this.compareKeys(keyBytes, endKey) <= 0) return i;
		}
		return -1;
	}

	private binarySearchInBlock(
		data: Uint8Array,
		searchKey: Uint8Array,
		entryCount: number,
	): Uint8Array | undefined {
		// Returns: Uint8Array = value bytes, undefined = not found
		// SST entry layout: [key(K)][value(V)]
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
			let oldestKey: number | null = null, oldestTime = Infinity;
			for (const [k, v] of this.blockCache) {
				if (v.lastAccess < oldestTime) { oldestTime = v.lastAccess; oldestKey = k; }
			}
			if (oldestKey !== null) this.blockCache.delete(oldestKey);
		}
		this.blockCache.set(key, { data: new Uint8Array(data), lastAccess: ++this.accessCounter });
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
		const blockSize = 8 + this.keyCodec.stride * 2 + 8 + 4;
		const totalSize = 8 + 4 + 4 + metadata.blocks.length * blockSize + metadata.bloomFilter.length + 4;
		const buffer = new Uint8Array(totalSize);
		const view = new DataView(buffer.buffer);
		let pos = 0;

		view.setUint32(pos, totalSize, true); pos += 4;
		view.setUint32(pos, 0x524F434B, true); pos += 4;
		view.setUint32(pos, metadata.bloomFilter.length, true); pos += 4;
		buffer.set(metadata.bloomFilter, pos); pos += metadata.bloomFilter.length;
		view.setUint32(pos, metadata.totalEntries, true); pos += 4;
		view.setUint32(pos, metadata.fileSize, true); pos += 4;
		view.setUint32(pos, metadata.blocks.length, true); pos += 4;

		for (const block of metadata.blocks) {
			buffer.set(block.startKey, pos); pos += this.keyCodec.stride;
			buffer.set(block.endKey, pos); pos += this.keyCodec.stride;
			view.setBigUint64(pos, BigInt(block.offset), true); pos += 8;
			view.setUint32(pos, block.size, true); pos += 4;
			view.setUint32(pos, block.entryCount, true); pos += 4;
		}

		return buffer;
	}

	private decodeMetadata(data: Uint8Array): SSTMetadata {
		const view = new Uint8ArrayView(data);
		let pos = 8;

		const bloomSize = view.getUint32(pos, true); pos += 4;
		const bloomFilter = new Uint8Array(data.subarray(pos, pos + bloomSize)); pos += bloomSize;
		const totalEntries = view.getUint32(pos, true); pos += 4;
		const fileSize = view.getUint32(pos, true); pos += 4;
		const blockCount = view.getUint32(pos, true); pos += 4;

		const blocks: BlockMetadata[] = [];
		for (let i = 0; i < blockCount; i++) {
			const startKey = new Uint8Array(data.subarray(pos, pos + this.keyCodec.stride)); pos += this.keyCodec.stride;
			const endKey = new Uint8Array(data.subarray(pos, pos + this.keyCodec.stride)); pos += this.keyCodec.stride;
			const offset = Number(view.getBigUint64(pos, true)); pos += 8;
			const size = view.getUint32(pos, true); pos += 4;
			const entryCount = view.getUint32(pos, true); pos += 4;
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
