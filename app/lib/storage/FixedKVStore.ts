import type { Codec } from "@nomadshiba/codec";

export interface FixedKVStoreOptions<K, V> {
	keyCodec: Codec<K>;
	valueCodec: Codec<V>;
	memtableSize?: number;
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

// Fast Uint8Array hash map using value equality
class Uint8ArrayMap {
	private buckets: Array<Array<{ key: Uint8Array; value: Uint8Array }>>;
	private size = 0;
	private keySize: number;
	private mask: number;

	constructor(keySize: number, capacity = 16384) {
		this.keySize = keySize;
		const pow2 = Math.pow(2, Math.ceil(Math.log2(capacity)));
		this.mask = pow2 - 1;
		this.buckets = new Array(pow2);
		for (let i = 0; i < pow2; i++) {
			this.buckets[i] = [];
		}
	}

	private hash(key: Uint8Array): number {
		return ((key[0]! | (key[1]! << 8) | (key[2]! << 16) | (key[3]! << 24)) >>> 0);
	}

	private keysEqual(a: Uint8Array, b: Uint8Array): boolean {
		for (let i = 0; i < this.keySize; i++) {
			if (a[i] !== b[i]) return false;
		}
		return true;
	}

	get(key: Uint8Array): Uint8Array | undefined {
		const hash = this.hash(key);
		const bucket = this.buckets[hash & this.mask]!;

		for (let i = 0; i < bucket.length; i++) {
			if (this.keysEqual(bucket[i]!.key, key)) {
				return bucket[i]!.value;
			}
		}
		return undefined;
	}

	set(key: Uint8Array, value: Uint8Array): void {
		const hash = this.hash(key);
		const idx = hash & this.mask;
		const bucket = this.buckets[idx]!;

		for (let i = 0; i < bucket.length; i++) {
			if (this.keysEqual(bucket[i]!.key, key)) {
				bucket[i]!.value = value;
				return;
			}
		}

		bucket.push({ key: key.slice(), value });
		this.size++;
	}

	clear(): void {
		for (let i = 0; i < this.buckets.length; i++) {
			this.buckets[i] = [];
		}
		this.size = 0;
	}

	getSize(): number {
		return this.size;
	}

	*entries(): Generator<{ key: Uint8Array; value: Uint8Array }> {
		for (const bucket of this.buckets) {
			for (const entry of bucket) {
				yield entry;
			}
		}
	}
}

/**
 * FixedKVStore - Optimized LSM store for fixed-size KV using Codec pattern
 */
export class FixedKVStore<K, V> {
	private dataFile: Deno.FsFile;
	private keyCodec: Codec<K>;
	private valueCodec: Codec<V>;

	private keySize: number;
	private valueSize: number;
	private entrySize: number;
	private memtableSize: number;
	private blockSize: number;
	private maxCacheSize: number;

	private memtable: Uint8ArrayMap;
	private sstFiles: SSTMetadata[] = [];
	private sstRanges: Array<{ startKey: Uint8Array; endKey: Uint8Array; sstIndex: number }> = [];
	private fileOffset = 0;

	private blockCache: Map<number, CachedBlock> = new Map();
	private cacheHits = 0;
	private cacheMisses = 0;
	private accessCounter = 0;

	private blockBuffer: Uint8Array;

	// Preparation state
	private prepared = false;

	constructor(
		dataFile: Deno.FsFile,
		options: FixedKVStoreOptions<K, V>,
	) {
		this.dataFile = dataFile;
		this.keyCodec = options.keyCodec;
		this.valueCodec = options.valueCodec;

		if (options.keyCodec.stride < 0) {
			throw new Error("Key codec must have fixed stride (>= 0)");
		}
		if (options.valueCodec.stride < 0) {
			throw new Error("Value codec must have fixed stride (>= 0)");
		}

		this.keySize = options.keyCodec.stride;
		this.valueSize = options.valueCodec.stride;
		this.entrySize = this.keySize + this.valueSize;
		this.memtableSize = options.memtableSize ?? 10000;
		this.blockSize = options.blockSize ?? 65536;
		this.maxCacheSize = options.blockCacheSize ?? 1000;

		this.memtable = new Uint8ArrayMap(this.keySize, this.memtableSize * 2);
		this.blockBuffer = new Uint8Array(this.blockSize);
	}

	/**
	 * Prepare the store by loading existing data from disk.
	 * Called automatically on first use, can be called manually.
	 */
	async prepare(): Promise<void> {
		if (this.prepared) return;

		const stat = await this.dataFile.stat();
		if (stat.size > 0) {
			await this.loadExistingData();
		}

		this.prepared = true;
	}

	/**
	 * Get a single value by key
	 * Optimized for single key lookups
	 */
	async get(key: K): Promise<V | undefined> {
		await this.prepare();

		const keyBytes = this.keyCodec.encode(key);

		// Check memtable first
		const memValue = this.memtable.get(keyBytes);
		if (memValue !== undefined) {
			return this.valueCodec.decode(memValue)[0];
		}

		// Find the SST with the highest index (most recent) that contains this key
		let sstIndex = -1;
		for (let i = this.sstFiles.length - 1; i >= 0; i--) {
			const sst = this.sstFiles[i]!;
			const startKey = sst.blocks[0]!.startKey;
			const endKey = sst.blocks[sst.blocks.length - 1]!.endKey;

			if (this.compareKeys(keyBytes, startKey) >= 0 && this.compareKeys(keyBytes, endKey) <= 0) {
				sstIndex = i;
				break;
			}
		}

		if (sstIndex === -1) return undefined;

		const sst = this.sstFiles[sstIndex]!;

		// Bloom filter check
		if (!this.mightContain(sst.bloomFilter, keyBytes)) return undefined;

		// Find block
		const blockIdx = this.findBlock(sst.blocks, keyBytes);
		if (blockIdx === -1) return undefined;

		// Read block
		const block = sst.blocks[blockIdx]!;
		const cacheKey = (sstIndex << 20) | blockIdx;

		let blockData: Uint8Array;
		const cached = this.blockCache.get(cacheKey);

		if (cached) {
			blockData = cached.data;
			cached.lastAccess = ++this.accessCounter;
			this.cacheHits++;
		} else {
			await this.dataFile.seek(block.offset, Deno.SeekMode.Start);
			blockData = new Uint8Array(block.size);
			await this.dataFile.read(blockData);

			this.addToCache(cacheKey, blockData);
			this.cacheMisses++;
		}

		// Search for key in block
		const valueBytes = this.binarySearchInBlock(blockData, keyBytes, block.entryCount);
		if (valueBytes !== undefined) {
			return this.valueCodec.decode(valueBytes)[0];
		}

		return undefined;
	}

	/**
	 * Batch get - much faster for multiple keys
	 * Groups reads by block to minimize disk seeks
	 */
	async getMany(keys: K[]): Promise<(V | undefined)[]> {
		await this.prepare();

		const keyBytes = keys.map((k) => this.keyCodec.encode(k));
		const results: (V | undefined)[] = new Array(keys.length).fill(undefined);
		const pendingByBlock = new Map<number, Array<{ keyIndex: number; key: Uint8Array }>>();

		// Phase 1: Check memtable and plan block reads
		for (let ki = 0; ki < keys.length; ki++) {
			const key = keyBytes[ki]!;

			// Check memtable first
			const memValue = this.memtable.get(key);
			if (memValue !== undefined) {
				results[ki] = this.valueCodec.decode(memValue)[0];
				continue;
			}

			// Find the SST with the highest index (most recent) that contains this key
			let sstIndex = -1;
			for (let i = this.sstFiles.length - 1; i >= 0; i--) {
				const sst = this.sstFiles[i]!;
				const startKey = sst.blocks[0]!.startKey;
				const endKey = sst.blocks[sst.blocks.length - 1]!.endKey;

				if (this.compareKeys(key, startKey) >= 0 && this.compareKeys(key, endKey) <= 0) {
					sstIndex = i;
					break;
				}
			}

			if (sstIndex === -1) continue;

			const sst = this.sstFiles[sstIndex]!;

			// Bloom filter check
			if (!this.mightContain(sst.bloomFilter, key)) continue;

			// Find block
			const blockIdx = this.findBlock(sst.blocks, key);
			if (blockIdx === -1) continue;

			// Queue for batch read
			const cacheKey = (sstIndex << 20) | blockIdx;
			if (!pendingByBlock.has(cacheKey)) {
				pendingByBlock.set(cacheKey, []);
			}
			pendingByBlock.get(cacheKey)!.push({ keyIndex: ki, key });
		}

		// Phase 2: Read blocks in batches
		for (const [cacheKey, pending] of pendingByBlock) {
			const si = cacheKey >>> 20;
			const blockIdx = cacheKey & 0xFFFFF;
			const sst = this.sstFiles[si]!;
			const block = sst.blocks[blockIdx]!;

			// Read block once
			let blockData: Uint8Array;
			const cached = this.blockCache.get(cacheKey);

			if (cached) {
				blockData = cached.data;
				cached.lastAccess = ++this.accessCounter;
				this.cacheHits++;
			} else {
				await this.dataFile.seek(block.offset, Deno.SeekMode.Start);
				blockData = new Uint8Array(block.size);
				await this.dataFile.read(blockData);

				this.addToCache(cacheKey, blockData);
				this.cacheMisses++;
			}

			// Search for all pending keys in this block
			for (const { keyIndex, key } of pending) {
				const valueBytes = this.binarySearchInBlock(blockData, key, block.entryCount);
				if (valueBytes !== undefined) {
					results[keyIndex] = this.valueCodec.decode(valueBytes)[0];
				}
			}
		}

		return results;
	}

	/**
	 * Set a single key-value pair
	 * Optimized for single key operations
	 */
	async set(key: K, value: V): Promise<void> {
		await this.prepare();

		const keyBytes = this.keyCodec.encode(key);
		const valueBytes = this.valueCodec.encode(value);

		// Add to memtable
		this.memtable.set(keyBytes, valueBytes.slice());

		// Flush if full
		if (this.memtable.getSize() >= this.memtableSize) {
			await this.flushMemtable();
		}
	}

	/**
	 * Batch set - optimized for multiple key-value pairs
	 * More efficient than calling set() multiple times
	 */
	async setMany(entries: Array<{ key: K; value: V }>): Promise<void> {
		await this.prepare();

		// Add all entries to memtable
		for (const { key, value } of entries) {
			const keyBytes = this.keyCodec.encode(key);
			const valueBytes = this.valueCodec.encode(value);
			this.memtable.set(keyBytes, valueBytes.slice());
		}

		// Flush if full
		if (this.memtable.getSize() >= this.memtableSize) {
			await this.flushMemtable();
		}
	}

	async close(): Promise<void> {
		if (this.memtable.getSize() > 0) {
			await this.flushMemtable();
			await this.dataFile.sync();
		}
	}

	getStats() {
		let sstSize = 0;
		let totalBlocks = 0;
		for (const sst of this.sstFiles) {
			sstSize += sst.fileSize;
			totalBlocks += sst.blocks.length;
		}

		const memtableEntries = this.memtable.getSize();
		const sstEntries = this.sstFiles.reduce((sum, s) => sum + s.totalEntries, 0);
		const totalCacheSize = this.blockCache.values()
			.reduce((sum, block) => sum + block.data.length, 0);

		return {
			memtableEntries,
			sstCount: this.sstFiles.length,
			sstEntries,
			totalEntries: memtableEntries + sstEntries,
			totalBlocks,
			sstSize,
			fileSize: this.fileOffset,
			rawDataSize: (memtableEntries + sstEntries) * this.entrySize,
			overhead: sstSize - ((memtableEntries + sstEntries) * this.entrySize),
			cacheEntries: this.blockCache.size,
			cacheSize: totalCacheSize,
			cacheHits: this.cacheHits,
			cacheMisses: this.cacheMisses,
			cacheHitRate: this.cacheHits / (this.cacheHits + this.cacheMisses || 1),
		};
	}

	private async flushMemtable(): Promise<void> {
		if (this.memtable.getSize() === 0) return;

		// Collect entries
		const entries: Array<{ key: Uint8Array; value: Uint8Array }> = this.memtable.entries().toArray();
		entries.sort((a, b) => this.compareKeys(a.key, b.key));

		// Build bloom filter
		const bloomFilter = this.buildBloomFilter(entries);

		// Track SST boundaries
		const sstDataStart = this.fileOffset;

		// Write data blocks first
		const blocks: BlockMetadata[] = [];
		let dataOffset = sstDataStart;
		let blockBufferPos = 0;
		let blockEntryCount = 0;
		let blockStartKey: Uint8Array | null = null;

		for (let i = 0; i < entries.length; i++) {
			const entry = entries[i]!;

			if (blockStartKey === null) {
				blockStartKey = entry.key;
			}

			// Write to block buffer
			this.blockBuffer.set(entry.key, blockBufferPos);
			this.blockBuffer.set(entry.value, blockBufferPos + this.keySize);
			blockBufferPos += this.entrySize;
			blockEntryCount++;

			// Flush block if full or last entry
			if (blockBufferPos + this.entrySize > this.blockSize || i === entries.length - 1) {
				const uncompressed = this.blockBuffer.subarray(0, blockBufferPos);
				const compressed = uncompressed;

				await this.dataFile.seek(dataOffset, Deno.SeekMode.Start);
				await this.dataFile.write(compressed);

				// Store offset relative to SST data start
				blocks.push({
					startKey: new Uint8Array(blockStartKey),
					endKey: new Uint8Array(entry.key),
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

		// Now write metadata at the end
		const metadata: SSTMetadata = {
			blocks,
			bloomFilter,
			totalEntries: entries.length,
			fileSize: dataOffset - sstDataStart,
		};

		const metadataBytes = this.encodeMetadata(metadata);
		await this.dataFile.seek(dataOffset, Deno.SeekMode.Start);
		await this.dataFile.write(metadataBytes);

		// Update state
		this.fileOffset = dataOffset + metadataBytes.length;
		const sstIndex = this.sstFiles.length;

		// Convert block offsets to absolute before storing
		for (const block of metadata.blocks) {
			block.offset = sstDataStart + block.offset;
		}

		this.sstFiles.push(metadata);

		// Add to range index
		this.sstRanges.push({
			startKey: blocks[0]!.startKey,
			endKey: blocks[blocks.length - 1]!.endKey,
			sstIndex,
		});

		// Clear memtable
		this.memtable.clear();
	}

	private async loadExistingData(): Promise<void> {
		const stat = await this.dataFile.stat();
		if (stat.size === 0) {
			this.fileOffset = 0;
			return;
		}

		// Read the whole file to find metadata blocks
		const fileData = new Uint8Array(stat.size);
		await this.dataFile.seek(0, Deno.SeekMode.Start);
		await this.dataFile.read(fileData);

		// Find all magic numbers (0x524F434B) which marks metadata blocks
		const magicValue = 0x524F434B;
		const sstInfos: Array<{ dataStart: number; metadataStart: number; metadataSize: number; fileSize: number }> =
			[];
		let searchPos = 4;

		while (searchPos <= stat.size) {
			const magicPos = searchPos;
			if (magicPos + 4 > stat.size) break;

			const view = new DataView(fileData.buffer, magicPos, 4);
			const magic = view.getUint32(0, true);

			if (magic === magicValue) {
				const metadataStart = magicPos - 4;
				if (metadataStart < 0) {
					searchPos++;
					continue;
				}

				const metadataSizeView = new DataView(fileData.buffer, metadataStart, 4);
				const metadataSize = metadataSizeView.getUint32(0, true);

				if (metadataStart + metadataSize <= stat.size && metadataSize > 8) {
					const sst = this.decodeMetadata(fileData.subarray(metadataStart, metadataStart + metadataSize));
					let dataStart: number;
					if (sstInfos.length === 0) {
						dataStart = 0;
					} else {
						const prev = sstInfos[sstInfos.length - 1]!;
						dataStart = prev.dataStart + prev.fileSize + prev.metadataSize;
					}
					sstInfos.push({ dataStart, metadataStart, metadataSize, fileSize: sst.fileSize });
				}
			}

			searchPos++;
		}

		// Load each SST metadata
		for (const info of sstInfos) {
			const metadataBuf = fileData.subarray(info.metadataStart, info.metadataStart + info.metadataSize);
			const sst = this.decodeMetadata(metadataBuf);

			// Adjust block offsets to be absolute from start of file
			for (const block of sst.blocks) {
				block.offset = info.dataStart + block.offset;
			}

			const sstIndex = this.sstFiles.length;
			this.sstFiles.push(sst);
			this.sstRanges.push({
				startKey: sst.blocks[0]!.startKey,
				endKey: sst.blocks[sst.blocks.length - 1]!.endKey,
				sstIndex,
			});
		}

		this.fileOffset = stat.size;
	}

	private addToCache(key: number, data: Uint8Array): void {
		// Evict oldest if cache is full
		if (this.blockCache.size >= this.maxCacheSize) {
			let oldestKey: number | null = null;
			let oldestTime = Infinity;

			for (const [k, v] of this.blockCache) {
				if (v.lastAccess < oldestTime) {
					oldestTime = v.lastAccess;
					oldestKey = k;
				}
			}

			if (oldestKey !== null) {
				this.blockCache.delete(oldestKey);
			}
		}

		this.blockCache.set(key, {
			data: new Uint8Array(data),
			lastAccess: ++this.accessCounter,
		});
	}

	private binarySearchInBlock(
		data: Uint8Array,
		searchKey: Uint8Array,
		entryCount: number,
	): Uint8Array | undefined {
		let left = 0;
		let right = entryCount - 1;

		while (left <= right) {
			const mid = Math.floor((left + right) / 2);
			const offset = mid * this.entrySize;
			const key = data.subarray(offset, offset + this.keySize);

			const cmp = this.compareKeys(key, searchKey);

			if (cmp === 0) {
				return data.subarray(offset + this.keySize, offset + this.entrySize);
			} else if (cmp < 0) {
				left = mid + 1;
			} else {
				right = mid - 1;
			}
		}

		return undefined;
	}

	private findBlock(blocks: BlockMetadata[], key: Uint8Array): number {
		let left = 0;
		let right = blocks.length - 1;

		while (left <= right) {
			const mid = Math.floor((left + right) / 2);
			const block = blocks[mid]!;

			if (this.compareKeys(key, block.startKey) < 0) {
				right = mid - 1;
			} else if (this.compareKeys(key, block.endKey) > 0) {
				left = mid + 1;
			} else {
				return mid;
			}
		}

		return -1;
	}

	private buildBloomFilter(entries: Array<{ key: Uint8Array; value: Uint8Array }>): Uint8Array {
		const bits = entries.length * 8;
		const bytes = Math.ceil(bits / 8);
		const filter = new Uint8Array(bytes);

		for (const entry of entries) {
			const h1 = this.hashKey(entry.key, 0);
			const h2 = this.hashKey(entry.key, 1);

			const idx1 = h1 % bits;
			const idx2 = h2 % bits;

			filter[Math.floor(idx1 / 8)]! |= 1 << (idx1 % 8);
			filter[Math.floor(idx2 / 8)]! |= 1 << (idx2 % 8);
		}

		return filter;
	}

	private mightContain(filter: Uint8Array, key: Uint8Array): boolean {
		const bits = filter.length * 8;
		const h1 = this.hashKey(key, 0);
		const h2 = this.hashKey(key, 1);

		const idx1 = h1 % bits;
		const idx2 = h2 % bits;

		return (filter[Math.floor(idx1 / 8)]! & (1 << (idx1 % 8))) !== 0 &&
			(filter[Math.floor(idx2 / 8)]! & (1 << (idx2 % 8))) !== 0;
	}

	private encodeMetadata(metadata: SSTMetadata): Uint8Array {
		const blockSize = 8 + this.keySize * 2 + 8 + 4;
		const totalSize = 8 + 4 + 4 + metadata.blocks.length * blockSize + metadata.bloomFilter.length + 4;

		const buf = new Uint8Array(totalSize);
		const view = new DataView(buf.buffer);
		let pos = 0;

		view.setUint32(pos, totalSize, true);
		pos += 4;
		view.setUint32(pos, 0x524F434B, true);
		pos += 4;

		view.setUint32(pos, metadata.bloomFilter.length, true);
		pos += 4;
		buf.set(metadata.bloomFilter, pos);
		pos += metadata.bloomFilter.length;

		view.setUint32(pos, metadata.totalEntries, true);
		pos += 4;

		view.setUint32(pos, metadata.fileSize, true);
		pos += 4;

		view.setUint32(pos, metadata.blocks.length, true);
		pos += 4;

		for (const block of metadata.blocks) {
			buf.set(block.startKey, pos);
			pos += this.keySize;
			buf.set(block.endKey, pos);
			pos += this.keySize;
			view.setBigUint64(pos, BigInt(block.offset), true);
			pos += 8;
			view.setUint32(pos, block.size, true);
			pos += 4;
			view.setUint32(pos, block.entryCount, true);
			pos += 4;
		}

		return buf;
	}

	private decodeMetadata(data: Uint8Array): SSTMetadata {
		const view = new DataView(data.buffer, data.byteOffset);
		let pos = 8; // Skip header (metadataSize + magic)

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
			const startKey = new Uint8Array(data.subarray(pos, pos + this.keySize));
			pos += this.keySize;
			const endKey = new Uint8Array(data.subarray(pos, pos + this.keySize));
			pos += this.keySize;
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
		for (let i = 0; i < 4 && i < key.length; i++) {
			hash = (hash * 31 + key[i]!) | 0;
		}
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
