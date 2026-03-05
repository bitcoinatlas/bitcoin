import type { Codec } from "@nomadshiba/codec";

export interface FixedKVStoreOptions<K, V> {
	keyCodec: Codec<K>;
	valueCodec: Codec<V>;
	maxCacheBlockCount?: number;
}

interface CachedBlock {
	data: Uint8Array;
	lastAccess: number;
}

/**
 * FixedKVStore - Disk-first store optimized for fast reads
 *
 * Design:
 * - Writes go directly to disk (append-only)
 * - In-memory hash index for O(1) key lookups
 * - Block cache for frequently read data
 * - Fixed size key-value pairs
 * - No deletes supported
 */
export class FixedKVStore<K, V> {
	private filePath: string;
	private dataFile?: Deno.FsFile;
	private keyCodec: Codec<K>;
	private valueCodec: Codec<V>;

	private maxCacheBlockCount: number;
	private fileOffset = 0;

	// In-memory index: key hash -> disk offset
	private index: Map<string, number> = new Map();

	// Block cache
	private blockCache: Map<number, CachedBlock> = new Map();
	private accessCounter = 0;

	// Preparation state
	private prepared = false;

	constructor(
		filePath: string,
		options: FixedKVStoreOptions<K, V>,
	) {
		this.filePath = filePath;
		this.keyCodec = options.keyCodec;
		this.valueCodec = options.valueCodec;

		if (options.keyCodec.stride < 0) {
			throw new Error("Key codec must have fixed stride (>= 0)");
		}
		if (options.valueCodec.stride < 0) {
			throw new Error("Value codec must have fixed stride (>= 0)");
		}

		this.maxCacheBlockCount = options.maxCacheBlockCount ?? 1000;
	}

	private async ensureOpen(): Promise<Deno.FsFile> {
		if (!this.dataFile) {
			this.dataFile = await Deno.open(this.filePath, {
				read: true,
				write: true,
				create: true,
			});
		}
		return this.dataFile;
	}

	/**
	 * Prepare the store by loading existing index from disk.
	 * Called automatically on first use, can be called manually.
	 */
	async prepare(): Promise<void> {
		if (this.prepared) return;

		const file = await this.ensureOpen();
		const stat = await file.stat();
		if (stat.size > 0) {
			await this.loadExistingData();
		}

		this.prepared = true;
	}

	/**
	 * Get a single value by key
	 * Optimized path: check cache, then disk
	 */
	async get(key: K): Promise<V | undefined> {
		await this.prepare();
		const file = await this.ensureOpen();

		const keyBytes = this.keyCodec.encode(key);
		const keyHash = this.hashKey(keyBytes);

		const offset = this.index.get(keyHash);
		if (offset === undefined) return undefined;

		// Check cache first
		const cacheKey = Math.floor(offset / this.getBlockSize());
		const cached = this.blockCache.get(cacheKey);

		let valueBytes: Uint8Array;

		if (cached) {
			valueBytes = this.extractValueFromBlock(cached.data, offset, cacheKey);
			cached.lastAccess = ++this.accessCounter;
		} else {
			const blockSize = this.getBlockSize();
			const blockStart = Math.floor(offset / blockSize) * blockSize;
			const blockEnd = Math.min(blockStart + blockSize, this.fileOffset);

			await file.seek(blockStart, Deno.SeekMode.Start);
			const blockData = new Uint8Array(blockEnd - blockStart);
			await file.read(blockData);

			// Add to cache
			this.addToCache(cacheKey, blockData);

			valueBytes = this.extractValueFromBlock(blockData, offset, blockStart);
		}

		return this.valueCodec.decode(valueBytes)[0];
	}

	/**
	 * Batch get - optimized for multiple keys
	 */
	async getMany(keys: K[]): Promise<(V | undefined)[]> {
		await this.prepare();
		const file = await this.ensureOpen();

		// Group by block to minimize disk reads
		const byBlock = new Map<number, Array<{ keyIndex: number; offset: number }>>();
		const results: (V | undefined)[] = new Array(keys.length).fill(undefined);

		for (let i = 0; i < keys.length; i++) {
			const keyBytes = this.keyCodec.encode(keys[i]!);
			const keyHash = this.hashKey(keyBytes);
			const offset = this.index.get(keyHash);

			if (offset === undefined) continue;

			const cacheKey = Math.floor(offset / this.getBlockSize());

			if (!byBlock.has(cacheKey)) {
				byBlock.set(cacheKey, []);
			}
			byBlock.get(cacheKey)!.push({ keyIndex: i, offset });
		}

		// Read blocks
		for (const [cacheKey, items] of byBlock) {
			const cached = this.blockCache.get(cacheKey);
			let blockData: Uint8Array;
			let blockStart: number;

			if (cached) {
				blockData = cached.data;
				blockStart = cacheKey * this.getBlockSize();
				cached.lastAccess = ++this.accessCounter;
			} else {
				const blockSize = this.getBlockSize();
				blockStart = cacheKey * blockSize;
				const blockEnd = Math.min(blockStart + blockSize, this.fileOffset);

				await file.seek(blockStart, Deno.SeekMode.Start);
				blockData = new Uint8Array(blockEnd - blockStart);
				await file.read(blockData);

				this.addToCache(cacheKey, blockData);
			}

			// Extract values
			for (const { keyIndex, offset } of items) {
				const valueBytes = this.extractValueFromBlock(blockData, offset, blockStart);
				results[keyIndex] = this.valueCodec.decode(valueBytes)[0];
			}
		}

		return results;
	}

	/**
	 * Set a single key-value pair
	 * Writes directly to disk, updates index
	 */
	async set(key: K, value: V): Promise<void> {
		await this.prepare();
		const file = await this.ensureOpen();

		const keyBytes = this.keyCodec.encode(key);
		const valueBytes = this.valueCodec.encode(value);
		const keyHash = this.hashKey(keyBytes);

		const entrySize = this.keyCodec.stride + this.valueCodec.stride;
		const buf = new Uint8Array(entrySize);
		buf.set(keyBytes, 0);
		buf.set(valueBytes, this.keyCodec.stride);

		// Write directly to disk at current offset
		const offset = this.fileOffset;
		await file.seek(offset, Deno.SeekMode.Start);
		await file.write(buf);

		// Update index
		this.index.set(keyHash, offset);
		this.fileOffset += entrySize;
	}

	/**
	 * Batch set - optimized for multiple writes
	 */
	async setMany(entries: Array<{ key: K; value: V }>): Promise<void> {
		await this.prepare();
		const file = await this.ensureOpen();

		const entrySize = this.keyCodec.stride + this.valueCodec.stride;
		const buf = new Uint8Array(entries.length * entrySize);
		let pos = 0;

		for (const { key, value } of entries) {
			const keyBytes = this.keyCodec.encode(key);
			const valueBytes = this.valueCodec.encode(value);
			const keyHash = this.hashKey(keyBytes);

			buf.set(keyBytes, pos);
			buf.set(valueBytes, pos + this.keyCodec.stride);

			// Update index
			this.index.set(keyHash, this.fileOffset + pos);

			pos += entrySize;
		}

		// Single write
		await file.seek(this.fileOffset, Deno.SeekMode.Start);
		await file.write(buf);

		this.fileOffset += buf.length;
	}

	async close(): Promise<void> {
		if (this.dataFile) {
			await this.dataFile.sync();
			this.dataFile.close();
			this.dataFile = undefined;
		}
		this.prepared = false;
		this.index.clear();
		this.blockCache.clear();
		this.fileOffset = 0;
		this.accessCounter = 0;
	}

	getStats() {
		const totalCacheSize = this.blockCache.values()
			.reduce((sum, block) => sum + block.data.length, 0);

		return {
			totalEntries: this.index.size,
			fileSize: this.fileOffset,
			cacheEntries: this.blockCache.size,
			cacheSize: totalCacheSize,
		};
	}

	// Private helpers

	private getBlockSize(): number {
		// Calculate block size based on entry size
		// Target ~200 entries per block, rounded to power of 2
		const entrySize = this.keyCodec.stride + this.valueCodec.stride;
		const targetSize = entrySize * 200;
		const powerOf2 = Math.pow(2, Math.ceil(Math.log2(targetSize)));
		return Math.max(4096, Math.min(65536, powerOf2));
	}

	private hashKey(key: Uint8Array): string {
		// Simple hash - could be improved
		let hash = 0;
		for (let i = 0; i < key.length; i++) {
			hash = ((hash << 5) - hash) + key[i]!;
			hash = hash & hash; // Convert to 32bit integer
		}
		return hash.toString(16);
	}

	private extractValueFromBlock(blockData: Uint8Array, entryOffset: number, blockStart: number): Uint8Array {
		const relativeOffset = entryOffset - blockStart;
		const valueStart = relativeOffset + this.keyCodec.stride;
		const valueEnd = valueStart + this.valueCodec.stride;
		return blockData.subarray(valueStart, valueEnd);
	}

	private addToCache(key: number, data: Uint8Array): void {
		// Evict oldest if cache is full
		if (this.blockCache.size >= this.maxCacheBlockCount) {
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

	private async loadExistingData(): Promise<void> {
		const file = this.dataFile!;
		const stat = await file.stat();
		const entrySize = this.keyCodec.stride + this.valueCodec.stride;

		if (stat.size % entrySize !== 0) {
			throw new Error(`Corrupt file: size ${stat.size} not divisible by entry size ${entrySize}`);
		}

		// Read file and build index
		// For large files, read in chunks
		const CHUNK_SIZE = 65536;
		let offset = 0;

		while (offset < stat.size) {
			const chunkSize = Math.min(CHUNK_SIZE, stat.size - offset);
			const chunk = new Uint8Array(chunkSize);
			await file.seek(offset, Deno.SeekMode.Start);
			await file.read(chunk);

			// Process entries in chunk
			for (let pos = 0; pos + entrySize <= chunkSize; pos += entrySize) {
				const keyBytes = chunk.subarray(pos, pos + this.keyCodec.stride);
				const keyHash = this.hashKey(keyBytes);
				this.index.set(keyHash, offset + pos);
			}

			offset += chunkSize;
		}

		this.fileOffset = stat.size;
	}
}
