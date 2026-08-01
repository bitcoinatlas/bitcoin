import { Codec } from "@nomadshiba/codec";
import { Advice, Mmap } from "@nomadshiba/mmap";
import { existsSync } from "@std/fs";
import { join } from "@std/path";
import { MiB, SECOND } from "~/constants.ts";
import { PARALLELISM_THREADS } from "~/env.ts";
import { Store } from "~/libs/storage/Store.ts";
import { CompressWorkerPool } from "./CompressWorkerPool.ts";

import { createReadStream, createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import zlib from "node:zlib";

const COMPRESS_PARALLELISM = Math.min(PARALLELISM_THREADS, Math.max(8, Math.floor(PARALLELISM_THREADS * .5)));

// node:zlib streams default to 16 KiB chunks — ~65k event-loop hops per ~1 GiB
// chunk. 8 MiB buffers cut that to ~130.
const INFLATE_STREAM_BUFFER_SIZE = 8 * MiB;
// The sync decode path allocates + Buffer.concat()s per chunkSize output buffer
// — bigger chunks mean fewer of both. Default is tiny.
const INFLATE_SYNC_CHUNK_SIZE = 64 * MiB;

const { constants } = zlib;

/** Map `{ compressionLevel: 19, ... }` -> `{ [ZSTD_c_compressionLevel]: 19 }`. */
function mapZstdParams(params: Record<string, number | undefined>, prefix: "ZSTD_c_" | "ZSTD_d_"): Record<number, number> {
	const out: Record<number, number> = {};
	for (const [key, value] of Object.entries(params)) {
		if (value === undefined) continue;
		out[(constants as Record<string, number>)[`${prefix}${key}`]!] = value;
	}
	return out;
}

function deleteTmpFiles(root: string): void {
	for (const entry of Deno.readDirSync(root)) {
		const path = join(root, entry.name);
		if (entry.isDirectory) {
			deleteTmpFiles(path);
		} else if (entry.isFile && entry.name.endsWith(".tmp")) {
			Deno.removeSync(path);
		}
	}
}

function chunkPath(root: string, index: number): string {
	return join(root, `chunk_${index}`);
}
function chunkZstPath(root: string, index: number): string {
	return `${chunkPath(root, index)}.zst`;
}
function chunkZstTmpPath(root: string, index: number): string {
	return `${chunkPath(root, index)}.zst.tmp`;
}
// Written then atomically renamed into place so a reader never sees a half-inflated chunk_N.
function chunkRawTmpPath(root: string, index: number): string {
	return `${chunkPath(root, index)}.raw.tmp`;
}
// Serialises inflate across workers/processes.
function chunkInflateLockPath(root: string, index: number): string {
	return `${chunkPath(root, index)}.lock`;
}

// Create (if missing) and/or grow chunk_N to exactly `maxChunkSize`, zero-filled. Idempotent.
// Throws if the chunk is somehow bigger than maxChunkSize (corruption).
function ensureChunkFile(root: string, index: number, maxChunkSize: number): void {
	const path = chunkPath(root, index);
	let size = -1;
	try {
		size = Deno.statSync(path).size;
	} catch (e) {
		if (!(e instanceof Deno.errors.NotFound)) throw e;
	}
	if (size === maxChunkSize) return;
	if (size > maxChunkSize) throw new Error(`chunk ${index} has a weird size size=${size}, expected at most ${maxChunkSize}`);
	Deno.openSync(path, { create: true, write: true }).close();
	Deno.truncateSync(path, maxChunkSize);
}

// Remove every on-disk form of a chunk (raw, compressed, tmp, lock). Each is optional.
function removeAllChunkForms(root: string, index: number): void {
	Deno.removeSync(chunkPath(root, index), { recursive: true });
	Deno.removeSync(chunkZstPath(root, index), { recursive: true });
	Deno.removeSync(chunkZstTmpPath(root, index), { recursive: true });
	Deno.removeSync(chunkRawTmpPath(root, index), { recursive: true });
	Deno.removeSync(chunkInflateLockPath(root, index), { recursive: true });
}

// Drop a chunk's raw form + inflate leftovers, keeping the .zst. Only called on
// compressed (below-cursor, frozen) chunks that a read inflated and are now
// being evicted — never mutated after compression, so the .zst is always current.
function revertChunkToCompressed(root: string, index: number): void {
	Deno.removeSync(chunkPath(root, index), { recursive: true });
	Deno.removeSync(chunkRawTmpPath(root, index), { recursive: true });
	Deno.removeSync(chunkInflateLockPath(root, index), { recursive: true });
}

// Drop a chunk's compressed form (+ tmp/lock leftovers), keeping the raw file.
// Used at startup to clean up a crash-left half-committed compress pass — raw
// is always at least as up to date as zst.
function discardStaleCompressedForm(root: string, index: number): void {
	Deno.removeSync(chunkZstPath(root, index), { recursive: true });
	Deno.removeSync(chunkZstTmpPath(root, index), { recursive: true });
	Deno.removeSync(chunkInflateLockPath(root, index), { recursive: true });
}

export type CompressionOptions = {
	maxInflatedChunkAge: number;
	maxInflatedChunks: number;
	zstd: {
		compress: { [K in keyof typeof constants as K extends `ZSTD_c_${infer U}` ? U : never]?: number };
		decompress: { [K in keyof typeof constants as K extends `ZSTD_d_${infer U}` ? U : never]?: number };
	};
};

export type BlobStoreOptions = {
	path: string;
	chunkSize: number;
	readonly: boolean;
};

type MappedChunk = { mapping: Mmap; bytes: Uint8Array };

export class BlobStore extends Store implements Disposable {
	public readonly path: string;
	public readonly chunkSize: number;
	private cursor: number;
	private readonly: boolean;
	private compression: CompressionOptions | undefined;
	private zstdCompressOptions: Record<number, number>;
	private zstdDecompressOptions: Record<number, number>;
	private zstdDecompressSyncOptions: zlib.ZstdOptions;
	private zstdDecompressStreamOptions: zlib.ZstdOptions;

	private constructor(options: BlobStoreOptions) {
		super();
		this.cursor = 0;
		this.readonly = options.readonly;
		this.path = options.path;
		this.chunkSize = options.chunkSize;
		this.zstdCompressOptions = {};
		this.zstdDecompressOptions = {};
		this.zstdDecompressSyncOptions = {
			chunkSize: INFLATE_SYNC_CHUNK_SIZE,
			maxOutputLength: options.chunkSize,
			params: this.zstdDecompressOptions,
		};
		this.zstdDecompressStreamOptions = {
			chunkSize: INFLATE_STREAM_BUFFER_SIZE,
			params: this.zstdDecompressOptions,
		};
	}

	static open(options: BlobStoreOptions): BlobStore {
		const self = new BlobStore(options);
		Deno.mkdirSync(self.path, { recursive: true });
		deleteTmpFiles(self.path);
		return self;
	}

	isReadOnly(): boolean {
		return this.readonly;
	}

	sync() {
		for (const map of this.chunkMaps) map?.mapping.flush();
	}

	size(): number {
		return this.cursor;
	}

	next(maxItemSize: number, from: number = this.size()): number {
		const room = this.chunkSize - (from % this.chunkSize);
		return room < maxItemSize ? from + room : from;
	}

	reveal(size: number): void {
		const current = this.size();
		if (size < current) {
			throw new RangeError([
				`reveal size=${size} is behind the cursor (size=${current}).`,
				`reveal can only move the cursor forward, never backward`,
			].join("\n"));
		}
		const oldTailIndex = Math.floor(current / this.chunkSize);
		const newTailIndex = Math.floor(size / this.chunkSize);
		for (let index = oldTailIndex; index <= newTailIndex; index++) {
			const hasZst = existsSync(chunkZstPath(this.path, index));
			const hasRaw = existsSync(chunkPath(this.path, index));
			if (hasZst && hasRaw) {
				discardStaleCompressedForm(this.path, index);
				continue;
			}
			ensureChunkFile(this.path, index, this.chunkSize);
		}
		this.cursor = size;
	}

	truncate(size: number): void {
		if (size < 0) throw new RangeError(`truncate size=${size} must be non-negative`);
		const current = this.size();
		const oldTailIndex = Math.floor(current / this.chunkSize);
		const newTailIndex = Math.floor(size / this.chunkSize);
		const tailEnd = size % this.chunkSize;

		// Delete high-to-low so a crash leaves a contiguous prefix [0..k],
		// never a gap — a gap would brick recovery (open() throws).
		for (let index = oldTailIndex; index > newTailIndex; index--) {
			this.closeChunkMap(index);
			const timer = this.inflatedTimers.get(index);
			if (timer !== undefined) {
				clearTimeout(timer);
				this.inflatedTimers.delete(index);
			}
			removeAllChunkForms(this.path, index);
		}

		// Zero the surviving tail's stale bytes: truncate down to tailEnd
		// (drops them), then back up to maxChunkSize (zero-fills).
		this.closeChunkMap(newTailIndex);
		const timer = this.inflatedTimers.get(newTailIndex);
		if (timer !== undefined) {
			clearTimeout(timer);
			this.inflatedTimers.delete(newTailIndex);
		}
		if (existsSync(chunkZstPath(this.path, newTailIndex))) Deno.removeSync(chunkZstPath(this.path, newTailIndex));
		ensureChunkFile(this.path, newTailIndex, this.chunkSize);
		Deno.truncateSync(chunkPath(this.path, newTailIndex), tailEnd);
		Deno.truncateSync(chunkPath(this.path, newTailIndex), this.chunkSize);
	}

	get<T extends Codec>(pointer: number, codec: T): [Codec.InferOutput<T>, number] {
		const [bytes, offset] = this.view(pointer);
		return codec.decode(bytes, offset);
	}

	async getAsync<T extends Codec>(pointer: number, codec: T): Promise<[Codec.InferOutput<T>, number]> {
		const [bytes, offset] = await this.viewAsync(pointer);
		return codec.decode(bytes, offset);
	}

	private view(pointer: number): [bytes: Uint8Array, offset: number] {
		const size = this.size();
		if (pointer >= size) {
			throw new Error(`yeah you wanna read from offset=${pointer}, but all i have is size=${size}`);
		}
		const index = Math.floor(pointer / this.chunkSize);
		const start = pointer % this.chunkSize;

		let map = this.getChunkMap(index);
		if (!map) {
			this.inflateChunkSync(index);
			map = this.getChunkMap(index);
			if (!map) throw new Error(`chunk ${index} vanished after inflate`);
		} else if (this.compression && this.inflatedTimers.has(index)) {
			this.touchInflated(index);
		}
		return [map.bytes, start];
	}

	private async viewAsync(pointer: number): Promise<[bytes: Uint8Array, offset: number]> {
		const size = this.size();
		if (pointer >= size) {
			throw new Error(`yeah you wanna read from offset=${pointer}, but all i have is size=${size}`);
		}
		const index = Math.floor(pointer / this.chunkSize);
		const start = pointer % this.chunkSize;

		let map = this.getChunkMap(index);
		if (!map) {
			await this.inflateChunkAsync(index);
			map = this.getChunkMap(index);
			if (!map) throw new Error(`chunk ${index} vanished after inflate`);
		} else if (this.compression && this.inflatedTimers.has(index)) {
			this.touchInflated(index);
		}
		return [map.bytes, start];
	}

	write(offset: number, bytes: Uint8Array): number {
		const size = this.size();
		if (offset < size) {
			throw new Error(
				`writeInto offset=${offset} is behind the cursor (size=${size}); writeInto can only fill space at or in front of the cursor, never overwrite live data`,
			);
		}

		const index = Math.floor(offset / this.chunkSize);
		const start = offset % this.chunkSize;
		const available = this.chunkSize - start;
		if (bytes.length > available) {
			throw new Error(
				`writeInto of ${bytes.length} bytes at offset=${offset} doesn't fit in chunk ${index}'s remaining ${available} bytes — a write may never straddle a chunk boundary`,
			);
		}

		ensureChunkFile(this.path, index, this.chunkSize);
		const map = this.getChunkMap(index);
		if (!map) throw new Error(`chunk ${index} missing its raw file for a write at offset=${offset}`);
		map.bytes.set(bytes, start);

		return bytes.byteLength;
	}

	mmap(begin?: number, length?: number) {
		const size = this.size();
		begin ??= size;
		if (begin < size) {
			throw new Error(
				`writeInto begin=${begin} is behind the cursor (size=${size}); writeInto can only fill space at or in front of the cursor, never overwrite live data`,
			);
		}

		const index = Math.floor(begin / this.chunkSize);
		const start = begin % this.chunkSize;
		const available = this.chunkSize - start;
		length ??= available;
		if (length > available || length < 0) {
			throw new Error(
				`writeInto of ${length} bytes at offset=${begin} doesn't fit in chunk ${index}'s remaining ${available} bytes — a write may never straddle a chunk boundary`,
			);
		}

		ensureChunkFile(this.path, index, this.chunkSize);
		const map = this.getChunkMap(index);
		if (!map) throw new Error(`chunk ${index} missing its raw file for a write at offset=${begin}`);
		return map.bytes.subarray(begin, begin + length);
	}

	// undefined = never probed. null = probed, raw file absent (compressed, not
	// yet inflated). MappedChunk = raw file present and mapped.
	// SIGBUS SAFETY: every site that shrinks/removes a raw chunk MUST call
	// closeChunkMap(index) — which unmaps — BEFORE the shrink/remove.
	private chunkMaps: (MappedChunk | null | undefined)[] = [];

	private closeChunkMap(index: number) {
		const map = this.chunkMaps[index];
		if (map) map.mapping.close();
		this.chunkMaps[index] = undefined;
	}

	private getChunkMap(index: number): MappedChunk | null {
		let map = this.chunkMaps[index];
		if (map !== undefined) return map;
		try {
			const mapping = Mmap.openSync(chunkPath(this.path, index), { write: true });
			mapping.advise(Advice.Random);
			map = { mapping, bytes: mapping.bytes() };
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
			map = null;
		}
		this.chunkMaps[index] = map;
		return map;
	}

	// We never keep decompressed bytes in memory — inflating means recreating
	// the raw chunk_N file on disk. `inflatedTimers` is the set of currently-
	// inflated compressible chunks, doubling as LRU order (JS Maps keep
	// insertion order; delete+re-set moves to MRU tail; head is LRU victim).
	// Bounded by a per-chunk TTL and a hard count cap. With compression
	// disabled, inflate is one-way: the .zst is deleted, the raw file is permanent.
	private inflatedTimers = new Map<number, ReturnType<typeof setTimeout>>();

	/** Mark `index` as freshly read: move it to MRU, (re)arm its TTL, cap count. */
	private touchInflated(index: number): void {
		if (!this.compression) return;
		const existing = this.inflatedTimers.get(index);
		if (existing !== undefined) {
			clearTimeout(existing);
			this.inflatedTimers.delete(index);
		} else {
			this.evictToCapacity(index);
		}
		const timer = setTimeout(() => {
			this.inflatedTimers.delete(index);
			this.evictInflatedChunk(index);
		}, this.compression.maxInflatedChunkAge);
		this.inflatedTimers.set(index, timer);
	}

	/** Evict LRU inflated chunks until a new one fits under the cap. */
	private evictToCapacity(incoming: number): void {
		const max = this.compression!.maxInflatedChunks;
		while (this.inflatedTimers.size >= max) {
			let victim: number | undefined;
			for (const key of this.inflatedTimers.keys()) {
				if (key === incoming || this.inflatingInFlight.has(key)) continue;
				victim = key;
				break;
			}
			if (victim === undefined) break;
			clearTimeout(this.inflatedTimers.get(victim)!);
			this.inflatedTimers.delete(victim);
			this.evictInflatedChunk(victim);
		}
	}

	private evictInflatedChunk(index: number): void {
		if (!existsSync(chunkZstPath(this.path, index))) return;
		this.closeChunkMap(index);
		this.chunkMaps[index] = null;
		revertChunkToCompressed(this.path, index);
	}

	private inflatingInFlight = new Map<number, Promise<void>>();

	// Inflate chunk_N.zst back to raw chunk_N on disk (sync). Blocks the event
	// loop for the decompress; does NOT retain bytes in memory. The per-chunk
	// lock serialises against other workers/processes.
	private inflateChunkSync(index: number): void {
		if (this.getChunkMap(index)) return;

		const lock = Deno.openSync(chunkInflateLockPath(this.path, index), { create: true, write: true, read: true });
		try {
			lock.lockSync(true);

			// Re-probe: another holder may have inflated it while we waited.
			this.chunkMaps[index] = undefined;
			if (this.getChunkMap(index)) return;

			console.log(`[compress] inflating chunk ${index} (sync)`);
			const started = performance.now();
			const compressed = Deno.readFileSync(chunkZstPath(this.path, index));
			const readMs = performance.now() - started;
			const raw = zlib.zstdDecompressSync(compressed, this.zstdDecompressSyncOptions);
			const decodeMs = performance.now() - started - readMs;
			const tmpPath = chunkRawTmpPath(this.path, index);
			Deno.writeFileSync(tmpPath, raw);
			Deno.renameSync(tmpPath, chunkPath(this.path, index));
			const writeMs = performance.now() - started - readMs - decodeMs;
			console.log(
				`[compress] chunk ${index} inflated, ${raw.byteLength} bytes on disk ` +
					`(read ${readMs.toFixed(0)}ms, decode ${decodeMs.toFixed(0)}ms, write ${writeMs.toFixed(0)}ms)`,
			);

			this.chunkMaps[index] = undefined;
			this.afterInflate(index);
		} finally {
			try {
				lock.unlockSync();
			} catch { /* fd closing drops the lock anyway */ }
			lock.close();
		}
	}

	private async inflateChunkAsync(index: number): Promise<void> {
		if (this.getChunkMap(index)) return;

		const inFlight = this.inflatingInFlight.get(index);
		if (inFlight) return inFlight;

		const promise = (async () => {
			const lock = Deno.openSync(chunkInflateLockPath(this.path, index), { create: true, write: true, read: true });
			try {
				await lock.lock(true);
				this.chunkMaps[index] = undefined;
				if (this.getChunkMap(index)) return;

				console.log(`[compress] inflating chunk ${index} (async, streaming)`);
				const started = performance.now();
				const tmpPath = chunkRawTmpPath(this.path, index);
				// Stream .zst -> zstd inflate -> raw tmp. Bounded peak memory
				// (a handful of 8 MiB buffers, never the whole chunk).
				const source = createReadStream(chunkZstPath(this.path, index), { highWaterMark: INFLATE_STREAM_BUFFER_SIZE });
				const transform = zlib.createZstdDecompress(this.zstdDecompressStreamOptions);
				const sink = createWriteStream(tmpPath, { highWaterMark: INFLATE_STREAM_BUFFER_SIZE });
				await pipeline(source, transform, sink);
				await Deno.rename(tmpPath, chunkPath(this.path, index));
				const ms = (performance.now() - started).toFixed(0);
				console.log(`[compress] chunk ${index} inflated on disk ${ms}ms`);

				this.chunkMaps[index] = undefined;
				this.afterInflate(index);
			} finally {
				try {
					await lock.unlock();
				} catch { /* fd closing drops the lock anyway */ }
				lock.close();
			}
		})();
		this.inflatingInFlight.set(index, promise);
		try {
			await promise;
		} finally {
			this.inflatingInFlight.delete(index);
		}
	}

	private afterInflate(index: number): void {
		if (this.compression) {
			this.touchInflated(index);
		} else {
			Deno.removeSync(chunkZstPath(this.path, index), { recursive: true });
		}
	}

	private disposed = false;
	private compressPool: CompressWorkerPool | undefined;

	/**
	 * Start the background compression loop. Throws if called twice on the same
	 * instance. Runs for the process lifetime, stopped via `close()`.
	 */
	startCompression(options: CompressionOptions): void {
		if (this.compression) throw new Error("compression already started on this store");
		if (options.maxInflatedChunks < 1) throw new Error("compression.maxInflatedChunks must be >= 1");

		this.compression = options;
		this.zstdCompressOptions = mapZstdParams(options.zstd.compress, "ZSTD_c_");
		this.zstdDecompressOptions = mapZstdParams(options.zstd.decompress, "ZSTD_d_");
		this.zstdDecompressSyncOptions = { ...this.zstdDecompressSyncOptions, params: this.zstdDecompressOptions };
		this.zstdDecompressStreamOptions = { ...this.zstdDecompressStreamOptions, params: this.zstdDecompressOptions };

		this.runCompressionLoop().catch((e) => {
			console.error("[compress] background loop died:", e);
		});
	}

	private async runCompressionLoop(): Promise<void> {
		const pool = new CompressWorkerPool(COMPRESS_PARALLELISM);
		this.compressPool = pool;

		const inFlight = new Map<number, Promise<void>>();

		try {
			while (!this.disposed) {
				let dispatchedSomething = false;
				const tailIndex = Math.floor(this.size() / this.chunkSize);

				for (let index = 0; index < tailIndex && !this.disposed; index++) {
					if (inFlight.has(index)) continue;
					if (existsSync(chunkZstPath(this.path, index))) continue;

					let rawStat: Deno.FileInfo;
					try {
						rawStat = await Deno.stat(chunkPath(this.path, index));
					} catch (e) {
						if (e instanceof Deno.errors.NotFound) continue;
						throw e;
					}

					const promise = this.compressChunk(pool, index, rawStat.size)
						.catch((e) => {
							console.error(`[compress] chunk ${index} failed:`, e);
						})
						.finally(() => {
							inFlight.delete(index);
						});
					inFlight.set(index, promise);
					dispatchedSomething = true;
				}

				if (inFlight.size > 0) await Promise.all(inFlight.values());

				if (!dispatchedSomething) {
					await new Promise((resolve) => setTimeout(resolve, 10 * SECOND));
				}
			}

			await Promise.allSettled(inFlight.values());
		} finally {
			pool.dispose();
			this.compressPool = undefined;
		}
	}

	private async compressChunk(pool: CompressWorkerPool, index: number, rawSize: number): Promise<void> {
		const rawPath = chunkPath(this.path, index);
		const tmpPath = chunkZstTmpPath(this.path, index);
		const zstPath = chunkZstPath(this.path, index);

		const started = performance.now();

		// This chunk is strictly below the tail and writeInto only touches
		// at/above the cursor — nothing can mutate these bytes mid-compress.
		const zstSize = await pool.compress(index, rawPath, tmpPath, this.zstdCompressOptions);

		if (this.disposed) {
			return;
		}

		// Commit under the inflate lock — keeps a concurrent read's inflate
		// from racing our unmap+remove of the raw file.
		const lock = Deno.openSync(chunkInflateLockPath(this.path, index), { create: true, write: true, read: true });
		try {
			lock.lockSync(true);
			this.closeChunkMap(index);
			Deno.renameSync(tmpPath, zstPath);
			Deno.removeSync(rawPath);
			this.chunkMaps[index] = null;
		} finally {
			try {
				lock.unlockSync();
			} catch { /* fd closing drops the lock anyway */ }
			lock.close();
		}

		const ms = (performance.now() - started).toFixed(0);
		const ratio = (zstSize / rawSize).toFixed(4);
		console.log(`[compress] chunk ${index} done: ${rawSize} -> ${zstSize} bytes (ratio=${ratio}, ${ms}ms)`);
	}

	close() {
		this.disposed = true;
		this.compressPool?.dispose();
		this.compressPool = undefined;
		for (const map of this.chunkMaps) map?.mapping.close();
		this.chunkMaps.length = 0;
		for (const timer of this.inflatedTimers.values()) clearTimeout(timer);
		this.inflatedTimers.clear();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
