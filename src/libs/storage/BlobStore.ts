import { RocksDatabase, Transaction } from "@harperfast/rocksdb-js";
import { Codec, U64 } from "@nomadshiba/codec";
import { join } from "@std/path";
import { createReadStream, createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import zlib from "node:zlib";
import { MAX_BLOCK_SIZE, SECOND } from "~/constants.ts";
import { PARALLELISM_THREADS } from "~/env.ts";
import { readFileInto, readFileIntoSync, writeFileSync } from "~/libs/fs/mod.ts";
import { StoreAppendOnly } from "~/libs/storage/Store.ts";
import { CompressWorkerPool } from "./CompressWorkerPool.ts";

const COMPRESS_PARALLELISM = Math.min(PARALLELISM_THREADS, Math.max(8, Math.floor(PARALLELISM_THREADS * .5)));

const MiB = 1024 * 1024;

// node:zlib streams default to 64 KiB reads and 16 KiB output chunks — for a
// ~1 GiB chunk that's ~65k transform chunks, each one bounced through the
// event loop with its own callback/promise machinery. 8 MiB buffers cut that
// to ~130 hops while keeping peak memory bounded (a few in-flight buffers).
const INFLATE_STREAM_BUFFER_SIZE = 8 * MiB;

// The one-shot sync decode allocates an internal output buffer per chunkSize
// and Buffer.concat()s them at the end. The concat memcpy costs the same
// regardless of chunk count, but the allocation + list bookkeeping doesn't —
// bigger chunks mean fewer of both. (Default is tiny.)
const INFLATE_SYNC_CHUNK_SIZE = 64 * MiB;

const { constants } = zlib;

export type CompressionOptions = {
	/**
	 * How long (ms) a chunk that was inflated back to its raw form on disk is
	 * kept before it's deleted again (reverting to compressed-only). Each read
	 * re-arms the timer.
	 */
	maxInflatedChunkAge: number;
	/**
	 * Hard cap on how many chunks may be inflated (raw on disk) at once. When a
	 * new inflate would push past it, the least-recently-read inflated chunk is
	 * reverted to compressed-only first (LRU). Set it >= the number of chunks a
	 * single read can straddle (2 in practice) so a read never evicts a chunk
	 * it's still walking.
	 */
	maxInflatedChunks: number;
	zstd: {
		compress: { [K in keyof typeof constants as K extends `ZSTD_c_${infer U}` ? U : never]?: number };
		decompress: { [K in keyof typeof constants as K extends `ZSTD_d_${infer U}` ? U : never]?: number };
	};
};

export type BlobStoreOptions = {
	path: string;
	rocksdb: RocksDatabase;
	maxChunkSize: number;
	writable: boolean;
	compression?: CompressionOptions;
};

/** Remove a file, treating "already gone" as success. */
function removeIfExistsSync(path: string): void {
	try {
		Deno.removeSync(path);
	} catch (e) {
		if (!(e instanceof Deno.errors.NotFound)) throw e;
	}
}

function existsSync(path: string): boolean {
	try {
		Deno.statSync(path);
		return true;
	} catch (e) {
		if (e instanceof Deno.errors.NotFound) return false;
		throw e;
	}
}

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

export class BlobStore extends StoreAppendOnly implements Disposable {
	public readonly path: string;
	public override readonly rocksdb: RocksDatabase;

	private disk: DiskRegion;

	private constructor(disk: DiskRegion, options: BlobStoreOptions) {
		super();
		this.disk = disk;
		this.path = options.path;
		this.rocksdb = options.rocksdb;
	}

	static open(options: BlobStoreOptions): BlobStore {
		const { path, maxChunkSize, writable, compression } = options;
		deleteTmpFiles(path);
		return new BlobStore(DiskRegion.open({ path, maxChunkSize, writable, compression }), options);
	}

	get<T extends Codec<any>>(pointer: number, codec: T, options?: { readAheadSize?: number }): Codec.InferOutput<T> {
		const needed = codec.stride.size ?? options?.readAheadSize ?? MAX_BLOCK_SIZE;
		const output = new Uint8Array(needed);
		const filled = this.disk.readInto(pointer, needed, output);
		const [decoded] = codec.decode(filled === needed ? output : output.subarray(0, filled));
		return decoded;
	}

	async getAsync<T extends Codec<any>>(
		pointer: number,
		codec: T,
		options?: { readAheadSize?: number },
	): Promise<Codec.InferOutput<T>> {
		const needed = codec.stride.size ?? options?.readAheadSize ?? MAX_BLOCK_SIZE;
		const output = new Uint8Array(needed);
		const filled = await this.disk.readIntoAsync(pointer, needed, output);
		const [decoded] = codec.decode(filled === needed ? output : output.subarray(0, filled));
		return decoded;
	}

	append(data: Uint8Array): number {
		const offset = this.size();
		this.disk.append(data);
		return offset;
	}

	size(): number {
		return this.disk.size;
	}

	/**
	 * Reader-only: advance the visible size to the latest committed pin
	 * (`rollback.size`). A long-lived reader opened early in IBD sees ~nothing;
	 * calling this before a read reveals everything the writer has pinned since.
	 * `rollback.size` is monotonic and only ever names synced+committed bytes, so
	 * a reader bounded to it never observes provisional (rollback-able) data.
	 * No-op on the writable opener, which owns `size` directly via append().
	 */
	refresh(): void {
		const bytes = this.rocksdb.getSync("rollback.size") as Uint8Array | undefined;
		this.disk.reveal(bytes ? Number(U64.decode(bytes)[0]) : 0);
	}

	pin(transaction?: Transaction): void {
		this.disk.sync();
		this.rocksdb.putSync("rollback.size", U64.encode(this.disk.size), { transaction });
	}

	rollback(transaction?: Transaction): void {
		const bytes = this.rocksdb.getSync("rollback.size", { transaction }) as Uint8Array | undefined;
		this.truncate(bytes ? Number(U64.decode(bytes)[0]) : 0);
	}

	truncate(size: number): void {
		this.disk.truncate(size);
	}

	close(): void {
		this.disk.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}

type DiskRegionOptions = {
	path: string;
	maxChunkSize: number;
	writable: boolean;
	compression?: CompressionOptions;
};

class DiskRegion implements Disposable {
	public readonly path: string;
	public readonly maxChunkSize: number;
	private readonly writable: boolean;
	private readonly compression: CompressionOptions | undefined;
	private readonly zstdCompressOptions: Record<number, number>;
	private readonly zstdDecompressOptions: Record<number, number>;
	// Prebuilt option objects for the two decode paths — same params, different
	// buffer strategies. See the INFLATE_* constants at the top.
	private readonly zstdDecompressSyncOptions: zlib.ZstdOptions;
	private readonly zstdDecompressStreamOptions: zlib.ZstdOptions;

	public size: number;

	// --- chunk paths ----------------------------------------------------------
	private chunkPathCache: string[] = [];
	private chunkPath(index: number) {
		return this.chunkPathCache[index] ??= join(this.path, `chunk_${index}`);
	}
	private chunkPathZst(index: number) {
		return `${this.chunkPath(index)}.zst`;
	}
	private chunkPathZstTmp(index: number) {
		return `${this.chunkPath(index)}.zst.tmp`;
	}
	// Raw chunk being reconstructed from its .zst — written here then atomically
	// renamed into place so a reader never sees a half-inflated chunk_N.
	private chunkPathRawTmp(index: number) {
		return `${this.chunkPath(index)}.raw.tmp`;
	}
	// Per-chunk advisory lock held for the duration of an inflate so two workers
	// (or two reads in this process) don't decompress the same chunk twice.
	private chunkPathInflateLock(index: number) {
		return `${this.chunkPath(index)}.inflate.lock`;
	}

	private lockFile: Deno.FsFile | undefined;

	private constructor(options: DiskRegionOptions) {
		this.path = options.path;
		this.maxChunkSize = options.maxChunkSize;
		this.writable = options.writable;
		this.compression = options.compression;
		this.zstdCompressOptions = options.compression ? mapZstdParams(options.compression.zstd.compress, "ZSTD_c_") : {};
		this.zstdDecompressOptions = options.compression ? mapZstdParams(options.compression.zstd.decompress, "ZSTD_d_") : {};
		this.zstdDecompressSyncOptions = {
			chunkSize: INFLATE_SYNC_CHUNK_SIZE,
			// Sealed raw chunks are exactly maxChunkSize — a hard output bound is
			// free corruption detection on top of the frame checksum.
			maxOutputLength: options.maxChunkSize,
			params: this.zstdDecompressOptions,
		};
		this.zstdDecompressStreamOptions = {
			chunkSize: INFLATE_STREAM_BUFFER_SIZE,
			params: this.zstdDecompressOptions,
		};
		if (options.compression && options.compression.maxInflatedChunks < 1) {
			throw new Error("compression.maxInflatedChunks must be >= 1");
		}
		this.size = 0;
	}

	static open(options: DiskRegionOptions): DiskRegion {
		const self = new DiskRegion(options);
		Deno.mkdirSync(self.path, { recursive: true });

		let tailIndex = 0;
		const indexSet = new Set<number>([0]);
		for (const file of Deno.readDirSync(self.path)) {
			if (!file.isFile) continue;
			if (!file.name.startsWith("chunk_")) continue;
			if (file.name.endsWith(".tmp")) continue; // stray leftover from a crashed compression pass
			// chunk_N or chunk_N.zst — strip a trailing .zst before parsing the index
			const name = file.name.endsWith(".zst") ? file.name.slice(0, -".zst".length) : file.name;
			const index = Number(name.slice("chunk_".length));
			if (!Number.isInteger(index)) continue;
			indexSet.add(index);
			if (index > tailIndex) tailIndex = index;
		}

		for (let index = 0; index < tailIndex; index++) {
			if (!indexSet.has(index)) throw new Error("bro your chunks are fucked, has some gaps and stuff");

			const hasZst = existsSync(self.chunkPathZst(index));
			const hasRaw = existsSync(self.chunkPath(index));

			// Startup eviction: a SEALED chunk present as both raw and compressed is
			// a leftover inflation from before a restart — revert it to compressed-
			// only so inflated chunks don't survive restarts and leak disk.
			if (hasZst && hasRaw) {
				self.revertToCompressed(index);
				continue;
			}
			// Raw-only sealed chunk must be exactly maxChunkSize; a compressed
			// chunk's on-disk size is expected to differ, that's the whole point.
			if (hasRaw) {
				const size = Deno.statSync(self.chunkPath(index)).size;
				if (size !== self.maxChunkSize) throw new Error(`chunk ${index} has a weird size size=${size}`);
				continue;
			}
			if (!hasZst) throw new Error(`chunk ${index} is missing both raw and compressed forms`);
		}

		// The tail is always raw (compression never touches the active chunk).
		let tailChunkSize = 0;
		try {
			const tailChunkStat = Deno.statSync(self.chunkPath(tailIndex));
			if (tailChunkStat.size > self.maxChunkSize) {
				throw new Error(`your tail chunk is fat... size=${tailChunkStat.size}`);
			}
			tailChunkSize = tailChunkStat.size;
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
		}

		self.appender = {
			index: tailIndex,
			file: Deno.openSync(self.chunkPath(tailIndex), { create: true, append: true }),
		};
		self.appender.file.seekSync(0, Deno.SeekMode.Start);
		self.size = tailIndex * self.maxChunkSize + tailChunkSize;

		// Single-writer guard — ONLY for writable opens. Readers (on-demand block
		// data during IBD, spender indexer, API/explorer) open the same store
		// definition without a lock and coexist freely. Exactly one writable
		// opener per data dir is allowed; a second writer fails loudly here
		// instead of racing the first one's appends/compression pass.
		if (!self.writable) return self;
		const lockPath = join(self.path, "LOCK");
		const lockFile = Deno.openSync(lockPath, { create: true, write: true, read: true });
		try {
			// OS-level advisory lock (flock under the hood). The OS releases it
			// automatically if this process dies or the fd closes — no stale
			// lockfile/PID bookkeeping needed.
			// CAVEAT: confirm on your Deno version whether lockSync(true) blocks
			// until available or fails fast. If it blocks, wrap this in a
			// short-timeout race so a second writer fails loudly instead of
			// hanging forever. (Run the /tmp/lock_probe.ts check.)
			lockFile.lockSync(true);
		} catch (e) {
			lockFile.close();
			throw new Error(`another process already has this store open for writing (lock at ${lockPath}): ${e}`);
		}
		self.lockFile = lockFile;

		if (self.compression) {
			// Only the single writable opener runs compression — it deletes and
			// rewrites chunk files, which is a write. Readers never touch it.
			// Fire-and-forget: runs for the process lifetime, stopped via `disposed` in close().
			self.runCompressionLoop().catch((e) => {
				console.error("[compress] background loop died:", e);
			});
		}

		return self;
	}

	private appender!: { file: Deno.FsFile; index: number };
	private appending = false;
	append(bytes: Uint8Array): void {
		if (this.appending) throw new Error("you are trying to append back to back, check your logic");
		if (this.truncating) throw new Error("you are trying to append while truncating, check your logic");
		this.appending = true;
		try {
			let size = this.size;
			let appended = 0;
			while (appended < bytes.length) {
				const want = bytes.length - appended;
				const index = Math.floor(size / this.maxChunkSize);
				const taken = size % this.maxChunkSize;
				const available = this.maxChunkSize - taken;
				const append = Math.min(want, available);

				if (this.appender.index !== index) {
					this.appender.file.close();
					this.appender.file = Deno.openSync(this.chunkPath(index), { create: true, append: true });
					this.appender.index = index;
				}

				writeFileSync(this.appender.file, bytes.subarray(appended, appended + append));
				appended += append;
				size += append;
			}
			this.size = size;
		} finally {
			this.appending = false;
		}
	}

	sync() {
		this.appender.file.syncSync();
	}

	/**
	 * Reader-only: set the visible size to `size` (the committed pin). Writers own
	 * `size` through append() and must not have it moved out from under them, so
	 * this is a no-op when writable. `size` is the authoritative bound for a
	 * reader — chunk files for the newly-revealed region are opened lazily by
	 * getRawReader on first read.
	 */
	reveal(size: number): void {
		if (this.writable) return;
		this.size = size;
	}

	// --- raw chunk readers ---------------------------------------------------
	// undefined = never probed / needs re-probe. null = probed, raw file absent
	// (chunk is compressed and not yet inflated). A live fd = raw file present.
	private readers: (Deno.FsFile | null | undefined)[] = [];

	private closeRawReader(index: number) {
		const reader = this.readers[index];
		if (reader) reader.close();
		this.readers[index] = undefined;
	}

	private getRawReader(index: number): Deno.FsFile | null {
		let reader = this.readers[index];
		if (reader !== undefined) return reader;
		try {
			reader = Deno.openSync(this.chunkPath(index), { read: true });
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
			reader = null;
		}
		this.readers[index] = reader;
		return reader;
	}

	// --- inflated-chunk lifetime ----------------------------------------------
	// We NEVER keep decompressed bytes in memory. Decompressing a chunk means
	// recreating its raw chunk_N file on disk; subsequent reads then hit that
	// file through the normal raw-reader path. The transient decompressed buffer
	// is written straight out and dropped — nothing is cached in RAM.
	//
	// `inflatedTimers` is the set of currently-inflated compressible chunks, and
	// doubles as an LRU order: JS Maps keep insertion order, so we delete+re-set
	// a key on touch to move it to the tail (MRU); the head is the LRU victim.
	// Bounded two ways — a per-chunk TTL (decompressedMaxAge) and a hard count
	// cap (maxInflatedChunks). Eviction deletes the raw file (the .zst stays, so
	// it re-inflates on the next read). With compression disabled, inflating is
	// one-way: the .zst is deleted and the raw file is permanent (no tracking).
	private inflatedTimers = new Map<number, ReturnType<typeof setTimeout>>();

	/** Mark `index` as freshly read: move it to MRU, (re)arm its TTL, cap count. */
	private touchInflated(index: number): void {
		if (!this.compression) return; // one-way inflate; nothing to track/evict
		const existing = this.inflatedTimers.get(index);
		if (existing !== undefined) {
			clearTimeout(existing);
			this.inflatedTimers.delete(index); // re-inserted below at the MRU tail
		} else {
			this.evictToCapacity(index); // new entry — make room first
		}
		const timer = setTimeout(() => {
			this.inflatedTimers.delete(index);
			this.evictInflatedChunk(index);
		}, this.compression.maxInflatedChunkAge);
		this.inflatedTimers.set(index, timer);
	}

	/** Evict least-recently-read inflated chunks until a new one fits under the cap. */
	private evictToCapacity(incoming: number): void {
		const max = this.compression!.maxInflatedChunks;
		while (this.inflatedTimers.size >= max) {
			// LRU victim = first key, skipping the incoming chunk and any chunk with
			// an inflate still in flight. If nothing is safe to drop, allow a
			// temporary overshoot rather than spin.
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

	// Revert a chunk to compressed-only: drop its raw form + inflate leftovers,
	// keeping the .zst. Safe if the raw form is already gone.
	private revertToCompressed(index: number): void {
		removeIfExistsSync(this.chunkPath(index));
		removeIfExistsSync(this.chunkPathRawTmp(index));
		removeIfExistsSync(this.chunkPathInflateLock(index));
	}

	// Runtime eviction of an inflated chunk: only if a .zst exists to fall back
	// to, then detach the reader before deleting the raw file.
	private evictInflatedChunk(index: number): void {
		if (!existsSync(this.chunkPathZst(index))) return; // live raw chunk, don't touch
		this.closeRawReader(index);
		this.readers[index] = null;
		this.revertToCompressed(index);
	}

	// If two concurrent async reads miss for the same chunk, the second one
	// piggybacks on the first's in-flight inflate instead of starting a
	// redundant one.
	private inflatingInFlight = new Map<number, Promise<void>>();

	// Inflate chunk_N.zst back to the raw chunk_N file on disk (sync). The
	// per-chunk lock file serialises this against other workers/processes: a
	// second worker blocks on the lock, then finds the raw file already present
	// and skips the work. "Sync means sync" — this blocks the event loop for the
	// duration of the decompress, but it does NOT retain the bytes in memory.
	private inflateChunkSync(index: number): void {
		if (this.getRawReader(index)) return; // already raw, nothing to do

		const lock = Deno.openSync(this.chunkPathInflateLock(index), { create: true, write: true, read: true });
		try {
			lock.lockSync(true); // blocks until the other inflater releases
			// Re-probe: another holder may have inflated it while we waited.
			this.readers[index] = undefined;
			if (this.getRawReader(index)) return;

			console.log(`[compress] inflating chunk ${index} (sync)`);
			const started = performance.now();
			const compressed = Deno.readFileSync(this.chunkPathZst(index));
			const readMs = performance.now() - started;
			const raw = zlib.zstdDecompressSync(compressed, this.zstdDecompressSyncOptions);
			const decodeMs = performance.now() - started - readMs;
			const tmpPath = this.chunkPathRawTmp(index);
			Deno.writeFileSync(tmpPath, raw);
			Deno.renameSync(tmpPath, this.chunkPath(index)); // atomic swap-in
			const writeMs = performance.now() - started - readMs - decodeMs;
			console.log(
				`[compress] chunk ${index} inflated, ${raw.byteLength} bytes on disk ` +
					`(read ${readMs.toFixed(0)}ms, decode ${decodeMs.toFixed(0)}ms, write ${writeMs.toFixed(0)}ms)`,
			);

			this.readers[index] = undefined; // force re-open of the new raw file
			this.afterInflate(index);
		} finally {
			try {
				lock.unlockSync();
			} catch { /* fd closing drops the lock anyway */ }
			lock.close();
		}
	}

	private async inflateChunkAsync(index: number): Promise<void> {
		if (this.getRawReader(index)) return;

		const inFlight = this.inflatingInFlight.get(index);
		if (inFlight) return inFlight;

		const promise = (async () => {
			const lock = Deno.openSync(this.chunkPathInflateLock(index), { create: true, write: true, read: true });
			try {
				await lock.lock(true); // blocks until the other inflater releases
				this.readers[index] = undefined;
				if (this.getRawReader(index)) return;

				console.log(`[compress] inflating chunk ${index} (async, streaming)`);
				const started = performance.now();
				const tmpPath = this.chunkPathRawTmp(index);
				// Stream .zst -> zstd inflate transform -> raw tmp file. Bounded peak
				// memory (a handful of 8 MiB buffers in flight, never the whole
				// chunk). The decode itself runs on the libuv threadpool, but every
				// output chunk hops back through the event loop to reach the sink —
				// which is why the buffer sizes matter: at the 16 KiB default this
				// was ~65k hops per chunk, at 8 MiB it's ~130.
				const source = createReadStream(this.chunkPathZst(index), { highWaterMark: INFLATE_STREAM_BUFFER_SIZE });
				const transform = zlib.createZstdDecompress(this.zstdDecompressStreamOptions);
				const sink = createWriteStream(tmpPath, { highWaterMark: INFLATE_STREAM_BUFFER_SIZE });
				await pipeline(source, transform, sink);
				await Deno.rename(tmpPath, this.chunkPath(index)); // atomic swap-in
				const ms = (performance.now() - started).toFixed(0);
				console.log(`[compress] chunk ${index} inflated on disk ${ms}ms`);

				this.readers[index] = undefined;
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

	// Post-inflate bookkeeping shared by the sync and async paths.
	private afterInflate(index: number): void {
		if (this.compression) {
			// Track it: arm the TTL and enforce the count cap (may evict an older one).
			this.touchInflated(index);
		} else {
			// One-way inflate: the raw file is now the source of truth, drop .zst.
			removeIfExistsSync(this.chunkPathZst(index));
		}
	}

	readInto(offset: number, length: number, target: Uint8Array): number {
		if (offset >= this.size) {
			throw new Error(`yeah you wanna read from offset=${offset}, but all i have is size=${this.size}`);
		}
		const clamped = Math.min(length, this.size - offset);

		let copied = 0;
		while (copied < clamped) {
			const want = clamped - copied;
			const index = Math.floor(offset / this.maxChunkSize);
			const start = offset % this.maxChunkSize;
			const available = this.maxChunkSize - start;
			const read = Math.min(want, available);

			let reader = this.getRawReader(index);
			if (!reader) {
				// Compressed: recreate the raw chunk file on disk, then read it.
				this.inflateChunkSync(index);
				reader = this.getRawReader(index);
				if (!reader) throw new Error(`chunk ${index} vanished after inflate`);
			} else if (this.compression && this.inflatedTimers.has(index)) {
				// Touched a raw-but-compressible chunk — push its eviction back.
				this.touchInflated(index);
			}
			reader.seekSync(start, Deno.SeekMode.Start);
			readFileIntoSync(reader, target.subarray(copied, copied + read));

			copied += read;
			offset += read;
		}
		return copied;
	}

	async readIntoAsync(offset: number, length: number, target: Uint8Array): Promise<number> {
		if (offset >= this.size) {
			throw new Error(`yeah you wanna read from offset=${offset}, but all i have is size=${this.size}`);
		}
		const clamped = Math.min(length, this.size - offset);

		let copied = 0;
		while (copied < clamped) {
			const want = clamped - copied;
			const index = Math.floor(offset / this.maxChunkSize);
			const start = offset % this.maxChunkSize;
			const available = this.maxChunkSize - start;
			const read = Math.min(want, available);

			let reader = this.getRawReader(index);
			if (!reader) {
				// Compressed: stream-inflate the raw chunk file to disk, then read it.
				await this.inflateChunkAsync(index);
				reader = this.getRawReader(index);
				if (!reader) throw new Error(`chunk ${index} vanished after inflate`);
			} else if (this.compression && this.inflatedTimers.has(index)) {
				this.touchInflated(index);
			}
			// NOTE: concurrent async reads against the SAME chunk index race on
			// this seek+read pair — fine while callers are single-flight per
			// pointer; add a per-index queue/mutex if that stops being true.
			await reader.seek(start, Deno.SeekMode.Start);
			await readFileInto(reader, target.subarray(copied, copied + read));

			copied += read;
			offset += read;
		}
		return copied;
	}

	// Remove every on-disk form of a chunk (raw, compressed, and any transient
	// tmp/lock leftovers). Each is optional — missing forms are ignored.
	private removeAllForms(index: number): void {
		removeIfExistsSync(this.chunkPath(index));
		removeIfExistsSync(this.chunkPathZst(index));
		removeIfExistsSync(this.chunkPathZstTmp(index));
		removeIfExistsSync(this.chunkPathRawTmp(index));
		removeIfExistsSync(this.chunkPathInflateLock(index));
	}

	private truncating: boolean = false;
	truncate(size: number): void {
		if (this.appending) throw new Error("you cant truncate while appending");
		if (this.truncating) throw new Error("you are trying to truncate back to back, check your logic");
		if (size > this.size) throw new Error(`cant truncate upward size=${size} current=${this.size}`);
		this.truncating = true;

		const oldTail = this.appender.index;
		const newTail = Math.floor(size / this.maxChunkSize);
		const tailEnd = size % this.maxChunkSize;

		// close() tears down readers, timers, appender and the compression pool,
		// so nothing below races a background pass on the chunks we're dropping.
		this.close();

		// FIX #5: delete high-to-low. If a crash happens mid-truncate, the
		// surviving chunks are always a contiguous prefix [0..k], never a set
		// with a gap. A gap would make DiskRegion.open throw ("chunks are
		// fucked") and brick recovery. High-to-low guarantees no gap.
		for (let index = oldTail; index > newTail; index--) this.removeAllForms(index);

		// FIX #1: when size lands exactly on a chunk boundary (tailEnd === 0),
		// chunk `newTail` may not exist yet (it is only created when data first
		// spills into it). Create-or-truncate it to length 0 explicitly instead
		// of calling Deno.truncateSync on a possibly-missing file.
		if (tailEnd === 0) {
			using _ = Deno.openSync(this.chunkPath(newTail), { create: true, write: true, truncate: true });
		} else {
			Deno.truncateSync(this.chunkPath(newTail), tailEnd);
		}

		this.appender.file = Deno.openSync(this.chunkPath(newTail), { create: true, append: true });
		this.appender.index = newTail;

		this.size = size;

		// Should never fail to this point.
		// If it did data is corrupted
		// So that's why we are not doing try/catch
		this.truncating = false;
	}

	// --- background compression loop ------------------------------------------
	private disposed = false;
	private compressPool: CompressWorkerPool | undefined;

	private async runCompressionLoop(): Promise<void> {
		const pool = new CompressWorkerPool(COMPRESS_PARALLELISM);
		this.compressPool = pool;

		// index -> in-flight promise, so we never dispatch the same chunk twice.
		const inFlight = new Map<number, Promise<void>>();

		try {
			while (!this.disposed) {
				let dispatchedSomething = false;
				// Never touch the active tail chunk — it's still being appended to.
				// Re-read on every outer pass since the tail advances as IBD progresses.
				const tailIndex = this.appender.index;

				for (let index = 0; index < tailIndex && !this.disposed; index++) {
					if (inFlight.has(index)) continue; // already being compressed
					if (existsSync(this.chunkPathZst(index))) continue; // already compressed

					let rawStat: Deno.FileInfo;
					try {
						rawStat = await Deno.stat(this.chunkPath(index));
					} catch (e) {
						if (e instanceof Deno.errors.NotFound) continue; // shouldn't happen — skip rather than crash the loop
						throw e;
					}

					// Dispatch to the pool. The pool queues internally and only runs
					// PARALLELISM jobs at once, so we don't need to gate here — we just
					// fire the job and track its completion for bookkeeping.
					const promise = this.compressChunk(pool, index, rawStat.size)
						.catch((e) => {
							// Don't let one failed chunk kill the loop; log and move on.
							console.error(`[compress] chunk ${index} failed:`, e);
						})
						.finally(() => {
							inFlight.delete(index);
						});
					inFlight.set(index, promise);
					dispatchedSomething = true;
				}

				// Drain outstanding work before the next scan so the "nothing to do"
				// back-off decision sees an accurate picture.
				if (inFlight.size > 0) await Promise.all(inFlight.values());

				// Nothing left to do this pass — back off before rescanning for newly sealed chunks.
				if (!dispatchedSomething) {
					await new Promise((resolve) => setTimeout(resolve, 10 * SECOND));
				}
			}

			// Let any stragglers finish so we don't leave half-written .tmp files.
			await Promise.allSettled(inFlight.values());
		} finally {
			pool.dispose();
			this.compressPool = undefined;
		}
	}

	private async compressChunk(pool: CompressWorkerPool, index: number, rawSize: number): Promise<void> {
		const rawPath = this.chunkPath(index);
		const tmpPath = this.chunkPathZstTmp(index);
		const zstPath = this.chunkPathZst(index);

		const started = performance.now();

		// The worker streams raw -> zstd -> tmp on its own OS thread; nothing large
		// crosses the isolate boundary and the main event loop stays free.
		const zstSize = await pool.compress(index, rawPath, tmpPath, this.zstdCompressOptions);

		if (this.disposed) {
			// Pool was torn down while this job ran; don't mutate reader state on a
			// closing store. Leave the .tmp for the next open to clean up.
			return;
		}

		await Deno.rename(tmpPath, zstPath); // atomic on the same filesystem

		// A reader that opened the raw fd before this point keeps working fine —
		// Linux doesn't yank data out from under an already-open fd on unlink.
		await Deno.remove(rawPath);

		// Any reader cached as "raw" for this index must fall back to compressed from now on.
		this.closeRawReader(index);
		this.readers[index] = null;

		const ms = (performance.now() - started).toFixed(0);
		const ratio = (zstSize / rawSize).toFixed(4);
		console.log(`[compress] chunk ${index} done: ${rawSize} -> ${zstSize} bytes (ratio=${ratio}, ${ms}ms)`);
	}

	close() {
		this.disposed = true;
		// Tear down the compression worker pool. Idle workers terminate now; any
		// worker mid-job terminates when it posts its result back.
		this.compressPool?.dispose();
		this.compressPool = undefined;
		for (const reader of this.readers) reader?.close();
		this.readers.length = 0;
		for (const timer of this.inflatedTimers.values()) clearTimeout(timer);
		this.inflatedTimers.clear();
		this.appender.file.close();
		this.appender.index = -1;
		if (this.lockFile) {
			try {
				this.lockFile.unlockSync();
			} catch {
				// fd is going away either way; the OS drops the lock on close() too.
			}
			this.lockFile.close();
			this.lockFile = undefined;
		}
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
