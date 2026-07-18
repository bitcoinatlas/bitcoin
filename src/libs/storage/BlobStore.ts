import { RocksDatabase, Transaction } from "@harperfast/rocksdb-js";
import { Codec, U64 } from "@nomadshiba/codec";
import { join } from "@std/path";
import { createReadStream, createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import zlib from "node:zlib";
import { MAX_BLOCK_SIZE, SECOND } from "~/constants.ts";
import { PARALLELISM } from "~/env.ts";
import { readFileInto, readFileIntoSync, writeFileSync } from "~/libs/fs/mod.ts";
import { StoreAppendOnly } from "~/libs/storage/Store.ts";
import { CompressWorkerPool } from "./CompressWorkerPool.ts";

const COMPRESS_PARALLELISM = Math.min(PARALLELISM, Math.max(8, Math.floor(PARALLELISM * .5)));

const { constants } = zlib;

// TODO: Later keep these options inside the compression options.
// And also decide these based on a max memory usage, similar to p2p worker limits.
// Probably later have a main config.ts file where you calcualte everything. based on core count and devices memory.

// TODO: Probably have maximum number of inflated chunks at a time, so you can delete the oldest and stuff.

const ZSTD_PARAMS = {
	[constants.ZSTD_c_compressionLevel]: 19,
	[constants.ZSTD_c_enableLongDistanceMatching]: 1,
	[constants.ZSTD_c_windowLog]: 27, // maybe make it 24 later?
	[constants.ZSTD_c_checksumFlag]: 1, // 4-byte frame checksum, cheap integrity guard
	[constants.ZSTD_c_contentSizeFlag]: 1, // size in frame header — works on the sync path
	// nbWorkers deliberately omitted: Deno's node:zlib zstd binding threw
	// ERR_ZLIB_INITIALIZATION_FAILED ("Setting parameter failed") with it set —
	// the CLI `zstd -T4` test earlier used a completely different binary; Deno's
	// bundled libzstd likely wasn't built with ZSTD_MULTITHREAD. Compression is
	// single-threaded per call, but the async paths run it through a streaming
	// zstd transform on Deno's native binding, off the main thread, so they
	// stay non-blocking. Sync inflate (readInto) blocks by design.
} as const;

// Decompression rejects the compression-side parameters above ("N is not a
// valid zstd parameter"). It only takes decompress params — and it MUST be told
// the window can be as large as the compressor's windowLog (27), otherwise
// large frames fail to inflate.
const ZSTD_DECOMPRESS_PARAMS = {
	[constants.ZSTD_d_windowLogMax]: 27,
} as const;

type Region = {
	size: number;
	append(bytes: Uint8Array): void;
	readInto(offset: number, length: number, target: Uint8Array): number;
	readIntoAsync(offset: number, length: number, target: Uint8Array): Promise<number>;
};

export type CompressionOptions = {
	/**
	 * How long (ms) a chunk that was inflated back to its raw form on disk is
	 * kept before it's deleted again (reverting to compressed-only). Each read
	 * re-arms the timer.
	 */
	decompressedMaxAge: number;
};

type DiskRegionOptions = {
	path: string;
	maxChunkSize: number;
	writable: boolean;
	compression?: CompressionOptions;
};

export type BlobStoreOptions = {
	path: string;
	rocksdb: RocksDatabase;
	maxChunkSize: number;
	writable: boolean;
	compression?: CompressionOptions;
};

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
		const store = new BlobStore(DiskRegion.open({ path, maxChunkSize, writable, compression }), options);
		return store;
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

	append(data: Uint8Array) {
		const offset = this.size();
		this.disk.append(data);
		return offset;
	}

	size() {
		return this.disk.size;
	}

	private rollbackFile: Deno.FsFile | undefined;
	pin(transaction?: Transaction): void {
		this.disk.sync();
		this.rocksdb.putSync("rollback.size", U64.encode(this.disk.size), { transaction });
	}

	rollback(transaction?: Transaction): void {
		const bytes = this.rocksdb.getSync("rollback.size", { transaction }) as Uint8Array | undefined;
		let size: number;
		if (bytes) {
			const [decoded] = U64.decode(bytes);
			size = Number(decoded);
		} else {
			size = 0;
		}
		this.truncate(size);
	}

	truncate(size: number): void {
		this.disk.truncate(size);
	}

	close(): void {
		this.disk.close();
		this.rollbackFile?.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}

class DiskRegion implements Region, Disposable {
	public readonly path: string;
	public readonly maxChunkSize: number;
	private readonly writable: boolean;
	private readonly compression: CompressionOptions | undefined;

	public size: number;
	chunkPathCache: string[] = [];
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
		this.size = 0;
	}

	static open(options: DiskRegionOptions): DiskRegion {
		const self = new DiskRegion(options);
		Deno.mkdirSync(self.path, { recursive: true });

		const files = Deno.readDirSync(self.path);

		let tailIndex = 0;
		const indexSet = new Set<number>([0]);
		for (const file of files) {
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
			const exist = indexSet.has(index);
			if (!exist) {
				throw new Error("bro your chunks are fucked, has some gaps and stuff");
			}
			// A sealed chunk may now be raw (chunk_N) OR compressed (chunk_N.zst).
			// Only size-check the raw form — a compressed chunk's on-disk size is
			// expected to differ from maxChunkSize, that's the whole point of it.
			try {
				const chunkStat = Deno.statSync(self.chunkPath(index));
				if (chunkStat.size !== self.maxChunkSize) {
					throw new Error(`chunk ${index} has a weird size size=${chunkStat.size}`);
				}
			} catch (e) {
				if (!(e instanceof Deno.errors.NotFound)) throw e;
				try {
					Deno.statSync(self.chunkPathZst(index));
				} catch {
					throw new Error(`chunk ${index} is missing both raw and compressed forms`);
				}
			}
		}
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
			self.runCompressionLoop(self.compression).catch((e) => {
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
	// When compression is enabled we arm a TTL timer per inflated chunk; when it
	// fires we delete the raw file again (the .zst stays, so it re-inflates on
	// the next read). When compression is disabled, inflating is one-way: the
	// .zst is deleted and the raw file is permanent (no timer).
	private inflatedTimers = new Map<number, ReturnType<typeof setTimeout>>();

	private armInflatedEviction(index: number) {
		if (!this.compression) return; // one-way inflate; nothing to evict
		const existing = this.inflatedTimers.get(index);
		if (existing) clearTimeout(existing);
		const timer = setTimeout(() => {
			this.inflatedTimers.delete(index);
			this.evictInflatedChunk(index);
		}, this.compression.decompressedMaxAge);
		this.inflatedTimers.set(index, timer);
	}

	// Delete the raw form of a chunk that still has a .zst, reverting it to
	// compressed-only. Safe to call even if the raw file is already gone.
	private evictInflatedChunk(index: number) {
		// Only evict if a compressed form actually exists to fall back to.
		try {
			Deno.statSync(this.chunkPathZst(index));
		} catch {
			return; // no .zst — this is a live raw chunk, don't touch it
		}
		this.closeRawReader(index);
		this.readers[index] = null;
		try {
			Deno.removeSync(this.chunkPath(index));
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
		}
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

		const lockPath = this.chunkPathInflateLock(index);
		const lock = Deno.openSync(lockPath, { create: true, write: true, read: true });
		try {
			lock.lockSync(true); // blocks until the other inflater releases
			// Re-probe: another holder may have inflated it while we waited.
			this.readers[index] = undefined;
			if (this.getRawReader(index)) return;

			console.log(`[compress] inflating chunk ${index} (sync)`);
			const started = performance.now();
			const compressed = Deno.readFileSync(this.chunkPathZst(index));
			const raw = zlib.zstdDecompressSync(compressed, { params: ZSTD_DECOMPRESS_PARAMS });
			const tmpPath = this.chunkPathRawTmp(index);
			Deno.writeFileSync(tmpPath, raw);
			Deno.renameSync(tmpPath, this.chunkPath(index)); // atomic swap-in
			const ms = (performance.now() - started).toFixed(0);
			console.log(`[compress] chunk ${index} inflated, ${raw.byteLength} bytes on disk ${ms}ms`);

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
			const lockPath = this.chunkPathInflateLock(index);
			const lock = Deno.openSync(lockPath, { create: true, write: true, read: true });
			try {
				await lock.lock(true); // blocks until the other inflater releases
				this.readers[index] = undefined;
				if (this.getRawReader(index)) return;

				console.log(`[compress] inflating chunk ${index} (async, streaming)`);
				const tmpPath = this.chunkPathRawTmp(index);
				// Stream .zst -> zstd inflate transform -> raw tmp file. Bounded
				// peak memory; runs off the main thread; never blocks the loop.
				const source = createReadStream(this.chunkPathZst(index));
				const transform = zlib.createZstdDecompress({ params: ZSTD_DECOMPRESS_PARAMS });
				const sink = createWriteStream(tmpPath);
				await pipeline(source, transform, sink);
				await Deno.rename(tmpPath, this.chunkPath(index)); // atomic swap-in
				console.log(`[compress] chunk ${index} inflated on disk`);

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
	private afterInflate(index: number) {
		if (this.compression) {
			// Keep the .zst; schedule the raw file for deletion after the TTL.
			this.armInflatedEviction(index);
		} else {
			// One-way inflate: the raw file is now the source of truth, drop .zst.
			try {
				Deno.removeSync(this.chunkPathZst(index));
			} catch (e) {
				if (!(e instanceof Deno.errors.NotFound)) throw e;
			}
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
			} else {
				// Touched a raw-but-compressible chunk — push its eviction back.
				if (this.compression && this.inflatedTimers.has(index)) this.armInflatedEviction(index);
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
			} else {
				if (this.compression && this.inflatedTimers.has(index)) this.armInflatedEviction(index);
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

	private truncating: boolean = false;
	truncate(size: number): void {
		if (this.appending) throw new Error("you cant truncate while appending");
		if (this.truncating) throw new Error("you are trying to truncate back to back, check your logic");
		if (size > this.size) throw new Error(`cant truncate upward size=${size} current=${this.size}`);
		this.truncating = true;

		const oldTail = this.appender.index;
		const newTail = Math.floor(size / this.maxChunkSize);
		const tailEnd = size % this.maxChunkSize;

		this.close();

		// FIX #5: delete high-to-low. If a crash happens mid-truncate, the
		// surviving chunks are always a contiguous prefix [0..k], never a set
		// with a gap. A gap would make DiskRegion.open throw ("chunks are
		// fucked") and brick recovery. High-to-low guarantees no gap.
		for (let index = oldTail; index > newTail; index--) {
			this.removeChunkFile(this.chunkPath(index));
			// The chunk might already have been compressed by the background pass —
			// drop that form (and any inflate leftovers) too so nothing resurrects
			// a stale chunk on reopen.
			this.removeChunkFile(this.chunkPathZst(index));
			this.removeChunkFile(this.chunkPathZstTmp(index));
			this.removeChunkFile(this.chunkPathRawTmp(index));
			this.removeChunkFile(this.chunkPathInflateLock(index));
			const timer = this.inflatedTimers.get(index);
			if (timer) {
				clearTimeout(timer);
				this.inflatedTimers.delete(index);
			}
			this.readers[index] = undefined;
		}

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

	private async runCompressionLoop(_options: CompressionOptions): Promise<void> {
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

					let alreadyDone: boolean;
					try {
						await Deno.stat(this.chunkPathZst(index));
						alreadyDone = true;
					} catch {
						alreadyDone = false;
					}
					if (alreadyDone) continue;

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
					const rawSize = rawStat.size;
					const promise = this.compressChunk(pool, index, rawSize)
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
				if (inFlight.size > 0) {
					await Promise.all(inFlight.values());
				}

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
		const zstSize = await pool.compress(index, rawPath, tmpPath, ZSTD_PARAMS);

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

	private removeChunkFile(path: string) {
		try {
			Deno.removeSync(path);
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
		}
	}

	close() {
		this.disposed = true;
		// Tear down the compression worker pool. Idle workers terminate now; any
		// worker mid-job terminates when it posts its result back.
		this.compressPool?.dispose();
		this.compressPool = undefined;
		for (const reader of this.readers) {
			if (!reader) continue;
			reader.close();
		}
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
