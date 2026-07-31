import { Codec } from "@nomadshiba/codec";
import { Advice, Mmap } from "@nomadshiba/mmap";
import { existsSync } from "@std/fs";
import { join } from "@std/path";
import { createReadStream, createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import zlib from "node:zlib";
import { MiB, SECOND } from "~/constants.ts";
import { DEV, PARALLELISM_THREADS } from "~/env.ts";
import { Store } from "~/libs/storage/Store.ts";
import { CompressWorkerPool } from "./CompressWorkerPool.ts";

const COMPRESS_PARALLELISM = Math.min(PARALLELISM_THREADS, Math.max(8, Math.floor(PARALLELISM_THREADS * .5)));

/**
 * A chunk's memory mapping paired with its `bytes()` view, taken once at open
 * time and reused. `Mmap.bytes()` builds a fresh `Uint8Array` on every call, so
 * caching it here keeps the read/write hot paths allocation-free. The pair is
 * always created and discarded together.
 */
type MappedChunk = { mapping: Mmap; bytes: Uint8Array };

// node:zlib streams default to 16 KiB chunks — ~65k event-loop hops per ~1 GiB
// chunk. 8 MiB buffers cut that to ~130.
const INFLATE_STREAM_BUFFER_SIZE = 8 * MiB;
// The sync decode path allocates + Buffer.concat()s per chunkSize output buffer
// — bigger chunks mean fewer of both. Default is tiny.
const INFLATE_SYNC_CHUNK_SIZE = 64 * MiB;

const { constants } = zlib;

export type CompressionOptions = {
	/** How long (ms) an inflated-then-evicted chunk's raw form is kept before reverting to compressed-only. Each read re-arms the timer. */
	maxInflatedChunkAge: number;
	/** Hard cap on simultaneously-inflated chunks. Eviction is LRU. Set >= 2 so a read straddling two chunks never evicts one it's still walking. */
	maxInflatedChunks: number;
	zstd: {
		compress: { [K in keyof typeof constants as K extends `ZSTD_c_${infer U}` ? U : never]?: number };
		decompress: { [K in keyof typeof constants as K extends `ZSTD_d_${infer U}` ? U : never]?: number };
	};
};

export type BlobStoreOptions<C extends Codec<number>> = {
	path: string;
	cursor: C;
	chunkSize: number;
};

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

function cursorPath(root: string): string {
	return join(root, "CURSOR");
}
// Stable mutex inode (never renamed) — decoupled from the value file so the size
// publish can atomically rename over CURSOR without disturbing the lock.
function cursorLockPath(root: string): string {
	return join(root, "CURSOR.lock");
}
// New size staged here, then renamed over CURSOR (atomic publish).
function cursorTmpPath(root: string): string {
	return `${cursorPath(root)}.tmp`;
}
// DEV write-guard: a lock older than this is treated as crash-left and reclaimed
// (a real publish critical section is sub-millisecond, never close to this).
const CURSOR_LOCK_STALE_MS = 5 * SECOND;

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

/**
 * Chunked, append-only blob store. Records are addressed by absolute byte
 * `pointer` and read/written through memory maps of on-disk `chunk_N` files.
 *
 * ## Fixed-size chunks
 * Every `chunk_N` is created at exactly `maxChunkSize` bytes (zero-filled) and
 * never resized afterward — so its memory map is opened once and never goes
 * stale. Physical file size can't tell you how much is real data vs zero
 * padding; that's what `CURSOR` is for.
 *
 * ## The cursor
 * `CURSOR` (encoded via the `counter` codec) holds the store's logical size.
 * There is NO in-memory cache of it — every size check reads it fresh off
 * disk. Only `append()` and `resize()` write it, under `CURSOR`'s OS-level
 * file lock (the cross-process mutex for "who gets to grow/shrink right now").
 *
 * ## No straddle
 * `append()` never lets a record straddle a chunk boundary — if it doesn't
 * fit the remainder, the chunk is sealed early (padding gap) and the record
 * starts at the next chunk. `writeInto` has the same single-chunk rule.
 *
 * ## writeInto vs append
 * The store is append-only: live data (below the cursor) is never mutated.
 * Both writers only put bytes at or in front of the cursor:
 * - `append` reserves under the cursor lock (bump `CURSOR` past the record),
 *   then copies bytes straight into the mapping — unlocked.
 * - `writeInto(offset, bytes)` throws unless `offset >= cursor` — it only
 *   fills not-yet-live space. It's the primitive for parallel/out-of-order
 *   fills: reserve a region ahead of the cursor (via `resize`), have workers
 *   `writeInto` disjoint sub-ranges, then advance `CURSOR` past them.
 *
 * ## Compression
 * Reading a compressed chunk always works (inflated back to a raw file on
 * disk, never in memory). `startCompression(options)` starts a background loop
 * that proactively seals non-tail chunks into `.zst`. Throws if called twice.
 * Without it, inflated raw files are permanent (the .zst is deleted); with it,
 * they're tracked with a TTL + LRU cap.
 *
 * ## writeInto vs compression
 * No contention to synchronize: compression only touches chunks STRICTLY BELOW
 * the tail, both writers only touch space AT OR ABOVE the cursor — they can
 * never touch the same chunk. `compressChunk` reads+compresses with no lock
 * held and commits unconditionally. The only per-chunk lock (`chunkInflateLock
 * Path`) serialises inflate against a concurrent compress-commit.
 *
 * ## SIGBUS safety
 * A live mapping crashes the process (uncatchable) if the file shrinks/is
 * removed under it. Every site that shrinks/removes a raw chunk unmaps it first.
 */
export class BlobStore<C extends Codec<number>> extends Store implements Disposable {
	public readonly path: string;
	public readonly cursor: C;
	public readonly chunkSize: number;
	private compression: CompressionOptions | undefined;
	private zstdCompressOptions: Record<number, number>;
	private zstdDecompressOptions: Record<number, number>;
	private zstdDecompressSyncOptions: zlib.ZstdOptions;
	private zstdDecompressStreamOptions: zlib.ZstdOptions;

	private constructor(options: BlobStoreOptions<C>) {
		super();
		this.path = options.path;
		this.cursor = options.cursor;
		this.chunkSize = options.chunkSize;
		this.zstdCompressOptions = {};
		this.zstdDecompressOptions = {};
		this.zstdDecompressSyncOptions = {
			chunkSize: INFLATE_SYNC_CHUNK_SIZE,
			// Sealed raw chunks are always exactly maxChunkSize — a hard output
			// bound is free corruption detection on top of the frame checksum.
			maxOutputLength: options.chunkSize,
			params: this.zstdDecompressOptions,
		};
		this.zstdDecompressStreamOptions = {
			chunkSize: INFLATE_STREAM_BUFFER_SIZE,
			params: this.zstdDecompressOptions,
		};
	}

	static open<C extends Codec<number>>(options: BlobStoreOptions<C>): BlobStore<C> {
		const self = new BlobStore(options);
		Deno.mkdirSync(self.path, { recursive: true });
		deleteTmpFiles(self.path);

		const cursorSize = self.readCursor();
		const tailIndex = Math.floor(cursorSize / self.chunkSize);

		const indexSet = new Set<number>();
		for (const file of Deno.readDirSync(self.path)) {
			if (!file.isFile) continue;
			if (!file.name.startsWith("chunk_")) continue;
			if (file.name.endsWith(".tmp")) continue;
			const name = file.name.endsWith(".zst") ? file.name.slice(0, -".zst".length) : file.name;
			const index = Number(name.slice("chunk_".length));
			if (!Number.isInteger(index)) continue;
			indexSet.add(index);
		}

		for (let index = 0; index < tailIndex; index++) {
			if (!indexSet.has(index)) throw new Error("bro your chunks are fucked, has some gaps and stuff");

			const hasZst = existsSync(chunkZstPath(self.path, index));
			const hasRaw = existsSync(chunkPath(self.path, index));

			// Both present: raw always wins — drop the zst (crash-left half-committed compress).
			if (hasZst && hasRaw) {
				discardStaleCompressedForm(self.path, index);
				continue;
			}
			if (hasRaw) {
				const size = Deno.statSync(chunkPath(self.path, index)).size;
				if (size !== self.chunkSize) {
					throw new Error(`chunk ${index} has a weird size size=${size}, expected exactly ${self.chunkSize}`);
				}
				continue;
			}
			if (!hasZst) throw new Error(`chunk ${index} is missing both raw and compressed forms`);
		}

		// The tail is always raw — self-heal it into existence (e.g. brand-new store).
		ensureChunkFile(self.path, tailIndex, self.chunkSize);

		return self;
	}

	/** Unlocked read of the current logical size. Safe anytime; worst case sees a slightly-stale (never corrupt) value. */
	private readCursor(): number {
		let bytes: Uint8Array;
		try {
			bytes = Deno.readFileSync(cursorPath(this.path));
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
			return 0;
		}
		if (bytes.length === 0) return 0;
		return this.cursor.decode(bytes, 0)[0];
	}

	/**
	 * Lock `CURSOR`, hand its current value to `mutate`, persist whatever size
	 * it returns, then unlock. The ONLY place the cursor is written — the flock
	 * is the cross-process mutex for "who gets to grow/shrink right now".
	 * `mutate` should do any chunk provisioning/cleanup before returning.
	 */
	private withCursorLock<R>(mutate: (current: number) => { size: number; result: R }): R {
		// Single-writer per store is an INVARIANT, not something we synchronise on:
		// in production no lock is taken at all. In DEV we assert it — acquireWriteGuard
		// throws (never blocks) if another worker is publishing this cursor right now,
		// so a violation surfaces loudly instead of silently corrupting.
		if (DEV) this.acquireWriteGuard();
		try {
			const current = this.readCursor();
			const { size, result } = mutate(current);

			// Publish the new size ATOMICALLY: stage it in a tmp file, then rename
			// over CURSOR. rename() is atomic, so a reader sees the whole old value
			// or the whole new one — never an empty (mid-truncate) or half-written
			// file. That's the fence: size only advances once, all-or-nothing, and
			// (for append) only after the bytes are already in the map. Crash-safe
			// too — a torn CURSOR can never exist on disk.
			const tmp = cursorTmpPath(this.path);
			Deno.writeFileSync(tmp, this.cursor.encode(size));
			Deno.renameSync(tmp, cursorPath(this.path));

			return result;
		} finally {
			if (DEV) this.releaseWriteGuard();
		}
	}

	/**
	 * DEV-only assertion of the single-writer invariant. Create the lock file
	 * exclusively — that's a non-blocking test-and-set. If it already exists and is
	 * FRESH, another worker is mid-publish → throw (a real bug; never wait on it).
	 * A lock older than CURSOR_LOCK_STALE_MS is crash-left (a genuine publish is
	 * sub-millisecond), so reclaim it. Not compiled into the production path.
	 */
	private acquireWriteGuard(): void {
		const path = cursorLockPath(this.path);
		try {
			Deno.openSync(path, { createNew: true, write: true }).close();
			return;
		} catch (e) {
			if (!(e instanceof Deno.errors.AlreadyExists)) throw e;
		}

		let ageMs = Infinity;
		try {
			const mtime = Deno.statSync(path).mtime;
			if (mtime) ageMs = Date.now() - mtime.getTime();
		} catch { /* vanished between open and stat — treat as reclaimable */ }

		if (ageMs < CURSOR_LOCK_STALE_MS) {
			throw new Error(
				`[BlobStore] concurrent cursor write on ${this.path} — single-writer invariant violated (two workers writing the same store).`,
			);
		}

		// stale (crash-left): reclaim, then take it.
		try {
			Deno.removeSync(path);
		} catch { /* raced another reclaimer */ }
		Deno.openSync(path, { createNew: true, write: true }).close();
	}

	private releaseWriteGuard(): void {
		try {
			Deno.removeSync(cursorLockPath(this.path));
		} catch { /* already gone */ }
	}

	get<T extends Codec>(pointer: number, codec: T): [Codec.InferOutput<T>, number] {
		const [bytes, offset] = this.view(pointer);
		return codec.decode(bytes, offset);
	}

	// TODO: do we even need this as async anymore????
	async getAsync<T extends Codec>(pointer: number, codec: T): Promise<[Codec.InferOutput<T>, number]> {
		const [bytes, offset] = await this.viewAsync(pointer);
		return codec.decode(bytes, offset);
	}

	private view(pointer: number): [bytes: Uint8Array, offset: number] {
		const size = this.readCursor();
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
		const size = this.readCursor();
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

	/**
	 * Where the next record will actually land: `from` (default `size()`), bumped
	 * to the start of the next chunk when the current chunk has less than
	 * `maxItemSize` room left. Deterministic — it doesn't depend on the record's
	 * bytes — so a caller doing out-of-order `writeInto` fills can compute each
	 * slot ahead of time and know nothing will straddle. `append` uses it too.
	 */
	nextItemPointer(maxItemSize: number, from: number = this.size()): number {
		const room = this.chunkSize - (from % this.chunkSize);
		return room < maxItemSize ? from + room : from;
	}

	/**
	 * Write `bytes` at `offset`, which must be AT OR IN FRONT OF the current
	 * cursor (`offset >= size()`). Throws otherwise — this can only fill
	 * not-yet-live space, never overwrite committed data. The primitive for
	 * parallel/out-of-order fills: reserve ahead of the cursor (via `resize`),
	 * have workers fill disjoint sub-ranges, then advance `CURSOR` past them.
	 * Single-chunk only; provisions the target chunk if it doesn't exist yet.
	 */
	writeInto(offset: number, bytes: Uint8Array): number {
		const size = this.readCursor();
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
		this.writeIntoMap(offset, bytes);

		return bytes.byteLength;
	}

	unsafeMap(begin?: number, length?: number) {
		const size = this.readCursor();
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

	// Unchecked byte-copy shared by `append` and `writeInto`: store straight
	// into the chunk's writable mapping (MAP_SHARED). Callers guarantee the
	// offset is at/above the cursor and within a single existing chunk.
	private writeIntoMap(offset: number, bytes: Uint8Array): void {
		const index = Math.floor(offset / this.chunkSize);
		const start = offset % this.chunkSize;
		const map = this.getChunkMap(index);
		if (!map) throw new Error(`chunk ${index} missing its raw file for a write at offset=${offset}`);
		map.bytes.set(bytes, start);
	}

	sync() {
		for (const map of this.chunkMaps) map?.mapping.flush();
	}

	size(): number {
		return this.readCursor();
	}

	/**
	 * Move the cursor to `size`, growing or shrinking the store. Growing just
	 * provisions the new chunk file(s). Shrinking deletes orphaned higher-index
	 * chunks and zeroes the new tail's stale bytes past the new logical end.
	 * Only closes/unmaps the specific chunks being deleted/resized — the store
	 * stays usable and the compression loop is left alone.
	 */
	resize(size: number): void {
		if (size < 0) throw new Error(`resize size must be >= 0, got ${size}`);

		// TODO: withCursorLock is only used where, why is it a seperate method???
		this.withCursorLock((current) => {
			if (size > current) {
				const oldTailIndex = Math.floor(current / this.chunkSize);
				const newTailIndex = Math.floor(size / this.chunkSize);
				for (let index = oldTailIndex; index <= newTailIndex; index++) {
					ensureChunkFile(this.path, index, this.chunkSize);
				}
			} else if (size < current) {
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

			return { size, result: undefined };
		});
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
				const tailIndex = Math.floor(this.readCursor() / this.chunkSize);

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
