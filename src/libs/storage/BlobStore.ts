import { Codec } from "@nomadshiba/codec";
import { Advice, Mmap } from "@nomadshiba/mmap";
import { existsSync } from "@std/fs";
import { join } from "@std/path";
import { createReadStream, createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import zlib from "node:zlib";
import { MiB, SECOND } from "~/constants.ts";
import { PARALLELISM_THREADS } from "~/env.ts";
import { readFileSync, writeFileSync } from "~/libs/fs/mod.ts";
import { Store } from "~/libs/storage/Store.ts";
import { CompressWorkerPool } from "./CompressWorkerPool.ts";

const COMPRESS_PARALLELISM = Math.min(PARALLELISM_THREADS, Math.max(8, Math.floor(PARALLELISM_THREADS * .5)));

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

export type BlobStoreOptions<T extends Codec, C extends Codec<number>> = {
	path: string;
	entry: T;
	/** Codec used to encode/decode the on-disk cursor (the store's logical size). See `CURSOR` below. */
	counter: C;
	maxChunkSize: number;
};

// ── stateless helpers ────────────────────────────────────────────────────────
// Everything below is a pure function of its arguments — no instance state —
// kept out of the class so path/size arithmetic can be read (and tested) on
// its own.

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
// Raw chunk being reconstructed from its .zst — written here then atomically
// renamed into place so a reader never sees a half-inflated chunk_N.
function chunkRawTmpPath(root: string, index: number): string {
	return `${chunkPath(root, index)}.raw.tmp`;
}
// Per-chunk advisory lock, serving double duty:
// - Serialises inflate (so two workers/reads don't decompress the same chunk
//   twice) and writeInto against a concurrent compress commit (see the class
//   doc's "writeInto vs compression").
// - Its CONTENT is a tiny "generation" marker (see readChunkGeneration /
//   writeChunkGeneration below): writeInto rewrites it to a fresh random value
//   every time it patches a chunk, and compressChunk compares it before/after
//   compressing to detect whether the chunk changed meanwhile.
function chunkLockPath(root: string, index: number): string {
	return `${chunkPath(root, index)}.lock`;
}

// Open, lock (blocking), run `fn`, unlock, close — the shared plumbing for
// every chunk-lock critical section (writeInto's patch, an inflate, or
// compressChunk's tiny commit step). Always a near-instant hold: nothing that
// does real I/O proportional to chunk size ever runs while holding it.
function withChunkLock<R>(lockPath: string, fn: (file: Deno.FsFile) => R): R {
	const file = Deno.openSync(lockPath, { create: true, write: true, read: true });
	try {
		file.lockSync(true); // blocks until acquired
		return fn(file);
	} finally {
		try {
			file.unlockSync();
		} catch { /* fd closing drops the lock anyway */ }
		file.close();
	}
}

/** Read a chunk's current generation marker. Assumes the caller already holds its lock. */
function readChunkGeneration(file: Deno.FsFile): Uint8Array {
	const size = file.statSync().size;
	if (size === 0) return new Uint8Array(0);
	file.seekSync(0, Deno.SeekMode.Start);
	return readFileSync(file, size);
}

/** Overwrite a chunk's generation marker. Assumes the caller already holds its lock. */
function writeChunkGeneration(file: Deno.FsFile, value: Uint8Array): void {
	file.truncateSync(0);
	file.seekSync(0, Deno.SeekMode.Start);
	writeFileSync(file, value);
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
	return true;
}

// The store's logical size (the cursor), persisted so a restart doesn't need
// to guess where real data ends from chunk file sizes — every chunk file is
// always exactly `maxChunkSize` from the moment it's created, so file size
// alone can never answer that anymore. See the class doc for the full model.
function cursorPath(root: string): string {
	return join(root, "CURSOR");
}

// Create (if missing) and/or grow chunk_N to exactly `maxChunkSize`, zero-
// filled. Every chunk is always fully sized from the moment it exists — never
// grown incrementally — so it can be memory-mapped once and never remapped
// (see the class doc's "fixed-size chunks" section). Idempotent: a no-op if
// the chunk is already the right size. Throws if it's somehow bigger
// (corruption — nothing should ever make a chunk file exceed maxChunkSize).
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

// Remove every on-disk form of a chunk (raw, compressed, and any transient
// tmp/lock leftovers). Each is optional — missing forms are ignored.
function removeAllChunkForms(root: string, index: number): void {
	Deno.removeSync(chunkPath(root, index), { recursive: true });
	Deno.removeSync(chunkZstPath(root, index), { recursive: true });
	Deno.removeSync(chunkZstTmpPath(root, index), { recursive: true });
	Deno.removeSync(chunkRawTmpPath(root, index), { recursive: true });
	Deno.removeSync(chunkLockPath(root, index), { recursive: true });
}

// Revert a chunk to compressed-only: drop its raw form + inflate leftovers,
// keeping the .zst. Safe if the raw form is already gone. Only ever called on
// a chunk that's merely a cached inflate of its .zst with nothing written
// into it since (writeInto eagerly deletes a chunk's .zst the moment it
// patches that chunk — see writeInto — so if this chunk had been mutated,
// there'd be no .zst left here to revert to in the first place).
function revertChunkToCompressed(root: string, index: number): void {
	Deno.removeSync(chunkPath(root, index), { recursive: true });
	Deno.removeSync(chunkRawTmpPath(root, index), { recursive: true });
	Deno.removeSync(chunkLockPath(root, index), { recursive: true });
}

// The opposite direction: drop a chunk's compressed form (+ any transient
// tmp/lock leftovers), keeping the raw file. Raw is always at least as up to
// date as zst (see the class doc's "writeInto vs compression" section), so
// wherever both happen to exist on disk at once, the zst is the disposable
// one. Used at startup (see open()) to clean up whatever state a crash left
// behind mid-compress.
function discardStaleCompressedForm(root: string, index: number): void {
	Deno.removeSync(chunkZstPath(root, index), { recursive: true });
	Deno.removeSync(chunkZstTmpPath(root, index), { recursive: true });
	Deno.removeSync(chunkLockPath(root, index), { recursive: true });
}

/**
 * Chunked, append-only blob store. Records are addressed by absolute byte
 * `pointer` and read/written through memory maps (`@nomadshiba/mmap`) of the
 * on-disk `chunk_N` files — reads decode zero-copy straight out of the
 * mapping, and `writeInto` patches store straight into it.
 *
 * ## Fixed-size chunks
 * Every `chunk_N` file is created at exactly `maxChunkSize` bytes (zero-
 * filled, via `ensureChunkFile`) and NEVER resized afterward — unlike a
 * naively-grown append log, a chunk's physical size never changes once it
 * exists. That means its memory map (`chunkMaps`) can be opened once, used for
 * both reads and writes for as long as the process runs, and never goes
 * stale/needs remapping — there's no "the tail chunk grew past the cached
 * map's length" case to handle anymore.
 *
 * The cost: since every chunk file is always fully sized, physical file size
 * can no longer tell you how much of a chunk is *logically* real data versus
 * unwritten zero padding. That's what the `CURSOR` file is for.
 *
 * ## The cursor
 * `CURSOR` (encoded via the `counter` codec) holds the store's logical size —
 * the authoritative answer to "how much of this store is real data". There is
 * NO in-memory cache of it anywhere in this class; every operation that needs
 * the current size (`size()`, `get`/`getAsync`, `writeInto`, `nextPointer`)
 * reads `CURSOR` fresh off disk every time. Only `append()` and `truncate()`
 * ever WRITE it, and they do so under `CURSOR`'s own OS-level file lock — that
 * lock doubles as the cross-process mutex for "who gets to grow/shrink the
 * store right now", so multiple processes can safely race to append/truncate
 * without stepping on each other, without either of them needing to trust a
 * private in-memory value the other can't see.
 *
 * ## No straddle
 * `append()` never lets a single record straddle a chunk boundary: if a
 * record doesn't fit the active chunk's remaining space, that chunk is sealed
 * early (its remaining bytes become an unaddressed, never-written, never-read
 * padding gap) and the record is written at the start of the next chunk
 * instead. The address space stays logical — chunk `N` always owns
 * `[N*maxChunkSize, (N+1)*maxChunkSize)` — so every record sits entirely
 * within one chunk's mapping; there's never a cross-seam read. `append` throws
 * if a single record exceeds `maxChunkSize`, and `writeInto` throws if the
 * bytes it's given don't fit the target chunk from its offset onward (it will
 * never spill into the next chunk).
 *
 * A full-log scan that walks records in append order (see
 * {@link BlobStore.nextPointer}) must skip these gaps explicitly — plain
 * `pointer + consumed` can land inside one.
 *
 * ## writeInto vs append
 * `writeInto(offset, bytes)` can only write WITHIN `[0, cursor)` — i.e. it can
 * only patch bytes that are already logically live; it can never extend the
 * store (that's exclusively `append`/`truncate`'s job, since only they touch
 * `CURSOR`). `append` is built on top of it: it locks `CURSOR`, computes where
 * the record goes (the no-straddle math above), makes sure the target chunk
 * physically exists, persists the bumped cursor, unlocks — and only THEN calls
 * `writeInto` to actually copy the bytes in. That two-step (reserve space,
 * then fill it) is deliberate: the lock is only held for the cheap "reserve"
 * part, not for the byte copy.
 *
 * ## Compression
 * Reading a compressed chunk (`chunk_N.zst` with no raw `chunk_N`) always
 * works — it's inflated back to a raw file on disk first (never keeping
 * decompressed bytes in memory), regardless of whether this instance ever
 * calls `startCompression()`. What `startCompression(options)` actually
 * controls is whether a background loop proactively SEALS chunks (everything
 * except the live tail) into `.zst` in the first place. It's meant to be
 * started from exactly one place for the store's whole lifetime — the loop
 * already saturates every core via its own worker pool, so more than one
 * running anywhere (even across processes) would only contend with itself for
 * no benefit; `startCompression` throws if called twice on the same instance.
 * Without it (or before it's called), an inflated chunk raw file is kept
 * around permanently once decompressed (the .zst is deleted); with it, raw
 * copies of non-tail chunks are tracked with a TTL + LRU cap instead, so they
 * don't accumulate forever.
 *
 * ## writeInto vs compression
 * Compressing a chunk is a long streaming operation — `writeInto` can't lock
 * for the whole thing, that would make writers block on compression for no
 * benefit. Instead, each chunk has a lock file (`chunkLockPath`) whose CONTENT
 * doubles as a tiny "generation" marker: `compressChunk` reads it right before
 * starting its slow read+compress (`genAtStart`), does the slow work with NO
 * lock held (writers never wait on it), then re-acquires the lock just long
 * enough to compare the marker now against `genAtStart`. `writeInto` grabs the
 * same lock for its own (near-instant) patch, rewriting the marker to a fresh
 * value every time it touches a chunk. So: unchanged marker means nothing
 * touched the chunk anywhere in that whole window — safe to commit (rename in
 * the `.zst`, delete raw); changed means a `writeInto` landed sometime during
 * the compress — the freshly made `.zst` is thrown away and the raw file is
 * left completely untouched, so nothing is lost; a later pass just tries
 * compressing it again. Both sides only ever hold the lock for a near-instant
 * critical section (never for anything proportional to chunk size), and
 * `writeInto` always re-probes its chunk map fresh while holding the lock
 * (never trusting one obtained before acquiring it) — otherwise it could still
 * end up writing into an mmap of an inode a compress commit already unlinked
 * out from under it while it was waiting for the lock. As a second line of
 * defense, "raw + zst both exist" is treated as "keep raw, discard zst"
 * wherever it can happen (startup) — raw is always at least as up to date as
 * zst, never the other way around.
 *
 * ## SIGBUS safety
 * A live mapping crashes the process (uncatchable) if the file it maps
 * shrinks or is removed out from under it. Every place that shrinks/removes a
 * raw chunk (compression, eviction, truncate) unmaps it first.
 */
export class BlobStore<T extends Codec, C extends Codec<number>> extends Store implements Disposable {
	public readonly path: string;
	public readonly entry: T;
	public readonly counter: C;
	public readonly maxChunkSize: number;
	// undefined until startCompression() is called — see the class doc's
	// "Compression" section. Decompression-on-read works regardless.
	private compression: CompressionOptions | undefined;
	private zstdCompressOptions: Record<number, number>;
	private zstdDecompressOptions: Record<number, number>;
	// Prebuilt option objects for the two decode paths — same params, different
	// buffer strategies. See the INFLATE_* constants at the top. Default to
	// empty decompress params until startCompression() supplies real ones —
	// inflating already works fine with defaults for ordinary zstd frames.
	private zstdDecompressSyncOptions: zlib.ZstdOptions;
	private zstdDecompressStreamOptions: zlib.ZstdOptions;

	private constructor(options: BlobStoreOptions<T, C>) {
		super();
		this.path = options.path;
		this.entry = options.entry;
		this.counter = options.counter;
		this.maxChunkSize = options.maxChunkSize;
		this.zstdCompressOptions = {};
		this.zstdDecompressOptions = {};
		this.zstdDecompressSyncOptions = {
			chunkSize: INFLATE_SYNC_CHUNK_SIZE,
			// Sealed raw chunks are always exactly maxChunkSize — a hard output
			// bound is free corruption detection on top of the frame checksum.
			maxOutputLength: options.maxChunkSize,
			params: this.zstdDecompressOptions,
		};
		this.zstdDecompressStreamOptions = {
			chunkSize: INFLATE_STREAM_BUFFER_SIZE,
			params: this.zstdDecompressOptions,
		};
	}

	static open<T extends Codec, C extends Codec<number>>(options: BlobStoreOptions<T, C>): BlobStore<T, C> {
		const self = new BlobStore(options);
		Deno.mkdirSync(self.path, { recursive: true });
		deleteTmpFiles(self.path);

		const cursorSize = self.readCursor();
		const tailIndex = Math.floor(cursorSize / self.maxChunkSize);

		const indexSet = new Set<number>();
		for (const file of Deno.readDirSync(self.path)) {
			if (!file.isFile) continue;
			if (!file.name.startsWith("chunk_")) continue;
			if (file.name.endsWith(".tmp")) continue; // stray leftover from a crashed compression pass
			// chunk_N or chunk_N.zst — strip a trailing .zst before parsing the index
			const name = file.name.endsWith(".zst") ? file.name.slice(0, -".zst".length) : file.name;
			const index = Number(name.slice("chunk_".length));
			if (!Number.isInteger(index)) continue;
			indexSet.add(index);
		}

		for (let index = 0; index < tailIndex; index++) {
			if (!indexSet.has(index)) throw new Error("bro your chunks are fucked, has some gaps and stuff");

			const hasZst = existsSync(chunkZstPath(self.path, index));
			const hasRaw = existsSync(chunkPath(self.path, index));

			// Both present: raw always wins (it's always at least as up to date as
			// zst — see the class doc's "writeInto vs compression" section) —
			// drop the zst, whether it's a leftover cached inflate from before a
			// restart or a compression pass that lost a race with a writeInto and
			// never got to clean up after itself.
			if (hasZst && hasRaw) {
				discardStaleCompressedForm(self.path, index);
				continue;
			}
			// Every raw chunk is always exactly maxChunkSize now (chunks are never
			// physically short — see the class doc) — anything else is corruption.
			if (hasRaw) {
				const size = Deno.statSync(chunkPath(self.path, index)).size;
				if (size !== self.maxChunkSize) {
					throw new Error(`chunk ${index} has a weird size size=${size}, expected exactly ${self.maxChunkSize}`);
				}
				continue;
			}
			if (!hasZst) throw new Error(`chunk ${index} is missing both raw and compressed forms`);
		}

		// The tail is always raw (compression never touches the active chunk) and
		// always exactly maxChunkSize — self-heal it into existence if it isn't
		// there yet (e.g. brand-new store).
		ensureChunkFile(self.path, tailIndex, self.maxChunkSize);

		return self;
	}

	// ── cursor (persisted logical size) ────────────────────────────────────
	// No in-memory cache anywhere — see the class doc's "The cursor" section.

	/** Unlocked read of the current logical size. Safe to call anytime; worst case sees a slightly-stale (never corrupt) value. */
	private readCursor(): number {
		let bytes: Uint8Array;
		try {
			bytes = Deno.readFileSync(cursorPath(this.path));
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
			return 0; // brand-new store — nothing appended yet
		}
		if (bytes.length === 0) return 0;
		return this.counter.decode(bytes, 0)[0];
	}

	/**
	 * Lock `CURSOR`, hand its current value to `mutate`, persist whatever size
	 * it returns, then unlock. This is the ONLY place the cursor is ever
	 * written — the flock on `CURSOR` is the cross-process mutex for "who gets
	 * to grow/shrink the store right now" (`append`/`truncate` both go through
	 * here). `mutate` should do any chunk-file provisioning/cleanup itself
	 * before returning, since once this unlocks, `writeInto` (unlocked) is free
	 * to assume everything up to the new size is real.
	 */
	private withCursorLock<R>(mutate: (current: number) => { size: number; result: R }): R {
		const file = Deno.openSync(cursorPath(this.path), { read: true, write: true, create: true });
		try {
			file.lockSync(true); // blocks until acquired — cross-process mutex
			const stat = file.statSync();
			let current = 0;
			if (stat.size > 0) {
				file.seekSync(0, Deno.SeekMode.Start);
				current = this.counter.decode(readFileSync(file, stat.size), 0)[0];
			}

			const { size, result } = mutate(current);

			const encoded = this.counter.encode(size);
			file.truncateSync(0);
			file.seekSync(0, Deno.SeekMode.Start);
			writeFileSync(file, encoded);

			return result;
		} finally {
			try {
				file.unlockSync();
			} catch { /* fd closing drops the lock anyway */ }
			file.close();
		}
	}

	// ── reads ──────────────────────────────────────────────────────────────
	// Decode a record at absolute byte `pointer`, straight out of the mapped
	// chunk bytes — zero-copy, no read-ahead buffer, no scratch allocation.
	// Safe because append() never lets a record straddle a chunk boundary, so
	// the whole record is guaranteed to sit within one chunk's mapping.
	get(pointer: number): [Codec.InferOutput<T>, number];
	get<T extends Codec>(pointer: number, codec: T): [Codec.InferOutput<T>, number];
	get(pointer: number, codec: Codec = this.entry): [unknown, number] {
		const [bytes, offset] = this.view(pointer);
		return codec.decode(bytes, offset);
	}

	async getAsync(pointer: number): Promise<[Codec.InferOutput<T>, number]>;
	async getAsync<T extends Codec>(pointer: number, codec: T): Promise<[Codec.InferOutput<T>, number]>;
	async getAsync(pointer: number, codec: Codec = this.entry): Promise<[unknown, number]> {
		const [bytes, offset] = await this.viewAsync(pointer);
		return codec.decode(bytes, offset);
	}

	private view(pointer: number): [bytes: Uint8Array, offset: number] {
		const size = this.readCursor();
		if (pointer >= size) {
			throw new Error(`yeah you wanna read from offset=${pointer}, but all i have is size=${size}`);
		}
		const index = Math.floor(pointer / this.maxChunkSize);
		const start = pointer % this.maxChunkSize;

		let map = this.getChunkMap(index);
		if (!map) {
			this.inflateChunkSync(index); // compressed: recreate raw chunk on disk
			map = this.getChunkMap(index);
			if (!map) throw new Error(`chunk ${index} vanished after inflate`);
		} else if (this.compression && this.inflatedTimers.has(index)) {
			this.touchInflated(index); // touched a compressible chunk — defer eviction
		}
		return [map.bytes, start];
	}

	private async viewAsync(pointer: number): Promise<[bytes: Uint8Array, offset: number]> {
		const size = this.readCursor();
		if (pointer >= size) {
			throw new Error(`yeah you wanna read from offset=${pointer}, but all i have is size=${size}`);
		}
		const index = Math.floor(pointer / this.maxChunkSize);
		const start = pointer % this.maxChunkSize;

		let map = this.getChunkMap(index);
		if (!map) {
			await this.inflateChunkAsync(index); // compressed: stream-inflate to disk
			map = this.getChunkMap(index);
			if (!map) throw new Error(`chunk ${index} vanished after inflate`);
		} else if (this.compression && this.inflatedTimers.has(index)) {
			this.touchInflated(index);
		}
		return [map.bytes, start];
	}

	/**
	 * For full-log scans that walk records in append order (e.g. HashMapStore's
	 * rehash): given a record's `pointer` and how many bytes it consumed (as
	 * `get`/`getAsync` returned), return the pointer of the NEXT appended
	 * record — skipping the padding gap `append()` may have inserted when it
	 * sealed a chunk early rather than let that next record straddle. Naively
	 * doing `pointer + consumed` would land inside that gap and misdecode it as
	 * a record.
	 *
	 * Only needed when crossing from one `append()` call to another. Reading two
	 * parts of the SAME appended record (e.g. a struct's fixed prefix then its
	 * tail) never has a gap between them — plain `pointer + consumed` is fine there.
	 *
	 * TODO: since chunks are now always fully sized on disk from the moment
	 * they're created (see the class doc), a sealed-early chunk's physical file
	 * size is ALWAYS maxChunkSize — it no longer differs from a chunk that
	 * filled exactly. The `physicalSize` check below can therefore never fire
	 * anymore; it's dead weight left over from the old grow-by-append model.
	 * Detecting the gap needs a different signal now (not addressed in this
	 * pass — flagged, not fixed).
	 */
	nextPointer(pointer: number, consumed: number): number {
		// TODO: maybe replace this with an iterator??
		const end = pointer + consumed;
		const size = this.readCursor();
		if (end >= size) return end;

		const index = Math.floor(pointer / this.maxChunkSize);
		const boundary = (index + 1) * this.maxChunkSize;
		const localEnd = end - index * this.maxChunkSize;
		if (localEnd >= this.maxChunkSize) return end; // filled exactly, no gap possible

		// The record just read out of this chunk must have inflated it already
		// (get/getAsync do that before decoding), but don't assume it — a caller
		// invoking this cold would otherwise stat a nonexistent raw file.
		if (!this.getChunkMap(index)) this.inflateChunkSync(index);
		const physicalSize = Deno.statSync(chunkPath(this.path, index)).size;
		if (localEnd === physicalSize) return boundary; // skip the padding gap
		return end;
	}

	// ── writes ─────────────────────────────────────────────────────────────

	/**
	 * Append `data` as one atomic record and return its pointer. A record
	 * NEVER straddles a chunk boundary: if it doesn't fit the active chunk's
	 * remaining space, that chunk is sealed early (its remainder becomes an
	 * unaddressed padding gap) and the whole record is written at the start of
	 * the next chunk instead. The returned pointer is NOT necessarily the
	 * pre-call `size()` — it always points at the record's real start, which
	 * may be past a padding gap.
	 *
	 * Internally this only ever LOCKS to reserve the space (compute the
	 * pointer, provision the target chunk file, bump `CURSOR`) — the actual
	 * byte copy happens afterward, unlocked, via `writeInto`. See the class
	 * doc's "writeInto vs append" section.
	 */
	append(data: Codec.InferInput<T>): number {
		const bytes = this.entry.encode(data);
		if (bytes.length > this.maxChunkSize) {
			throw new Error(`record of ${bytes.length} bytes exceeds maxChunkSize=${this.maxChunkSize}; can't fit in a chunk`);
		}

		const pointer = this.withCursorLock((current) => {
			let position = current;
			let index = Math.floor(position / this.maxChunkSize);
			const taken = position % this.maxChunkSize;
			const available = this.maxChunkSize - taken;

			// Won't fit in the current chunk's remainder — seal it short and skip
			// the padding by advancing straight to the next chunk boundary.
			if (bytes.length > available) {
				position += available; // logical padding; nothing written to disk
				index += 1;
			}

			// The target chunk must physically exist (fully sized) BEFORE the
			// cursor claims bytes in it — writeInto (called right after this
			// unlocks) requires its target range to already be <= CURSOR.
			ensureChunkFile(this.path, index, this.maxChunkSize);

			return { size: position + bytes.length, result: position };
		});

		this.writeInto(pointer, bytes);
		return pointer;
	}

	/**
	 * Overwrite `bytes.length` bytes in place starting at absolute byte
	 * `offset`. The whole target range must already be within the current
	 * cursor (`[0, size())`) — this can only patch already-live data, never
	 * extend the store (only `append`/`truncate` do that, since only they
	 * touch `CURSOR`). The target range must also fit within a single chunk
	 * from `offset` onward — this never spans a chunk boundary; it throws
	 * instead, same as `append` does for a record that doesn't fit.
	 *
	 * The patch stores straight into the chunk's writable mapping (MAP_SHARED
	 * write:true) — no open/seek/write/close per call beyond the chunk's own
	 * near-instant lock (see the class doc's "writeInto vs compression"), which
	 * only ever costs real waiting if this exact chunk is mid-way through
	 * committing a compress pass — vanishingly rare, and brief either way.
	 */
	writeInto(offset: number, bytes: Uint8Array): void {
		const size = this.readCursor();
		if (offset < 0 || offset + bytes.length > size) {
			throw new Error(`writeInto out of bounds offset=${offset} length=${bytes.length} size=${size}`);
		}

		const index = Math.floor(offset / this.maxChunkSize);
		const start = offset % this.maxChunkSize;
		const available = this.maxChunkSize - start;
		if (bytes.length > available) {
			throw new Error(
				`writeInto of ${bytes.length} bytes at offset=${offset} doesn't fit in chunk ${index}'s remaining ${available} bytes — a write may never straddle a chunk boundary`,
			);
		}

		withChunkLock(chunkLockPath(this.path, index), (lock) => {
			// Always re-probe fresh while holding the lock — a map obtained
			// before acquiring it could be stale, pointing at an inode a
			// concurrent compress commit already unlinked in favor of a fresh
			// .zst (see the class doc). If it's compressed-only, inflate it back
			// first — inflateChunkSyncLocked assumes we already hold this lock.
			this.chunkMaps[index] = undefined;
			let map = this.getChunkMap(index);
			if (!map) {
				this.inflateChunkSyncLocked(index);
				map = this.getChunkMap(index);
				if (!map) throw new Error(`chunk ${index} vanished after inflate`);
			}
			if (existsSync(chunkZstPath(this.path, index))) Deno.removeSync(chunkZstPath(this.path, index));
			if (this.compression) {
				const timer = this.inflatedTimers.get(index);
				if (timer !== undefined) {
					clearTimeout(timer);
					this.inflatedTimers.delete(index);
				}
			}

			// Mark this chunk as touched — compressChunk compares this against a
			// snapshot taken before it started compressing to detect whether
			// writeInto landed mid-pass (see the class doc).
			writeChunkGeneration(lock, crypto.getRandomValues(new Uint8Array(8)));

			// Store straight into the mapped pages (MAP_SHARED — lands in the page
			// cache immediately, visible to concurrent reads off the same map;
			// reaches disk on writeback or an explicit map.flush()).
			map.bytes.set(bytes, start);
		});
	}

	// Flush every currently-mapped chunk's dirty pages to disk. Writes land via
	// mmap now (writeInto/append), not through an append fd, so there's no
	// single "the appender" to fsync anymore — flush covers whichever chunks
	// this instance actually has open.
	sync() {
		for (const map of this.chunkMaps) map?.flush();
	}

	size(): number {
		return this.readCursor();
	}

	/**
	 * Move the cursor to `size`, growing or shrinking the store. Growing just
	 * provisions the chunk file(s) newly covered (zero-filled, nothing else
	 * written) — actually placing data in that space is `writeInto`'s job.
	 * Shrinking deletes orphaned higher-index chunks and zeroes the new tail
	 * chunk's now-stale bytes past the new logical end (truncated down to the
	 * new end, then back up to `maxChunkSize`, so nothing but zeros ever sits
	 * past the cursor).
	 *
	 * Only closes/unmaps the specific chunks it's about to delete or resize
	 * (SIGBUS safety — see the class doc) — unlike a full `close()`, the store
	 * stays open and usable afterward, and the background compression loop (if
	 * running) is left alone.
	 */
	truncate(size: number): void {
		if (size < 0) throw new Error(`truncate size must be >= 0, got ${size}`);

		this.withCursorLock((current) => {
			if (size > current) {
				const oldTailIndex = Math.floor(current / this.maxChunkSize);
				const newTailIndex = Math.floor(size / this.maxChunkSize);
				for (let index = oldTailIndex; index <= newTailIndex; index++) {
					ensureChunkFile(this.path, index, this.maxChunkSize);
				}
			} else if (size < current) {
				const oldTailIndex = Math.floor(current / this.maxChunkSize);
				const newTailIndex = Math.floor(size / this.maxChunkSize);
				const tailEnd = size % this.maxChunkSize;

				// Delete high-to-low: if a crash happens mid-truncate, the surviving
				// chunks are always a contiguous prefix [0..k], never a set with a
				// gap — a gap would make open() throw ("chunks are fucked") and brick
				// recovery. High-to-low guarantees no gap.
				for (let index = oldTailIndex; index > newTailIndex; index--) {
					this.closeChunkMap(index); // unmap before delete — SIGBUS safety
					const timer = this.inflatedTimers.get(index);
					if (timer !== undefined) {
						clearTimeout(timer);
						this.inflatedTimers.delete(index);
					}
					removeAllChunkForms(this.path, index);
				}

				// Zero the surviving tail chunk's now-stale bytes past the new
				// logical end: truncate down to tailEnd (drops them), then back up
				// to maxChunkSize (zero-fills the region back in) — rather than
				// leaving old bytes sitting past the cursor.
				this.closeChunkMap(newTailIndex); // unmap before resizing — SIGBUS safety
				const timer = this.inflatedTimers.get(newTailIndex);
				if (timer !== undefined) {
					clearTimeout(timer);
					this.inflatedTimers.delete(newTailIndex);
				}
				if (existsSync(chunkZstPath(this.path, newTailIndex))) Deno.removeSync(chunkZstPath(this.path, newTailIndex));
				ensureChunkFile(this.path, newTailIndex, this.maxChunkSize); // in case it didn't exist at all yet
				Deno.truncateSync(chunkPath(this.path, newTailIndex), tailEnd);
				Deno.truncateSync(chunkPath(this.path, newTailIndex), this.maxChunkSize);
			}

			return { size, result: undefined };
		});
	}

	// ── raw chunk maps (memory-mapped) ────────────────────────────────────
	// Each chunk_N file is memory-mapped (MAP_SHARED) ONCE and serves BOTH
	// paths off that one mapping for as long as the process runs:
	// - reads: `get`/`getAsync` decode straight out of the mapped bytes (no copy).
	// - writeInto: fixed-width patches store straight into the mapped bytes (no
	//   open/seek/write/close syscalls per patch — a huge win for rehash, which
	//   patches one inline link per entry across the whole log).
	// Always opened `write: true` — see the class doc; there's no read-only
	// mode anymore since multiple workers may need to writeInto the same
	// store. Since every chunk is always fully sized from creation, the map is
	// never stale — there's no remap-on-grow case anymore.
	//
	// undefined = never probed / needs re-probe. null = probed, raw file absent
	// (chunk is compressed and not yet inflated). An Mmap = raw file present and
	// mapped.
	//
	// SIGBUS SAFETY: touching a mapped page after the file shrinks/is-removed
	// under it is an uncatchable crash, not a JS error. Every site that shrinks
	// or removes a raw chunk (compressChunk, evictInflatedChunk, truncate) MUST
	// call closeChunkMap(index) — which unmaps — BEFORE the shrink/remove.
	private chunkMaps: (Mmap | null | undefined)[] = [];

	private closeChunkMap(index: number) {
		const map = this.chunkMaps[index];
		if (map) map.close(); // munmap — must precede any file shrink/remove
		this.chunkMaps[index] = undefined;
	}

	// Map chunk_N. Returns null if the raw file is absent (compressed-only,
	// not yet inflated). Cached in `chunkMaps`.
	private getChunkMap(index: number): Mmap | null {
		let map = this.chunkMaps[index];
		if (map !== undefined) return map;
		try {
			map = Mmap.openSync(chunkPath(this.path, index), { write: true });
			map.advise(Advice.Random);
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
			map = null;
		}
		this.chunkMaps[index] = map;
		return map;
	}

	// ── inflated-chunk lifetime ────────────────────────────────────────────
	// We NEVER keep decompressed bytes in memory. Decompressing a chunk means
	// recreating its raw chunk_N file on disk; subsequent reads then hit that
	// file through the normal raw-map path. The transient decompressed buffer
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

	// Runtime eviction of an inflated chunk: only if a .zst exists to fall back
	// to, then detach the map before deleting the raw file.
	private evictInflatedChunk(index: number): void {
		if (!existsSync(chunkZstPath(this.path, index))) return; // live raw chunk, don't touch
		this.closeChunkMap(index);
		this.chunkMaps[index] = null;
		revertChunkToCompressed(this.path, index);
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
		if (this.getChunkMap(index)) return; // already raw, nothing to do
		withChunkLock(chunkLockPath(this.path, index), () => this.inflateChunkSyncLocked(index));
	}

	// The actual inflate work — assumes the caller already holds this chunk's
	// lock (writeInto calls this directly while it's already holding it, to
	// avoid re-acquiring the same lock and deadlocking against itself).
	private inflateChunkSyncLocked(index: number): void {
		// Re-probe: another holder may have inflated it while we waited for the lock.
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
		Deno.renameSync(tmpPath, chunkPath(this.path, index)); // atomic swap-in
		const writeMs = performance.now() - started - readMs - decodeMs;
		console.log(
			`[compress] chunk ${index} inflated, ${raw.byteLength} bytes on disk ` +
				`(read ${readMs.toFixed(0)}ms, decode ${decodeMs.toFixed(0)}ms, write ${writeMs.toFixed(0)}ms)`,
		);

		this.chunkMaps[index] = undefined; // force re-open of the new raw file
		this.afterInflate(index);
	}

	private async inflateChunkAsync(index: number): Promise<void> {
		if (this.getChunkMap(index)) return;

		const inFlight = this.inflatingInFlight.get(index);
		if (inFlight) return inFlight;

		const promise = (async () => {
			const lock = Deno.openSync(chunkLockPath(this.path, index), { create: true, write: true, read: true });
			try {
				await lock.lock(true); // blocks until the other inflater releases
				this.chunkMaps[index] = undefined;
				if (this.getChunkMap(index)) return;

				console.log(`[compress] inflating chunk ${index} (async, streaming)`);
				const started = performance.now();
				const tmpPath = chunkRawTmpPath(this.path, index);
				// Stream .zst -> zstd inflate transform -> raw tmp file. Bounded peak
				// memory (a handful of 8 MiB buffers in flight, never the whole
				// chunk). The decode itself runs on the libuv threadpool, but every
				// output chunk hops back through the event loop to reach the sink —
				// which is why the buffer sizes matter: at the 16 KiB default this
				// was ~65k hops per chunk, at 8 MiB it's ~130.
				const source = createReadStream(chunkZstPath(this.path, index), { highWaterMark: INFLATE_STREAM_BUFFER_SIZE });
				const transform = zlib.createZstdDecompress(this.zstdDecompressStreamOptions);
				const sink = createWriteStream(tmpPath, { highWaterMark: INFLATE_STREAM_BUFFER_SIZE });
				await pipeline(source, transform, sink);
				await Deno.rename(tmpPath, chunkPath(this.path, index)); // atomic swap-in
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

	// Post-inflate bookkeeping shared by the sync and async paths.
	private afterInflate(index: number): void {
		if (this.compression) {
			// Track it: arm the TTL and enforce the count cap (may evict an older one).
			this.touchInflated(index);
		} else {
			// One-way inflate: the raw file is now the source of truth, drop .zst.
			Deno.removeSync(chunkZstPath(this.path, index), { recursive: true });
		}
	}

	// ── background compression loop ────────────────────────────────────────
	private disposed = false;
	private compressPool: CompressWorkerPool | undefined;

	/**
	 * Start the background compression loop — see the class doc's
	 * "Compression" section for the full model. Meant to be called from
	 * exactly one place for the store's whole lifetime; throws if called twice
	 * on the same instance. Runs for the process lifetime, stopped via
	 * `close()`.
	 */
	startCompression(options: CompressionOptions): void {
		if (this.compression) throw new Error("compression already started on this store");
		if (options.maxInflatedChunks < 1) throw new Error("compression.maxInflatedChunks must be >= 1");

		this.compression = options;
		this.zstdCompressOptions = mapZstdParams(options.zstd.compress, "ZSTD_c_");
		this.zstdDecompressOptions = mapZstdParams(options.zstd.decompress, "ZSTD_d_");
		this.zstdDecompressSyncOptions = { ...this.zstdDecompressSyncOptions, params: this.zstdDecompressOptions };
		this.zstdDecompressStreamOptions = { ...this.zstdDecompressStreamOptions, params: this.zstdDecompressOptions };

		// Fire-and-forget: runs for the process lifetime, stopped via `disposed` in close().
		this.runCompressionLoop().catch((e) => {
			console.error("[compress] background loop died:", e);
		});
	}

	private async runCompressionLoop(): Promise<void> {
		const pool = new CompressWorkerPool(COMPRESS_PARALLELISM);
		this.compressPool = pool;

		// index -> in-flight promise, so we never dispatch the same chunk twice.
		const inFlight = new Map<number, Promise<void>>();

		try {
			while (!this.disposed) {
				let dispatchedSomething = false;
				// Never touch the active tail chunk — it's still being appended to.
				// Re-read the cursor on every outer pass since the tail advances as
				// IBD progresses (no cached size anymore — see the class doc).
				const tailIndex = Math.floor(this.readCursor() / this.maxChunkSize);

				for (let index = 0; index < tailIndex && !this.disposed; index++) {
					if (inFlight.has(index)) continue; // already being compressed
					if (existsSync(chunkZstPath(this.path, index))) continue; // already compressed

					let rawStat: Deno.FileInfo;
					try {
						rawStat = await Deno.stat(chunkPath(this.path, index));
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
		const rawPath = chunkPath(this.path, index);
		const tmpPath = chunkZstTmpPath(this.path, index);
		const zstPath = chunkZstPath(this.path, index);
		const lockPath = chunkLockPath(this.path, index);

		const started = performance.now();

		// Snapshot this chunk's generation marker BEFORE the slow read+compress
		// starts — held only long enough to read one small file, not for the
		// compress itself. Compared again after, under the same lock, to detect
		// whether writeInto touched the chunk anywhere in between (see the class
		// doc's "writeInto vs compression").
		const genAtStart = withChunkLock(lockPath, readChunkGeneration);

		// The worker streams raw -> zstd -> tmp on its own OS thread; nothing large
		// crosses the isolate boundary and the main event loop stays free. No lock
		// is held here — writers never wait on the compress itself.
		const zstSize = await pool.compress(index, rawPath, tmpPath, this.zstdCompressOptions);

		if (this.disposed) {
			// Pool was torn down while this job ran; don't mutate map state on a
			// closing store. Leave the .tmp for the next open to clean up.
			return;
		}

		// Commit under the lock — a near-instant critical section, no I/O
		// proportional to chunk size happens while holding it. If the generation
		// marker changed, writeInto landed sometime during the compress: the
		// .zst we just made is stale — discard it and leave the raw file
		// completely untouched (a later pass just retries once things settle).
		// Otherwise commit: rename the .zst in, delete the now-superseded raw.
		const committed = withChunkLock(lockPath, (lock) => {
			if (!bytesEqual(readChunkGeneration(lock), genAtStart)) return false;
			this.closeChunkMap(index); // unmap BEFORE removing the raw file — SIGBUS safety
			Deno.renameSync(tmpPath, zstPath); // atomic on the same filesystem
			Deno.removeSync(rawPath);
			this.chunkMaps[index] = null; // any cached map for this index must now fall back to compressed
			return true;
		});

		if (!committed) {
			console.log(`[compress] chunk ${index} changed mid-compress — discarding this pass, will retry`);
			await Deno.remove(tmpPath).catch(() => {});
			return;
		}

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
		for (const map of this.chunkMaps) map?.close();
		this.chunkMaps.length = 0;
		for (const timer of this.inflatedTimers.values()) clearTimeout(timer);
		this.inflatedTimers.clear();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
