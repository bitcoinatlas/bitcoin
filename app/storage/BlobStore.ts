import { Codec } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import type { Batch, Store, WAL } from "~/storage/Store.ts";
import { writeFile } from "~/utils/fs.ts";
import { Uint8ArrayView } from "~/utils/Uint8ArrayView.ts";
import { MAX_BLOCK_SIZE } from "~/constants.ts";

/**
 * Append-only store for variable-size blobs over a single logical byte stream.
 *
 * The stream is split into fixed-size *file chunks* on disk (chunk_0, chunk_1, …) and,
 * before they are flushed, into fixed-size *memory chunks* in RAM. Both use the same
 * addressing: a logical byte offset `p` maps to (floor(p/size), p%size). Reads and writes
 * walk chunks and copy across boundaries — one loop, whether the bytes live on disk or in RAM.
 *
 * Layering, in offset order:
 *
 *   disk [0, diskLength)  ->  frozen [base, base+len)  ->  staged [stagedBase, stagedBase+len)
 *
 * There are no per-blob records and no pointers stored alongside the data: staged/frozen are
 * just the raw tail of the stream held in 4 MiB memory chunks. append() copies bytes in and
 * returns the offset where they landed (that offset is the caller's blob pointer). A read is a
 * straight range partition across the three regions — no scan.
 *
 * Non-blocking flush: createWAL() moves the staged memory chunks into `frozen` and resets
 * staged in the same tick, so new appends proceed while apply() copies frozen -> disk in the
 * background (Atomic runs flush unawaited). Reads cap disk at `frozen.base`, so apply()'s
 * incremental disk growth is never seen as a torn read; frozen serves that range until discard().
 *
 * WAL format: [u64 base LE][u64 byteLen LE][raw bytes]. apply() truncates disk back to `base`
 * then appends the bytes — idempotent and self-healing.
 *
 * Truncate (reorg): discards all staged data and shrinks disk to `newLength`, relative to the
 * flushed (disk) length. A target-length sentinel makes the shrink crash-safe.
 *
 * Single writer assumed (one batch at a time, enforced). Reads do not race a commit.
 *
 * fds: each file chunk keeps a cached read handle and a cached append handle on the instance,
 * so the hot path of millions of tiny random reads pays no per-read open/close. Handles are
 * evicted when their chunk is truncated/removed. Memory chunks are pooled across flushes to
 * cut allocation churn. close() / [Symbol.dispose] release all fds.
 *
 * Recycling safety: every Uint8Array returned to a caller is a fresh copy, never a subarray
 * view into a poolable memory chunk, so recycling a buffer can never corrupt a held value.
 */
export interface BlobStoreBatch extends Batch {
	/** Stage a blob for append. Returns the logical offset where it begins. */
	append(data: Uint8Array): number;
	get(pointer: number, length: number): Promise<Uint8Array>;
	// deno-lint-ignore no-explicit-any
	get<T>(pointer: number, codec: Codec<T, any>, options?: { readAheadSize?: number }): Promise<T>;
	size(): number;
}

export type BlobStoreOptions = {
	path: string;
	/** Max size per on-disk chunk file in bytes. Default 1 GiB. */
	chunkByteSize?: number;
	/** Size of each in-memory staging chunk in bytes. Default 4 MiB. */
	memChunkSize?: number;
};

/** A region held in fixed-size memory chunks: bytes start at `base`, `len` are in use. */
type MemRegion = {
	base: number;
	chunks: Uint8Array[];
	len: number;
};

type ReadSnapshot = {
	frozen: MemRegion | null;
	staged: MemRegion;
	diskLength: number;
};

/** Cached file handles for one on-disk chunk. */
type ChunkFds = {
	read?: Deno.FsFile;
	append?: Deno.FsFile;
};

export class BlobStore implements Store<BlobStoreBatch> {
	readonly #path: string;
	readonly #chunkByteSize: number;
	readonly #memChunkSize: number;
	readonly #walPath: string;
	readonly #truncatePath: string;

	/** Bytes physically on disk across chunk files. Authoritative file state. */
	#diskLength: number;
	/** Committed-but-unflushed bytes, held in memory chunks. */
	#staged: MemRegion;
	/** Set during a flush; serves reads of the in-flight range until discard(). */
	#frozen: MemRegion | null = null;

	#batchOpen = false;
	#truncating = false;

	/** Per-file-chunk cached read/append handles, keyed by file chunk index. */
	#fds = new Map<number, ChunkFds>();
	/** Recycled memory chunks, reused by staging to avoid reallocation. Capped. */
	#freeList: Uint8Array[] = [];
	/** Reused readahead buffer for variable-stride codec reads (single reader at a time). */
	#scratch: Uint8Array = new Uint8Array(0);

	wal: WAL | null = null;

	private constructor(
		path: string,
		chunkByteSize: number,
		memChunkSize: number,
		walPath: string,
		truncatePath: string,
		diskLength: number,
	) {
		this.#path = path;
		this.#chunkByteSize = chunkByteSize;
		this.#memChunkSize = memChunkSize;
		this.#walPath = walPath;
		this.#truncatePath = truncatePath;
		this.#diskLength = diskLength;
		this.#staged = { base: diskLength, chunks: [], len: 0 };
	}

	static async open(options: BlobStoreOptions): Promise<BlobStore> {
		const { path } = options;
		const chunkByteSize = options.chunkByteSize ?? 1 * 1024 * 1024 * 1024; // 1 GiB
		const memChunkSize = options.memChunkSize ?? 4 * 1024 * 1024; // 4 MiB
		const walPath = join(path, "data.wal");
		const truncatePath = join(path, "truncate.target");

		await Deno.mkdir(path, { recursive: true });

		// Disk length = (maxIndex full chunks) + size of the last chunk.
		let maxIndex = -1;
		for await (const entry of Deno.readDir(path)) {
			if (entry.isFile && entry.name.startsWith("chunk_")) {
				const i = parseInt(entry.name.slice(6), 10);
				if (!isNaN(i) && i > maxIndex) maxIndex = i;
			}
		}
		let diskLength = 0;
		if (maxIndex >= 0) {
			const lastSize = (await Deno.stat(join(path, `chunk_${maxIndex}`))).size;
			diskLength = maxIndex * chunkByteSize + lastSize;
		}

		const store = new BlobStore(path, chunkByteSize, memChunkSize, walPath, truncatePath, diskLength);

		// Crash-safe truncate recovery (idempotent), before WAL detection (mutually exclusive).
		if (await exists(truncatePath)) {
			const buf = await Deno.readFile(truncatePath);
			const target = Number(new Uint8ArrayView(buf).getBigUint64(0));
			await store.#truncateDiskToOffset(target);
			await Deno.remove(truncatePath).catch(() => {});
		}

		// A WAL on disk means a flush was interrupted; expose it for recovery.
		if (await exists(walPath)) {
			store.wal = store.#makeWal();
		}

		return store;
	}

	// -- memory-chunk helpers --------------------------------------------------

	#allocMemChunk(): Uint8Array {
		return this.#freeList.pop() ?? new Uint8Array(this.#memChunkSize);
	}

	/** Return memory chunks to the free list, capped so an outlier tick can't pin memory. */
	#recycle(chunks: Uint8Array[]): void {
		const CAP = 64; // 64 x 4 MiB = 256 MiB ceiling on the pool
		for (const c of chunks) {
			if (this.#freeList.length >= CAP) break;
			this.#freeList.push(c);
		}
	}

	#emptyStaged(base: number): MemRegion {
		return { base, chunks: [], len: 0 };
	}

	/** Copy `data` into a memory region at its current end, allocating chunks as needed. */
	#appendToRegion(region: MemRegion, data: Uint8Array): void {
		let written = 0;
		while (written < data.length) {
			const pos = region.len; // offset within the region
			const chunkIndex = Math.floor(pos / this.#memChunkSize);
			const offsetInChunk = pos % this.#memChunkSize;
			if (chunkIndex === region.chunks.length) region.chunks.push(this.#allocMemChunk());
			const chunk = region.chunks[chunkIndex]!;
			const take = Math.min(this.#memChunkSize - offsetInChunk, data.length - written);
			chunk.set(data.subarray(written, written + take), offsetInChunk);
			written += take;
			region.len += take;
		}
	}

	/** Copy bytes [start, start+want) of a region into `dst` at `dstOffset`. Returns bytes copied. */
	#readFromRegion(region: MemRegion, start: number, dst: Uint8Array, dstOffset: number, want: number): number {
		let copied = 0;
		let pos = start; // offset within the region
		while (copied < want && pos < region.len) {
			const chunkIndex = Math.floor(pos / this.#memChunkSize);
			const offsetInChunk = pos % this.#memChunkSize;
			const chunk = region.chunks[chunkIndex]!;
			const take = Math.min(this.#memChunkSize - offsetInChunk, region.len - pos, want - copied);
			dst.set(chunk.subarray(offsetInChunk, offsetInChunk + take), dstOffset + copied);
			copied += take;
			pos += take;
		}
		return copied;
	}

	/** Flatten a region's used bytes into one contiguous buffer (for WAL encoding). */
	#flattenRegion(region: MemRegion): Uint8Array {
		const out = new Uint8Array(region.len);
		this.#readFromRegion(region, 0, out, 0, region.len);
		return out;
	}

	// -- file-handle cache ------------------------------------------------------

	#getReadHandle(chunkIndex: number): Deno.FsFile | null {
		let fds = this.#fds.get(chunkIndex);
		if (fds?.read) return fds.read;
		const chunkPath = join(this.#path, `chunk_${chunkIndex}`);
		let file: Deno.FsFile;
		try {
			file = Deno.openSync(chunkPath, { read: true });
		} catch (e) {
			if (e instanceof Deno.errors.NotFound) return null;
			throw e;
		}
		if (!fds) this.#fds.set(chunkIndex, fds = {});
		fds.read = file;
		return file;
	}

	#getAppendHandle(chunkIndex: number): Deno.FsFile {
		let fds = this.#fds.get(chunkIndex);
		if (fds?.append) return fds.append;
		const chunkPath = join(this.#path, `chunk_${chunkIndex}`);
		const file = Deno.openSync(chunkPath, { create: true, write: true });
		if (!fds) this.#fds.set(chunkIndex, fds = {});
		fds.append = file;
		return file;
	}

	#evictFds(chunkIndex: number): void {
		const fds = this.#fds.get(chunkIndex);
		if (!fds) return;
		try {
			fds.read?.close();
		} catch { /* already closed */ }
		try {
			fds.append?.close();
		} catch { /* already closed */ }
		this.#fds.delete(chunkIndex);
	}

	/** Close all cached file handles. Call when discarding the store. */
	close(): void {
		for (const [, fds] of this.#fds) {
			try {
				fds.read?.close();
			} catch { /* already closed */ }
			try {
				fds.append?.close();
			} catch { /* already closed */ }
		}
		this.#fds.clear();
	}

	[Symbol.dispose](): void {
		this.close();
	}

	// -- snapshot / length ------------------------------------------------------

	#snapshot(): ReadSnapshot {
		return { frozen: this.#frozen, staged: this.#staged, diskLength: this.#diskLength };
	}

	length(): number {
		const snap = this.#snapshot();
		return snap.staged.base + snap.staged.len;
	}

	// -- reads ------------------------------------------------------------------

	async #readInto(
		snap: ReadSnapshot,
		pointer: number,
		buf: Uint8Array,
		allowEOF: boolean,
		extra?: MemRegion,
	): Promise<number> {
		if (pointer < 0) throw new Error("pointer must be non-negative");

		let bytesRead = 0;
		let cur = pointer;
		const diskBoundary = snap.frozen ? snap.frozen.base : snap.diskLength;

		// --- disk region [0, diskBoundary): synchronous reads against cached read fds ---
		while (bytesRead < buf.length && cur < diskBoundary) {
			const chunkIndex = Math.floor(cur / this.#chunkByteSize);
			const offsetInChunk = cur % this.#chunkByteSize;
			const want = Math.min(
				buf.length - bytesRead,
				diskBoundary - cur,
				this.#chunkByteSize - offsetInChunk,
			);

			const file = this.#getReadHandle(chunkIndex);
			if (!file) {
				if (bytesRead === 0) throw new Error(`Chunk ${chunkIndex} not found for pointer ${pointer}`);
				break;
			}
			file.seekSync(offsetInChunk, Deno.SeekMode.Start);
			let got = 0;
			while (got < want) {
				const n = file.readSync(buf.subarray(bytesRead + got, bytesRead + want));
				if (n === null) break;
				got += n;
			}
			bytesRead += got;
			cur += got;
			if (got < want) break;
		}

		// --- frozen region [base, base+len): in-memory ---
		if (bytesRead < buf.length && snap.frozen && cur >= snap.frozen.base) {
			const start = cur - snap.frozen.base;
			if (start < snap.frozen.len) {
				const n = this.#readFromRegion(snap.frozen, start, buf, bytesRead, buf.length - bytesRead);
				bytesRead += n;
				cur += n;
			}
		}

		// --- staged region, then batch-local extra (same shape) ---
		for (const region of extra ? [snap.staged, extra] : [snap.staged]) {
			if (bytesRead >= buf.length) break;
			if (cur < region.base) continue;
			const start = cur - region.base;
			if (start >= region.len) continue;
			const n = this.#readFromRegion(region, start, buf, bytesRead, buf.length - bytesRead);
			bytesRead += n;
			cur += n;
		}

		if (!allowEOF && bytesRead < buf.length) {
			throw new Error("Unexpected EOF reading blob");
		}
		return bytesRead;
	}

	get(pointer: number, length: number): Promise<Uint8Array>;
	// deno-lint-ignore no-explicit-any
	get<T>(pointer: number, codec: Codec<T, any>, options?: { readAheadSize?: number }): Promise<T>;
	async get<T>(
		pointer: number,
		// deno-lint-ignore no-explicit-any
		lengthOrCodec: number | Codec<T, any>,
		options?: { readAheadSize?: number },
	): Promise<Uint8Array | T> {
		const snap = this.#snapshot();
		if (typeof lengthOrCodec === "number") {
			const buf = new Uint8Array(lengthOrCodec); // fresh copy -- safe to return
			await this.#readInto(snap, pointer, buf, false);
			return buf;
		}
		const codec = lengthOrCodec;
		const explicit = options?.readAheadSize;
		if (codec.stride.kind === "fixed" && explicit === undefined) {
			const buf = new Uint8Array(codec.stride.size);
			const n = await this.#readInto(snap, pointer, buf, true);
			const [value] = codec.decode(buf.subarray(0, n));
			return value;
		}
		// Variable stride: decode out of a reused scratch buffer (never returned to caller).
		const readAhead = explicit ?? MAX_BLOCK_SIZE;
		if (this.#scratch.length < readAhead) this.#scratch = new Uint8Array(readAhead);
		const buf = this.#scratch.subarray(0, readAhead);
		const n = await this.#readInto(snap, pointer, buf, true);
		const [value] = codec.decode(buf.subarray(0, n));
		return value;
	}

	// -- batch ------------------------------------------------------------------

	batch(): BlobStoreBatch {
		if (this.#batchOpen) throw new Error("A batch is already open");
		if (this.#truncating) throw new Error("Can't start a batch while a truncate is in progress");
		this.#batchOpen = true;

		const batchBase = this.length();
		// Batch-local staging region, merged into #staged on apply().
		const region: MemRegion = this.#emptyStaged(batchBase);
		let live = true;

		const close = () => {
			live = false;
			this.#batchOpen = false;
		};

		return {
			append: (data: Uint8Array): number => {
				if (!live) throw new Error("Batch already settled");
				const pointer = region.base + region.len;
				this.#appendToRegion(region, data); // copies -- caller may reuse the buffer
				return pointer;
			},
			get: async (
				pointer: number,
				// deno-lint-ignore no-explicit-any
				lengthOrCodec: number | Codec<any, any>,
				options?: { readAheadSize?: number },
				// deno-lint-ignore no-explicit-any
			): Promise<any> => {
				if (!live) throw new Error("Batch already settled");
				const snap = this.#snapshot();
				if (typeof lengthOrCodec === "number") {
					const buf = new Uint8Array(lengthOrCodec);
					await this.#readInto(snap, pointer, buf, false, region);
					return buf;
				}
				const codec = lengthOrCodec;
				const readAhead = options?.readAheadSize ?? (codec.stride.kind === "fixed" ? codec.stride.size : 4096);
				const buf = new Uint8Array(readAhead);
				const n = await this.#readInto(snap, pointer, buf, true, region);
				const [value] = codec.decode(buf.subarray(0, n));
				return value;
			},
			size: (): number => region.base + region.len,
			apply: (): void => {
				if (!live) throw new Error("Batch already settled");
				// Merge batch-local bytes into #staged. Both share #memChunkSize, and the batch
				// began at this.length(), so staged is contiguous with it.
				for (let off = 0; off < region.len;) {
					const chunkIndex = Math.floor(off / this.#memChunkSize);
					const offsetInChunk = off % this.#memChunkSize;
					const chunk = region.chunks[chunkIndex]!;
					const take = Math.min(this.#memChunkSize - offsetInChunk, region.len - off);
					this.#appendToRegion(this.#staged, chunk.subarray(offsetInChunk, offsetInChunk + take));
					off += take;
				}
				this.#recycle(region.chunks);
				close();
			},
			discard: (): void => {
				if (!live) return;
				this.#recycle(region.chunks);
				close();
			},
		};
	}

	// -- truncate ---------------------------------------------------------------

	async truncate(newLength: number): Promise<void> {
		if (this.#batchOpen) throw new Error("Can't truncate while a batch is open");
		if (this.#frozen || this.wal) throw new Error("Can't truncate while a flush is in progress");
		if (this.#truncating) throw new Error("A truncate is already in progress");
		if (newLength < 0) throw new Error("newLength must be non-negative");
		if (this.#staged.len > 0) {
			throw new Error("Can't truncate while staged data is present; flush first");
		}
		if (newLength > this.#diskLength) {
			throw new Error(
				`newLength (${newLength}) exceeds flushed length (${this.#diskLength}); flush before truncating into staged data`,
			);
		}

		this.#truncating = true;
		try {
			this.#recycle(this.#staged.chunks);
			this.#staged = this.#emptyStaged(newLength);

			if (newLength < this.#diskLength) {
				await this.#writeTruncateTarget(newLength);
				await this.#truncateDiskToOffset(newLength);
				await this.#removeTruncateTarget();
			}
		} finally {
			this.#truncating = false;
		}
	}

	async #writeTruncateTarget(n: number): Promise<void> {
		const buf = new Uint8Array(8);
		new Uint8ArrayView(buf).setBigUint64(0, BigInt(n));
		await Deno.writeFile(this.#truncatePath, buf, { create: true });
	}

	async #removeTruncateTarget(): Promise<void> {
		await Deno.remove(this.#truncatePath).catch(() => {});
	}

	// -- flush / WAL ------------------------------------------------------------

	async flush(): Promise<void> {
		const wal = await this.createWAL();
		await wal.apply();
		await wal.discard();
	}

	async createWAL(): Promise<WAL> {
		if (this.#batchOpen) throw new Error("Can't start a flush while a batch is open");
		if (this.#truncating) throw new Error("Can't start a flush while a truncate is in progress");
		if (this.#frozen || this.wal) throw new Error("A flush is already in progress");

		// Freeze staged in place, install a fresh empty staged in the same tick.
		const frozen = this.#staged;
		this.#frozen = frozen;
		this.#staged = this.#emptyStaged(frozen.base + frozen.len);

		const bytes = this.#flattenRegion(frozen);
		const buf = new Uint8Array(16 + bytes.length);
		const view = new Uint8ArrayView(buf);
		view.setBigUint64(0, BigInt(frozen.base));
		view.setBigUint64(8, BigInt(bytes.length));
		buf.set(bytes, 16);
		await Deno.writeFile(this.#walPath, buf, { create: true });

		this.wal = this.#makeWal();
		return this.wal;
	}

	#makeWal(): WAL {
		const apply = async (): Promise<void> => {
			const buf = await Deno.readFile(this.#walPath);
			const view = new Uint8ArrayView(buf);
			const base = Number(view.getBigUint64(0));
			const byteLen = Number(view.getBigUint64(8));
			await this.#truncateDiskToOffset(base);
			await this.#appendToDisk(buf.subarray(16, 16 + byteLen));
		};

		const discard = async (): Promise<void> => {
			if (this.#frozen) {
				this.#recycle(this.#frozen.chunks);
				this.#frozen = null;
			}
			this.wal = null;
			await Deno.remove(this.#walPath).catch(() => {});
		};

		return { apply, discard };
	}

	/** Append a contiguous buffer to disk, splitting across file chunks, via cached append fds. */
	async #appendToDisk(data: Uint8Array): Promise<void> {
		let written = 0;
		while (written < data.length) {
			const cur = this.#diskLength;
			const chunkIndex = Math.floor(cur / this.#chunkByteSize);
			const offsetInChunk = cur % this.#chunkByteSize;
			const take = Math.min(this.#chunkByteSize - offsetInChunk, data.length - written);

			const file = this.#getAppendHandle(chunkIndex);
			file.seekSync(offsetInChunk, Deno.SeekMode.Start);
			await writeFile(file, data.subarray(written, written + take));

			written += take;
			this.#diskLength += take;

			// Rotating to a new file chunk: the just-filled chunk's append fd is done.
			if (this.#diskLength % this.#chunkByteSize === 0) this.#evictFds(chunkIndex);
		}
	}

	/** Truncate chunk files so the stream ends at exactly `offset`. Sets #diskLength = offset. */
	async #truncateDiskToOffset(offset: number): Promise<void> {
		// Shrink logical length first (sync, before any await) so concurrent reads cap at `offset`.
		this.#diskLength = offset;
		// Evict every cached fd: chunks at/after the boundary are removed or trimmed, and an
		// append fd's internal position would also be stale.
		this.close();
		for await (const entry of Deno.readDir(this.#path)) {
			if (!entry.isFile || !entry.name.startsWith("chunk_")) continue;
			const i = parseInt(entry.name.slice(6), 10);
			if (isNaN(i)) continue;
			const chunkStart = i * this.#chunkByteSize;
			if (chunkStart >= offset) {
				await Deno.remove(join(this.#path, entry.name));
			} else if (chunkStart + this.#chunkByteSize > offset) {
				// chunk straddles the boundary -- trim it (strict ">" leaves a boundary-aligned
				// preceding chunk untouched).
				await Deno.truncate(join(this.#path, entry.name), offset - chunkStart);
			}
			// else: chunk fully below offset -- leave it
		}
	}
}
