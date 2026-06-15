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
 * The stream is laid out in four contiguous regions, low offset to high:
 *
 *   disk [0, diskEnd) -> frozen [frozen.base, ...) -> staged [stagedBase, ...) -> batch [batchBase, ...)
 *
 * `disk` is the durable tail held in fixed-size chunk files (1 GiB each). `staged` is
 * committed-but-unflushed bytes in memory. `frozen` is the snapshot of `staged` taken when a
 * flush starts; it serves reads of the in-flight range until the flush is discarded. `batch`
 * is the optional uncommitted region of the single open batch.
 *
 * Because the stream is append-only the regions never overlap, so a read of [p, p+len) is a
 * straight arithmetic partition across them -- no scan, no per-blob records. append() copies
 * bytes into the active region and returns the offset where they landed; that offset IS the
 * blob pointer.
 *
 * Key invariant -- stagedBase is DERIVED, never stored:
 *
 *   stagedBase = frozen ? frozen.base + frozen.length : disk.length
 *
 * While a flush is live, disk physically grows from frozen.base toward frozen.base+len, but
 * stagedBase stays pinned to frozen.base+len, so the staged region (and any batch opened during
 * the flush) never shifts. Reads cap disk at frozen.base and let `frozen` serve that range, so a
 * half-applied disk is never observed as a torn read. After discard, disk.length == frozen.base+len,
 * so stagedBase is unchanged -- the layout is seamless across the discard.
 *
 * WAL: createWAL() freezes `staged`, installs a fresh empty `staged`, and writes
 * [u64 base][u64 byteLen][bytes] to disk (fsync'd). apply() truncates disk back to `base` then
 * appends the bytes (fsync'd) -- idempotent and self-healing, so it can be replayed any number of
 * times after a crash. discard() drops `frozen` and deletes the WAL file; it does NOT truncate
 * (apply already wrote the data; truncating here would undo it on the happy path).
 *
 * truncate() (reorg) is the odd one out: it requires no batch, no flush, and no staged data, and
 * shrinks disk directly. A target-length sentinel makes the shrink crash-safe.
 *
 * Single writer, single reader, one batch at a time (all enforced).
 */

// ---------------------------------------------------------------------------
// Stage: an in-memory, append-only, chunked byte region.
//
// Bytes live in fixed-size chunks backed by resizable ArrayBuffers. Every chunk except the last
// is exactly `chunkSize` bytes, so a logical offset maps to (floor(off/chunkSize), off%chunkSize)
// with no search. Stages are never pooled -- a fresh one is created per batch / per freeze.
// ---------------------------------------------------------------------------

type StageChunk = { buf: ArrayBuffer; view: Uint8Array };

export class Stage {
	readonly #chunkSize: number;
	#chunks: StageChunk[] = [];
	#len = 0;

	constructor(chunkSize: number) {
		this.#chunkSize = chunkSize;
	}

	get length(): number {
		return this.#len;
	}

	/** Copy `data` onto the end of the stage, growing / adding chunks as needed. */
	append(data: Uint8Array): void {
		let off = 0;
		while (off < data.length) {
			let last = this.#chunks[this.#chunks.length - 1];
			if (!last || last.buf.byteLength === this.#chunkSize) {
				const buf = new ArrayBuffer(0, { maxByteLength: this.#chunkSize });
				last = { buf, view: new Uint8Array(buf) }; // length-tracking view follows buf.resize()
				this.#chunks.push(last);
			}
			const used = last.buf.byteLength;
			const take = Math.min(this.#chunkSize - used, data.length - off);
			last.buf.resize(used + take);
			last.view.set(data.subarray(off, off + take), used);
			off += take;
			this.#len += take;
		}
	}

	/** Collapse another stage onto the end of this one (re-packs into this stage's chunking). */
	appendStage(other: Stage): void {
		for (const chunk of other.#chunks) this.append(chunk.view);
	}

	/** Copy [start, start+want) into `dst` at `dstOffset`. Returns bytes copied (clamped to length). */
	readInto(start: number, dst: Uint8Array, dstOffset: number, want: number): number {
		let copied = 0;
		let chunkIndex = Math.floor(start / this.#chunkSize);
		let offsetInChunk = start % this.#chunkSize;
		while (copied < want && chunkIndex < this.#chunks.length) {
			const view = this.#chunks[chunkIndex]!.view;
			const avail = view.length - offsetInChunk;
			if (avail <= 0) break;
			const take = Math.min(avail, want - copied);
			dst.set(view.subarray(offsetInChunk, offsetInChunk + take), dstOffset + copied);
			copied += take;
			chunkIndex += 1;
			offsetInChunk = 0;
		}
		return copied;
	}

	/** Iterate the used bytes chunk by chunk (for streaming a WAL to disk without flattening). */
	*chunks(): Generator<Uint8Array> {
		for (const chunk of this.#chunks) yield chunk.view;
	}
}

// ---------------------------------------------------------------------------
// Disk: the durable tail, split across fixed-size chunk files (chunk_0, chunk_1, ...).
//
// Reads are synchronous (cached read fds + readSync); appends are async (cached append fd on the
// current tail chunk, evicted on rotation). Same (floor, mod) addressing as Stage.
// ---------------------------------------------------------------------------

type ChunkFds = { read?: Deno.FsFile; append?: Deno.FsFile };

export class Disk {
	readonly #path: string;
	readonly #chunkSize: number;
	#length: number;
	#fds = new Map<number, ChunkFds>();

	private constructor(path: string, chunkSize: number, length: number) {
		this.#path = path;
		this.#chunkSize = chunkSize;
		this.#length = length;
	}

	static async open(path: string, chunkSize: number): Promise<Disk> {
		await Deno.mkdir(path, { recursive: true });
		let maxIndex = -1;
		for await (const entry of Deno.readDir(path)) {
			if (entry.isFile && entry.name.startsWith("chunk_")) {
				const i = parseInt(entry.name.slice(6), 10);
				if (!isNaN(i) && i > maxIndex) maxIndex = i;
			}
		}
		let length = 0;
		if (maxIndex >= 0) {
			const lastSize = (await Deno.stat(join(path, `chunk_${maxIndex}`))).size;
			length = maxIndex * chunkSize + lastSize;
		}
		return new Disk(path, chunkSize, length);
	}

	get length(): number {
		return this.#length;
	}

	#getReadHandle(chunkIndex: number): Deno.FsFile | null {
		let fds = this.#fds.get(chunkIndex);
		if (fds?.read) return fds.read;
		let file: Deno.FsFile;
		try {
			file = Deno.openSync(join(this.#path, `chunk_${chunkIndex}`), { read: true });
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
		const file = Deno.openSync(join(this.#path, `chunk_${chunkIndex}`), { create: true, write: true });
		if (!fds) this.#fds.set(chunkIndex, fds = {});
		fds.append = file;
		return file;
	}

	#evict(chunkIndex: number): void {
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

	/** Sync reads against cached read fds. Returns bytes copied (may be short at EOF / missing chunk). */
	readInto(start: number, dst: Uint8Array, dstOffset: number, want: number): number {
		let copied = 0;
		let cur = start;
		while (copied < want) {
			const chunkIndex = Math.floor(cur / this.#chunkSize);
			const offsetInChunk = cur % this.#chunkSize;
			const take = Math.min(want - copied, this.#chunkSize - offsetInChunk);
			const file = this.#getReadHandle(chunkIndex);
			if (!file) break;
			file.seekSync(offsetInChunk, Deno.SeekMode.Start);
			let got = 0;
			while (got < take) {
				const n = file.readSync(dst.subarray(dstOffset + copied + got, dstOffset + copied + take));
				if (n === null) break;
				got += n;
			}
			copied += got;
			cur += got;
			if (got < take) break;
		}
		return copied;
	}

	/** Append a contiguous buffer, splitting across chunk files. */
	async append(data: Uint8Array): Promise<void> {
		let written = 0;
		while (written < data.length) {
			const cur = this.#length;
			const chunkIndex = Math.floor(cur / this.#chunkSize);
			const offsetInChunk = cur % this.#chunkSize;
			const take = Math.min(this.#chunkSize - offsetInChunk, data.length - written);
			const file = this.#getAppendHandle(chunkIndex);
			file.seekSync(offsetInChunk, Deno.SeekMode.Start);
			await writeFile(file, data.subarray(written, written + take));
			written += take;
			this.#length += take;
			if (this.#length % this.#chunkSize === 0) this.#evict(chunkIndex); // tail chunk filled
		}
	}

	/** fsync the open append handle(s). Durability barrier; call before a WAL may be discarded. */
	async sync(): Promise<void> {
		for (const [, fds] of this.#fds) {
			if (fds.append) await fds.append.sync();
		}
	}

	/** Shrink the stream to exactly `offset`. Sets length first (sync) so concurrent reads cap there. */
	async truncateTo(offset: number): Promise<void> {
		this.#length = offset;
		this.close(); // every cached fd's position / target may now be stale
		for await (const entry of Deno.readDir(this.#path)) {
			if (!entry.isFile || !entry.name.startsWith("chunk_")) continue;
			const i = parseInt(entry.name.slice(6), 10);
			if (isNaN(i)) continue;
			const chunkStart = i * this.#chunkSize;
			if (chunkStart >= offset) {
				await Deno.remove(join(this.#path, entry.name));
			} else if (chunkStart + this.#chunkSize > offset) {
				await Deno.truncate(join(this.#path, entry.name), offset - chunkStart);
			}
			// else: fully below offset -- leave it
		}
	}

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
}

// ---------------------------------------------------------------------------
// BlobStore: coordinates Disk + the in-memory regions, and owns the WAL protocol.
// ---------------------------------------------------------------------------

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
	/** Size of each in-memory staging chunk in bytes. Default 8 MiB. */
	memChunkSize?: number;
};

type FrozenRegion = { base: number; stage: Stage };
/** A region the read partition can pull from: absolute [base, base+length) + a reader. */
type ReadRegion = {
	base: number;
	length: number;
	read: (local: number, dst: Uint8Array, dstOffset: number, want: number) => number;
};

export class BlobStore implements Store<BlobStoreBatch> {
	readonly #disk: Disk;
	readonly #memChunkSize: number;
	readonly #walPath: string;
	readonly #truncatePath: string;

	#staged: Stage;
	#frozen: FrozenRegion | null = null;

	#batchOpen = false;
	#truncating = false;

	/** Reused readahead buffer for variable-stride codec reads (single reader assumed). */
	#scratch: Uint8Array = new Uint8Array(0);

	wal: WAL | null = null;

	private constructor(disk: Disk, memChunkSize: number, walPath: string, truncatePath: string) {
		this.#disk = disk;
		this.#memChunkSize = memChunkSize;
		this.#walPath = walPath;
		this.#truncatePath = truncatePath;
		this.#staged = new Stage(memChunkSize);
	}

	static async open(options: BlobStoreOptions): Promise<BlobStore> {
		const { path } = options;
		const chunkByteSize = options.chunkByteSize ?? 1 * 1024 * 1024 * 1024; // 1 GiB
		const memChunkSize = options.memChunkSize ?? 8 * 1024 * 1024; // 8 MiB
		const walPath = join(path, "data.wal");
		const truncatePath = join(path, "truncate.target");

		const disk = await Disk.open(path, chunkByteSize);
		const store = new BlobStore(disk, memChunkSize, walPath, truncatePath);

		// Crash-safe truncate recovery (idempotent), before WAL detection (mutually exclusive).
		if (await exists(truncatePath)) {
			const buf = await Deno.readFile(truncatePath);
			const target = Number(new Uint8ArrayView(buf).getBigUint64(0));
			await disk.truncateTo(target);
			await Deno.remove(truncatePath).catch(() => {});
		}

		// A WAL on disk means a flush was interrupted; expose it for recover().
		if (await exists(walPath)) store.wal = store.#makeWal();

		return store;
	}

	// -- layout ----------------------------------------------------------------

	/** Derived, never stored. See the class doc for why this is the load-bearing invariant. */
	#stagedBase(): number {
		return this.#frozen ? this.#frozen.base + this.#frozen.stage.length : this.#disk.length;
	}

	length(): number {
		return this.#stagedBase() + this.#staged.length;
	}

	// -- reads ------------------------------------------------------------------

	#regions(batch: FrozenRegion | null): ReadRegion[] {
		const diskEnd = this.#frozen ? this.#frozen.base : this.#disk.length;
		const regions: ReadRegion[] = [
			{ base: 0, length: diskEnd, read: (l, d, o, w) => this.#disk.readInto(l, d, o, w) },
		];
		if (this.#frozen) {
			const f = this.#frozen;
			regions.push({ base: f.base, length: f.stage.length, read: (l, d, o, w) => f.stage.readInto(l, d, o, w) });
		}
		regions.push({
			base: this.#stagedBase(),
			length: this.#staged.length,
			read: (l, d, o, w) => this.#staged.readInto(l, d, o, w),
		});
		if (batch) {
			regions.push({
				base: batch.base,
				length: batch.stage.length,
				read: (l, d, o, w) => batch.stage.readInto(l, d, o, w),
			});
		}
		return regions;
	}

	/** Fill `buf` from `pointer` by walking the contiguous regions. Returns bytes read. */
	#readInto(pointer: number, buf: Uint8Array, allowEOF: boolean, batch: FrozenRegion | null): number {
		if (pointer < 0) throw new Error("pointer must be non-negative");
		let cur = pointer;
		let got = 0;
		for (const region of this.#regions(batch)) {
			if (got >= buf.length) break;
			const end = region.base + region.length;
			if (cur >= end) continue; // entirely before this region
			if (cur < region.base) break; // hole between regions -- only reachable via a bad pointer
			const local = cur - region.base;
			const want = Math.min(buf.length - got, region.length - local);
			const n = region.read(local, buf, got, want);
			got += n;
			cur += n;
			if (n < want) break; // short read (e.g. disk EOF) -- stop
		}
		if (!allowEOF && got < buf.length) throw new Error("Unexpected EOF reading blob");
		return got;
	}

	// deno-lint-ignore no-explicit-any
	#read(
		pointer: number,
		lengthOrCodec: number | Codec<any, any>,
		options: { readAheadSize?: number } | undefined,
		batch: FrozenRegion | null,
	): unknown {
		if (typeof lengthOrCodec === "number") {
			const buf = new Uint8Array(lengthOrCodec); // fresh -- safe to return
			this.#readInto(pointer, buf, false, batch);
			return buf;
		}
		const codec = lengthOrCodec;
		const explicit = options?.readAheadSize;
		if (codec.stride.kind === "fixed" && explicit === undefined) {
			const buf = new Uint8Array(codec.stride.size);
			const n = this.#readInto(pointer, buf, true, batch);
			return codec.decode(buf.subarray(0, n))[0];
		}
		// Variable stride: decode out of a reused scratch buffer. The decoded value is a fresh
		// object (codec.decode allocates), never a view into scratch, so reuse is safe.
		const readAhead = explicit ?? MAX_BLOCK_SIZE;
		if (this.#scratch.length < readAhead) this.#scratch = new Uint8Array(readAhead);
		const buf = this.#scratch.subarray(0, readAhead);
		const n = this.#readInto(pointer, buf, true, batch);
		return codec.decode(buf.subarray(0, n))[0];
	}

	get(pointer: number, length: number): Promise<Uint8Array>;
	// deno-lint-ignore no-explicit-any
	get<T>(pointer: number, codec: Codec<T, any>, options?: { readAheadSize?: number }): Promise<T>;
	// deno-lint-ignore no-explicit-any
	async get<T>(
		pointer: number,
		lengthOrCodec: number | Codec<T, any>,
		options?: { readAheadSize?: number },
	): Promise<Uint8Array | T> {
		return this.#read(pointer, lengthOrCodec, options, null) as Uint8Array | T;
	}

	// -- batch ------------------------------------------------------------------

	batch(): BlobStoreBatch {
		if (this.#batchOpen) throw new Error("A batch is already open");
		if (this.#truncating) throw new Error("Can't start a batch while a truncate is in progress");
		this.#batchOpen = true;

		const region: FrozenRegion = { base: this.length(), stage: new Stage(this.#memChunkSize) };
		let live = true;
		const close = () => {
			live = false;
			this.#batchOpen = false;
		};

		return {
			append: (data: Uint8Array): number => {
				if (!live) throw new Error("Batch already settled");
				const pointer = region.base + region.stage.length;
				region.stage.append(data); // copies -- caller may reuse the buffer
				return pointer;
			},
			// deno-lint-ignore no-explicit-any
			get: async (
				pointer: number,
				lengthOrCodec: number | Codec<any, any>,
				options?: { readAheadSize?: number },
			): Promise<any> => {
				if (!live) throw new Error("Batch already settled");
				return this.#read(pointer, lengthOrCodec, options, region);
			},
			size: (): number => region.base + region.stage.length,
			apply: (): void => {
				if (!live) throw new Error("Batch already settled");
				// Batch began at this.length(), so it is contiguous with the end of staged.
				this.#staged.appendStage(region.stage);
				close();
			},
			discard: (): void => {
				if (!live) return;
				close();
			},
		};
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

		// Freeze staged in place; install a fresh empty staged in the same tick. frozen.base is the
		// current disk length (no frozen exists yet, so stagedBase == disk.length here).
		const frozen: FrozenRegion = { base: this.#disk.length, stage: this.#staged };
		this.#frozen = frozen;
		this.#staged = new Stage(this.#memChunkSize);

		// Stream [u64 base][u64 byteLen][bytes] to disk, fsync'd. Streaming avoids flattening the
		// whole frozen region into one buffer (can be 100+ MiB per batch at high heights).
		const header = new Uint8Array(16);
		const view = new Uint8ArrayView(header);
		view.setBigUint64(0, BigInt(frozen.base));
		view.setBigUint64(8, BigInt(frozen.stage.length));
		const file = Deno.openSync(this.#walPath, { create: true, write: true, truncate: true });
		try {
			await writeFile(file, header);
			for (const chunk of frozen.stage.chunks()) await writeFile(file, chunk);
			await file.sync(); // durability: the WAL must survive a crash for recovery to work
		} finally {
			file.close();
		}

		this.wal = this.#makeWal();
		return this.wal;
	}

	#makeWal(): WAL {
		const apply = async (): Promise<void> => {
			const buf = await Deno.readFile(this.#walPath);
			const view = new Uint8ArrayView(buf);
			const base = Number(view.getBigUint64(0));
			const byteLen = Number(view.getBigUint64(8));
			await this.#disk.truncateTo(base);
			await this.#disk.append(buf.subarray(16, 16 + byteLen));
			await this.#disk.sync(); // barrier: applied bytes must be durable before the WAL is discarded
		};

		const discard = async (): Promise<void> => {
			// Cleanup only -- apply() already wrote the data to disk. Truncating here would undo it.
			this.#frozen = null;
			this.wal = null;
			await Deno.remove(this.#walPath).catch(() => {});
		};

		return { apply, discard };
	}

	// -- truncate (reorg) -------------------------------------------------------

	async truncate(newLength: number): Promise<void> {
		if (this.#batchOpen) throw new Error("Can't truncate while a batch is open");
		if (this.#frozen || this.wal) throw new Error("Can't truncate while a flush is in progress");
		if (this.#truncating) throw new Error("A truncate is already in progress");
		if (newLength < 0) throw new Error("newLength must be non-negative");
		if (this.#staged.length > 0) throw new Error("Can't truncate while staged data is present; flush first");
		if (newLength > this.#disk.length) {
			throw new Error(
				`newLength (${newLength}) exceeds flushed length (${this.#disk.length}); flush before truncating into staged data`,
			);
		}

		this.#truncating = true;
		try {
			this.#staged = new Stage(this.#memChunkSize); // staged was empty; reset so its base re-derives
			if (newLength < this.#disk.length) {
				await this.#writeTruncateTarget(newLength);
				await this.#disk.truncateTo(newLength);
				await Deno.remove(this.#truncatePath).catch(() => {});
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

	// -- lifecycle --------------------------------------------------------------

	close(): void {
		this.#disk.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
