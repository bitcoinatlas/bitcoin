import { join } from "@std/path";
import { exists } from "@std/fs";
import { Codec } from "@nomadshiba/codec";
import { readFile, writeFile } from "~/utils/fs.ts";
import { Uint8ArrayView } from "~/utils/Uint8ArrayView.ts";
import type { Batch, Store, WAL } from "~/storage/Store.ts";
import { MAX_BLOCK_SIZE } from "~/constants.ts";

/**
 * Append-only store for variable-size blobs over a single logical byte stream.
 *
 * The stream is laid out in four contiguous regions, low offset to high:
 *
 *   disk [0, diskEnd) -> frozen [base, ...) -> staged [stagedBase, ...) -> batch [batchBase, ...)
 *
 * `disk` is the durable tail in fixed-size chunk files. `staged` is committed-but-unflushed
 * bytes in memory. `frozen` is the snapshot of `staged` taken when a flush starts; it serves
 * reads of the in-flight range until the flush is discarded. `batch` is the optional
 * uncommitted region of the single open batch.
 *
 * Because the stream is append-only the regions never overlap, so a read of [p, p+len) is a
 * straight arithmetic partition across them -- no scan, no per-blob records. append() copies
 * bytes into the active region and returns the offset where they landed; that offset IS the
 * blob pointer.
 *
 * Key invariant -- stagedBase is DERIVED, never stored:
 *
 *   stagedBase = frozen ? frozen.base + frozen.region.size : disk.size
 *
 * While a flush is live, disk physically grows from frozen.base toward frozen.base+len, but
 * stagedBase stays pinned to frozen.base+len, so staged (and any batch opened during the flush)
 * never shifts. Reads cap disk at frozen.base and let `frozen` serve that range, so a
 * half-applied disk is never observed as a torn read. After discard, disk.size == frozen.base+len,
 * so stagedBase is unchanged -- the layout is seamless across the discard.
 *
 * WAL: createWAL() freezes `staged`, installs a fresh empty `staged`, and writes
 * [u64 base][u64 byteLen][bytes] to disk (fsync'd). apply() truncates disk back to `base` then
 * appends the bytes (fsync'd) -- idempotent and self-healing, so it can be replayed any number of
 * times after a crash. discard() drops `frozen` and deletes the WAL file; it does NOT truncate
 * (apply already wrote the data; truncating here would undo it on the happy path).
 *
 * createWAL / apply / discard are kept as three separate steps precisely so Atomic can drive
 * many stores together: create all, then apply all, then discard all, with a state file between
 * each phase. Collapsing them (as flush() does for the single-store case) breaks that.
 *
 * truncate() (reorg) is the odd one out: it requires no batch, no flush, and no staged data, and
 * shrinks disk directly. A target-length sentinel makes the shrink crash-safe.
 *
 * Single writer, single reader, one batch at a time (all enforced).
 */

// ---------------------------------------------------------------------------
// MemoryRegion: in-memory, append-only, chunked byte region. (unchanged)
// ---------------------------------------------------------------------------

type Region = {
	size: number;
	append(bytes: Uint8Array): Promise<void> | void;
	read(offset: number, length: number): Promise<Uint8Array> | Uint8Array;
};

type MemoryRegionOptions = {
	maxChunkSize: number;
};

class MemoryRegion implements Region {
	public readonly maxChunkSize: number;

	public size: number;
	public readonly chunks: Uint8Array<ArrayBuffer>[];

	constructor(options: MemoryRegionOptions) {
		this.maxChunkSize = options.maxChunkSize;
		this.size = 0;
		this.chunks = [];
	}

	append(bytes: Uint8Array): void {
		let tailChunk = this.chunks.at(-1);
		if (!tailChunk) {
			this.chunks.push(tailChunk = new Uint8Array(new ArrayBuffer(0, { maxByteLength: this.maxChunkSize })));
		}

		let offset = 0;
		while (offset < bytes.length) {
			let available = tailChunk.buffer.maxByteLength - tailChunk.length;
			if (available === 0) {
				this.chunks.push(tailChunk = new Uint8Array(new ArrayBuffer(0, { maxByteLength: this.maxChunkSize })));
				available = tailChunk.buffer.maxByteLength;
			}

			const left = bytes.length - offset;
			const give = Math.min(left, available);

			const start = tailChunk.length;
			tailChunk.buffer.resize(tailChunk.length + give);
			tailChunk.set(bytes.subarray(offset, offset + give), start);
			offset += give;
		}
		this.size += offset;
	}

	read(offset: number, length: number): Uint8Array {
		if (offset < 0 || length < 0) {
			throw new RangeError(`negative offset/length: offset=${offset} length=${length}`);
		}
		if (offset + length > this.size) {
			throw new RangeError(`read out of bounds: offset=${offset} length=${length} size=${this.size}`);
		}

		const output = new Uint8Array(length);

		let chunkIndex = Math.floor(offset / this.maxChunkSize);
		let chunkOffset = offset % this.maxChunkSize;
		let outputOffset = 0;
		while (outputOffset < output.length) {
			const chunk = this.chunks[chunkIndex];
			if (!chunk) {
				throw new Error(`Yo no chunk 🍣. index=${chunkIndex}`);
			}

			const start = chunkOffset;
			const want = output.length - outputOffset;
			const end = Math.min(start + want, chunk.length);

			output.set(chunk.subarray(start, end), outputOffset);
			const got = end - start;
			if (got === 0) {
				throw new Error("Bro WHAT?! got literal 0 bytes ;-; ~~");
			}
			outputOffset += got;
			chunkIndex++;
			chunkOffset = 0;
		}

		return output;
	}
}

// ---------------------------------------------------------------------------
// DiskRegion: the durable tail, split across fixed-size chunk files.
//   (user's version + sync() and truncateTo(), required by the WAL / reorg paths)
// ---------------------------------------------------------------------------

type DiskRegionOptions = {
	path: string;
	maxChunkSize: number;
};

class DiskRegion implements Region {
	public readonly path: string;
	public readonly maxChunkSize: number;

	public size: number;
	private _appender: Deno.FsFile | null = null;
	private _appenderIndex = 0;

	private constructor(options: DiskRegionOptions) {
		this.path = options.path;
		this.maxChunkSize = options.maxChunkSize;
		this.size = 0;

		this._readers = [];
		this._appendQueue = Promise.resolve();
		this._readQueue = Promise.resolve();
	}

	static async open(options: DiskRegionOptions): Promise<DiskRegion> {
		const self = new DiskRegion(options);
		await Deno.mkdir(self.path, { recursive: true });
		const files = Deno.readDir(self.path);

		self._appenderIndex = 0;
		const indexSet = new Set<number>([0]);
		for await (const file of files) {
			if (!file.isFile) continue;
			if (!file.name.startsWith("chunk_")) continue;
			const index = Number(file.name.slice("chunk_".length));
			if (!Number.isInteger(index)) continue;
			indexSet.add(index);
			if (index > self._appenderIndex) self._appenderIndex = index;
		}

		for (let index = 0; index < self._appenderIndex; index++) {
			const exist = indexSet.has(index);
			if (!exist) {
				throw new Error("bro your chunks are fucked, has some    gaps   and  stuff");
			}
			const chunkStat = await Deno.stat(join(self.path, `chunk_${index}`));
			if (chunkStat.size !== self.maxChunkSize) {
				throw new Error(`chunk ${index} has a weird size size=${chunkStat.size}`);
			}
		}
		let tailChunkSize = 0;
		try {
			const tailChunkStat = await Deno.stat(join(self.path, `chunk_${self._appenderIndex}`));
			if (tailChunkStat.size > self.maxChunkSize) {
				throw new Error(`your tail chunk is fat... size=${tailChunkStat.size}`);
			}
			tailChunkSize = tailChunkStat.size;
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
		}
		self.size = self._appenderIndex * self.maxChunkSize + tailChunkSize;

		return self;
	}

	private _readers: (Promise<Deno.FsFile> | undefined)[];
	private _getReader(chunkIndex: number) {
		const file = this._readers[chunkIndex];
		if (file) return file;
		const chunkName = `chunk_${chunkIndex}`;
		return this._readers[chunkIndex] = Deno.open(join(this.path, chunkName), { read: true });
	}

	// Serializes appends so overlapping callers can't interleave size mutations.
	private _appendQueue: Promise<void>;
	append(bytes: Uint8Array): Promise<void> {
		const run = this._appendQueue.then(() => this._append(bytes));
		// Keep the chain alive even if a write rejects, but don't swallow the error
		// for the caller.
		this._appendQueue = run.then(() => {}, () => {});
		return run;
	}

	private async _append(bytes: Uint8Array): Promise<void> {
		let offset = 0;
		let size = this.size;
		while (offset < bytes.length) {
			const tailChunkIndex = Math.floor(size / this.maxChunkSize);
			const tailChunkOffset = size % this.maxChunkSize;

			const available = this.maxChunkSize - tailChunkOffset;
			const left = bytes.length - offset;
			const give = Math.min(available, left);

			if (!this._appender || tailChunkIndex !== this._appenderIndex) {
				if (this._appender) this._appender.close();
				this._appender = await Deno.open(
					join(this.path, `chunk_${tailChunkIndex}`),
					{ create: true, append: true },
				);
				this._appenderIndex = tailChunkIndex;
			}
			await writeFile(this._appender, bytes.subarray(offset, offset + give));

			offset += give;
			size += give;
		}
		this.size = size;
	}

	// Serializes reads (and orders them against appends/truncate). A cached reader fd is shared
	// per chunk, so two concurrent reads would interleave their seek()+read() on the same handle
	// and tear each other's data; the queue makes each seek+read sequence atomic.
	private _readQueue: Promise<unknown>;
	read(offset: number, length: number): Promise<Uint8Array> {
		const run = this._readQueue.then(() => this._read(offset, length));
		this._readQueue = run.then(() => {}, () => {});
		return run;
	}

	private async _read(offset: number, length: number): Promise<Uint8Array> {
		if (offset < 0 || length < 0) {
			throw new RangeError(`negative offset/length: offset=${offset} length=${length}`);
		}
		if (offset + length > this.size) {
			throw new RangeError(
				`read out of bounds: offset=${offset} length=${length} size=${this.size}`,
			);
		}

		const output = new Uint8Array(length);

		let chunkIndex = Math.floor(offset / this.maxChunkSize);
		let chunkOffset = offset % this.maxChunkSize;
		let outputOffset = 0;

		while (outputOffset < output.length) {
			const need = output.length - outputOffset;
			const available = this.maxChunkSize - chunkOffset;
			const want = Math.min(need, available);

			const file = await this._getReader(chunkIndex);
			await file.seek(chunkOffset, Deno.SeekMode.Start);
			const data = await readFile(file, want);
			output.set(data, outputOffset);

			outputOffset += want;
			chunkIndex++;
			chunkOffset = 0;
		}

		return output;
	}

	/** fsync the open append handle. Durability barrier; call before a WAL may be discarded. */
	async sync(): Promise<void> {
		await this._appendQueue; // drain in-flight appends first
		if (this._appender) await this._appender.sync();
	}

	/** Shrink the stream to exactly `offset`. Drops cached handles since their positions / target are now stale. */
	async truncate(offset: number): Promise<void> {
		if (offset < 0) throw new RangeError(`negative offset: ${offset}`);
		if (offset > this.size) throw new RangeError(`truncateTo grows the region: offset=${offset} size=${this.size}`);
		const oldTailChunkIndex = this.size > 0 ? Math.floor((this.size - 1) / this.maxChunkSize) : -1;
		const newTailChunkIndex = Math.floor(offset / this.maxChunkSize);

		// Drain in-flight appends, then drop every cached handle.
		await this._appendQueue;

		if (this._appender) {
			this._appender.close();
			this._appender = null;
		}

		const removed = this._readers.splice(newTailChunkIndex);
		for (const reader of removed) {
			if (!reader) continue;
			try {
				(await reader).close();
			} catch {
				// already closed / open failed; nothing to do
			}
		}

		// Set size first so any racing reader caps at the new bound.
		this.size = offset;
		for (let index = oldTailChunkIndex; index >= newTailChunkIndex; index--) {
			const name = `chunk_${index}`;
			const chunkPath = join(this.path, name);
			const chunkStart = index * this.maxChunkSize;
			if (chunkStart >= offset) {
				await Deno.remove(chunkPath).catch(() => {}); // fully at/after offset -- gone
			} else if (chunkStart + this.maxChunkSize > offset) {
				await Deno.truncate(chunkPath, offset - chunkStart); // straddles -- new tail
			}
			// else: fully below offset -- leave it intact
		}

		this._appenderIndex = newTailChunkIndex;
	}

	async close(): Promise<void> {
		// Drain in-flight appends first.
		await this._appendQueue;

		if (this._appender) this._appender.close();

		const readers = this._readers;
		this._readers = [];
		for (const reader of readers) {
			if (!reader) continue;
			try {
				(await reader).close();
			} catch {
				// already closed / open failed; nothing to do
			}
		}
	}

	async [Symbol.asyncDispose](): Promise<void> {
		await this.close();
	}
}

// ---------------------------------------------------------------------------
// BlobStore: coordinates DiskRegion + the in-memory regions, owns the WAL protocol.
// ---------------------------------------------------------------------------

export interface BlobStoreBatch extends Batch {
	/** Stage a blob for append. Returns the logical offset where it begins (the blob pointer). */
	append(data: Uint8Array): number;
	get(pointer: number, length: number): Promise<Uint8Array>;
	get<T extends Codec<any>>(
		pointer: number,
		codec: T,
		options?: { readAheadSize?: number },
	): Promise<Codec.InferOutput<T>>;
	size(): number;
}

export type BlobStoreOptions = {
	path: string;
	/** Max size per on-disk chunk file in bytes. Default 1 GiB. */
	maxDiskChunkSize?: number;
	/** Size of each in-memory chunk in bytes. Default 8 MiB. */
	maxMemoryChunkSize?: number;
};

type FrozenRegion = { base: number; region: MemoryRegion };

export class BlobStore implements Store<BlobStoreBatch> {
	private readonly _disk: DiskRegion;
	private readonly _maxMemoryChunkSize: number;
	private readonly _walPath: string;
	private readonly _truncatePath: string;

	private _staged: MemoryRegion;
	private _frozen: FrozenRegion | null = null;

	private _batchOpen = false;
	private _truncating = false;

	wal: WAL | null = null;

	private constructor(disk: DiskRegion, maxMemoryChunkSize: number, walPath: string, truncatePath: string) {
		this._disk = disk;
		this._maxMemoryChunkSize = maxMemoryChunkSize;
		this._walPath = walPath;
		this._truncatePath = truncatePath;
		this._staged = new MemoryRegion({ maxChunkSize: maxMemoryChunkSize });
	}

	static async open(options: BlobStoreOptions): Promise<BlobStore> {
		const { path } = options;
		const maxDiskChunkSize = options.maxDiskChunkSize ?? 1 * 1024 * 1024 * 1024; // 1 GiB
		const maxMemoryChunkSize = options.maxMemoryChunkSize ?? 8 * 1024 * 1024; // 8 MiB
		const walPath = join(path, "data.wal");
		const truncatePath = join(path, "truncate.target");

		const disk = await DiskRegion.open({ path, maxChunkSize: maxDiskChunkSize });
		const store = new BlobStore(disk, maxMemoryChunkSize, walPath, truncatePath);

		// Crash-safe truncate recovery (idempotent), before WAL detection (mutually exclusive).
		if (await exists(truncatePath)) {
			const buf = await Deno.readFile(truncatePath);
			const target = Number(new Uint8ArrayView(buf).getBigUint64(0));
			await disk.truncate(target);
			await Deno.remove(truncatePath).catch(() => {});
		}

		// A WAL on disk means a flush was interrupted; expose it for recover() (driven by Atomic).
		if (await exists(walPath)) store.wal = store._makeWal();

		return store;
	}

	private _newMemoryRegion(): MemoryRegion {
		return new MemoryRegion({ maxChunkSize: this._maxMemoryChunkSize });
	}

	// -- layout ----------------------------------------------------------------

	/** Derived, never stored. See the class doc for why this is the load-bearing invariant. */
	private _stagedBase(): number {
		return this._frozen ? this._frozen.base + this._frozen.region.size : this._disk.size;
	}

	length(): number {
		return this._stagedBase() + this._staged.size;
	}

	// -- reads ------------------------------------------------------------------

	/** Walk the contiguous regions and copy [pointer, pointer+length) out. May be short only at the true end of stream. */
	private async _gather(pointer: number, length: number, batch: FrozenRegion | null): Promise<Uint8Array> {
		if (pointer < 0) throw new RangeError(`pointer must be non-negative: ${pointer}`);

		const diskEnd = this._frozen ? this._frozen.base : this._disk.size;
		const stagedBase = this._stagedBase();
		const batchBase = stagedBase + this._staged.size;

		type ReadRegion = {
			base: number;
			length: number;
			read: (local: number, want: number) => Uint8Array | Promise<Uint8Array>;
		};

		const regions: ReadRegion[] = [
			{ base: 0, length: diskEnd, read: (l, w) => this._disk.read(l, w) },
		];
		if (this._frozen) {
			const f = this._frozen;
			regions.push({ base: f.base, length: f.region.size, read: (l, w) => f.region.read(l, w) });
		}
		regions.push({ base: stagedBase, length: this._staged.size, read: (l, w) => this._staged.read(l, w) });
		if (batch) {
			regions.push({ base: batchBase, length: batch.region.size, read: (l, w) => batch.region.read(l, w) });
		}

		const out = new Uint8Array(length);
		let filled = 0;
		let pos = pointer;
		for (const region of regions) {
			if (filled >= length) break;
			const end = region.base + region.length;
			if (pos >= end) continue; // entirely before this region
			if (pos < region.base) break; // hole between regions -- only reachable via a bad pointer
			const local = pos - region.base;
			const want = Math.min(length - filled, region.length - local);
			const data = await region.read(local, want); // returns exactly `want` (in bounds by construction)
			out.set(data, filled);
			filled += data.length;
			pos += data.length;
		}
		return filled === length ? out : out.subarray(0, filled);
	}

	private async _read(
		pointer: number,
		lengthOrCodec: number | Codec<any>,
		options: { readAheadSize?: number } | undefined,
		batch: FrozenRegion | null,
	): Promise<unknown> {
		if (typeof lengthOrCodec === "number") {
			const buf = await this._gather(pointer, lengthOrCodec, batch);
			if (buf.length !== lengthOrCodec) throw new Error("Unexpected EOF reading blob");
			return buf;
		}
		const codec = lengthOrCodec;
		const explicit = options?.readAheadSize;
		const length = codec.stride.kind === "fixed" && explicit === undefined
			? codec.stride.size
			: (explicit ?? MAX_BLOCK_SIZE);
		const slice = await this._gather(pointer, length, batch); // may be short for variable stride -- that's fine
		return codec.decode(slice)[0];
	}

	get(pointer: number, length: number): Promise<Uint8Array>;
	get<T extends Codec<any>>(
		pointer: number,
		codec: T,
		options?: { readAheadSize?: number },
	): Promise<Codec.InferOutput<T>>;
	async get(
		pointer: number,
		lengthOrCodec: number | Codec<any>,
		options?: { readAheadSize?: number },
	): Promise<Uint8Array | unknown> {
		return this._read(pointer, lengthOrCodec, options, null);
	}

	// -- batch ------------------------------------------------------------------

	batch(): BlobStoreBatch {
		if (this._batchOpen) throw new Error("A batch is already open");
		if (this._truncating) throw new Error("Can't start a batch while a truncate is in progress");
		this._batchOpen = true;

		// base == this.length(), so the batch is contiguous with the end of staged.
		const region: FrozenRegion = { base: this.length(), region: this._newMemoryRegion() };
		let live = true;
		const close = () => {
			live = false;
			this._batchOpen = false;
		};

		return {
			append: (data: Uint8Array): number => {
				if (!live) throw new Error("Batch already settled");
				const pointer = region.base + region.region.size;
				region.region.append(data); // copies -- caller may reuse the buffer
				return pointer;
			},
			get: ((
				pointer: number,
				lengthOrCodec: number | Codec<any>,
				options?: { readAheadSize?: number },
			): Promise<any> => {
				if (!live) return Promise.reject(new Error("Batch already settled"));
				return this._read(pointer, lengthOrCodec, options, region);
			}) as BlobStoreBatch["get"],
			size: (): number => region.base + region.region.size,
			apply: (): void => {
				if (!live) throw new Error("Batch already settled");
				for (const chunk of region.region.chunks) this._staged.append(chunk);
				close(); // batch ended -- region dropped, next batch() gets a fresh one
			},
			discard: (): void => {
				if (!live) return;
				close(); // batch ended -- region dropped
			},
		};
	}

	// -- flush / WAL ------------------------------------------------------------

	/** Single-store convenience: create + apply + discard in one go. Atomic does these separately. */
	async flush(): Promise<void> {
		const wal = await this.createWAL();
		await wal.apply();
		await wal.discard();
	}

	async createWAL(): Promise<WAL> {
		if (this._batchOpen) throw new Error("Can't start a flush while a batch is open");
		if (this._truncating) throw new Error("Can't start a flush while a truncate is in progress");
		if (this._frozen || this.wal) throw new Error("A flush is already in progress");

		// Freeze staged in place; install a fresh empty staged in the same tick. frozen.base is the
		// current disk size (no frozen exists yet, so stagedBase == disk.size here).
		const frozen: FrozenRegion = { base: this._disk.size, region: this._staged };
		this._frozen = frozen;
		this._staged = this._newMemoryRegion();

		// Stream [u64 base][u64 byteLen][bytes] to disk, fsync'd. Streaming avoids flattening the
		// whole frozen region into one buffer (can be 100+ MiB per batch at high heights).
		const header = new Uint8Array(16);
		const view = new Uint8ArrayView(header);
		view.setBigUint64(0, BigInt(frozen.base));
		view.setBigUint64(8, BigInt(frozen.region.size));
		const file = Deno.openSync(this._walPath, { create: true, write: true, truncate: true });
		try {
			await writeFile(file, header);
			for (const chunk of frozen.region.chunks) await writeFile(file, chunk);
			await file.sync(); // durability: the WAL must survive a crash for recovery to work
		} finally {
			file.close();
		}

		this.wal = this._makeWal();
		return this.wal;
	}

	private _makeWal(): WAL {
		const apply = async (): Promise<void> => {
			// Self-contained: reads everything from the WAL file, so it works after a crash even
			// though the in-memory `frozen` is gone. Idempotent -- truncate back then re-append.
			const buf = await Deno.readFile(this._walPath);
			const view = new Uint8ArrayView(buf);
			const base = Number(view.getBigUint64(0));
			const byteLen = Number(view.getBigUint64(8));
			await this._disk.truncate(base);
			await this._disk.append(buf.subarray(16, 16 + byteLen));
			await this._disk.sync(); // barrier: applied bytes must be durable before the WAL is discarded
		};

		const discard = async (): Promise<void> => {
			// Cleanup only -- apply() already wrote the data to disk. Truncating here would undo it.
			this._frozen = null;
			this.wal = null;
			await Deno.remove(this._walPath).catch(() => {});
		};

		return { apply, discard };
	}

	// -- truncate (reorg) -------------------------------------------------------

	async truncate(newLength: number): Promise<void> {
		if (this._batchOpen) throw new Error("Can't truncate while a batch is open");
		if (this._frozen || this.wal) throw new Error("Can't truncate while a flush is in progress");
		if (this._truncating) throw new Error("A truncate is already in progress");
		if (newLength < 0) throw new Error("newLength must be non-negative");
		if (this._staged.size > 0) throw new Error("Can't truncate while staged data is present; flush first");
		if (newLength > this._disk.size) {
			throw new Error(
				`newLength (${newLength}) exceeds flushed length (${this._disk.size}); flush before truncating into staged data`,
			);
		}

		this._truncating = true;
		try {
			this._staged = this._newMemoryRegion(); // staged was empty; reset so its base re-derives
			if (newLength < this._disk.size) {
				await this._writeTruncateTarget(newLength);
				await this._disk.truncate(newLength);
				await Deno.remove(this._truncatePath).catch(() => {});
			}
		} finally {
			this._truncating = false;
		}
	}

	private async _writeTruncateTarget(n: number): Promise<void> {
		const buf = new Uint8Array(8);
		new Uint8ArrayView(buf).setBigUint64(0, BigInt(n));
		const file = await Deno.open(this._truncatePath, { create: true, write: true, truncate: true });
		try {
			await writeFile(file, buf);
			await file.sync();
		} finally {
			file.close();
		}
	}

	// -- lifecycle --------------------------------------------------------------

	async close(): Promise<void> {
		await this._disk.close();
	}

	async [Symbol.asyncDispose](): Promise<void> {
		await this.close();
	}
}
