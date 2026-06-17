import { Codec, type FixedCodec } from "@nomadshiba/codec";
import { concat } from "@std/bytes";
import { join } from "@std/path";
import { Batch, Store } from "~/storage/Store.ts";
import { readFileInto, writeFile } from "~/utils/fs.ts";
import { Uint8ArrayView } from "~/utils/Uint8ArrayView.ts";

/**
 * Chunked, fixed-stride disk storage with in-place writes.
 *
 * Unlike BlobStore's DiskRegion this one is settable via {@link writeInto}.
 * `maxChunkSize` MUST be a multiple of the item stride (enforced by IndexStore),
 * so no single item ever straddles a chunk boundary — a set is always a
 * single-chunk positioned write.
 *
 * Reads and in-place writes open a fresh fd per call (closed via `using`), so
 * each operation owns its own kernel file offset and concurrent gets/writes
 * can never race on a shared seek position.
 */
class DiskRegion implements Disposable {
	public readonly path: string;
	public readonly maxChunkSize: number;

	public size: number;

	_chunkPathCache: string[] = [];
	private _chunkPath(index: number) {
		return this._chunkPathCache[index] ??= join(this.path, `chunk_${index}`);
	}

	private constructor(options: { path: string; maxChunkSize: number }) {
		this.path = options.path;
		this.maxChunkSize = options.maxChunkSize;
		this.size = 0;
	}

	static async open(options: { path: string; maxChunkSize: number }): Promise<DiskRegion> {
		const self = new DiskRegion(options);
		await Deno.mkdir(self.path, { recursive: true });
		const files = Deno.readDir(self.path);

		let tailIndex = 0;
		const indexSet = new Set<number>([0]);
		for await (const file of files) {
			if (!file.isFile) continue;
			if (!file.name.startsWith("chunk_")) continue;
			const index = Number(file.name.slice("chunk_".length));
			if (!Number.isInteger(index)) continue;
			indexSet.add(index);
			if (index > tailIndex) tailIndex = index;
		}

		for (let index = 0; index < tailIndex; index++) {
			if (!indexSet.has(index)) {
				throw new Error("bro your chunks are fucked, has some    gaps   and  stuff");
			}
			const chunkStat = await Deno.stat(self._chunkPath(index));
			if (chunkStat.size !== self.maxChunkSize) {
				throw new Error(`chunk ${index} has a weird size size=${chunkStat.size}`);
			}
		}

		let tailChunkSize = 0;
		try {
			const tailChunkStat = await Deno.stat(self._chunkPath(tailIndex));
			if (tailChunkStat.size > self.maxChunkSize) {
				throw new Error(`your tail chunk is fat... size=${tailChunkStat.size}`);
			}
			tailChunkSize = tailChunkStat.size;
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
		}

		self._appender = {
			index: tailIndex,
			file: await Deno.open(self._chunkPath(tailIndex), { create: true, append: true }),
		};
		self.size = tailIndex * self.maxChunkSize + tailChunkSize;

		return self;
	}

	private _appender!: { file: Deno.FsFile; index: number };
	private _appending = false;
	async append(bytes: Uint8Array): Promise<void> {
		if (this._appending) throw new Error("you are trying to append back to back, check your logic");
		if (this._truncating) throw new Error("you are trying to append while truncating, check your logic");
		this._appending = true;
		try {
			let size = this.size;
			let appended = 0;
			while (appended < bytes.length) {
				const want = bytes.length - appended;
				const index = Math.floor(size / this.maxChunkSize);
				const taken = size % this.maxChunkSize;
				const available = this.maxChunkSize - taken;
				const append = Math.min(want, available);

				if (this._appender.index !== index) {
					const file = await Deno.open(this._chunkPath(index), { create: true, append: true });
					this._appender.file.close();
					this._appender.file = file;
					this._appender.index = index;
				}

				await writeFile(this._appender.file, bytes.subarray(appended, appended + append));
				await this._appender.file.sync();
				appended += append;
				size += append;
			}
			this.size = size;
		} finally {
			this._appending = false;
		}
	}

	async readInto(offset: number, length: number, target: Uint8Array): Promise<void> {
		if (offset + length > this.size) {
			throw new Error(`read out of bounds offset=${offset} length=${length} size=${this.size}`);
		}

		let copied = 0;
		while (copied < length) {
			const want = length - copied;
			const index = Math.floor(offset / this.maxChunkSize);
			const start = offset % this.maxChunkSize;
			const available = this.maxChunkSize - start;
			const read = Math.min(want, available);

			using reader = await Deno.open(this._chunkPath(index), { read: true });
			await reader.seek(start, Deno.SeekMode.Start);
			await readFileInto(reader, target.subarray(copied, copied + read));

			copied += read;
			offset += read;
		}
	}

	/** In-place positioned write. Caller guarantees `offset + bytes.length <= size`. */
	async writeInto(offset: number, bytes: Uint8Array): Promise<void> {
		if (this._truncating) throw new Error("you cant writeInto while truncating");
		if (offset + bytes.length > this.size) {
			throw new Error(`write out of bounds offset=${offset} length=${bytes.length} size=${this.size}`);
		}

		let written = 0;
		while (written < bytes.length) {
			const want = bytes.length - written;
			const index = Math.floor(offset / this.maxChunkSize);
			const start = offset % this.maxChunkSize;
			const available = this.maxChunkSize - start;
			const write = Math.min(want, available);

			using writer = await Deno.open(this._chunkPath(index), { read: true, write: true });
			await writer.seek(start, Deno.SeekMode.Start);
			await writeFile(writer, bytes.subarray(written, written + write));
			await writer.sync();

			written += write;
			offset += write;
		}
	}

	/**
	 * Batched in-place writes. Items never straddle a chunk (stride divides
	 * maxChunkSize), so each lands in exactly one chunk. We group by chunk and do
	 * a single open + single fsync per touched chunk instead of per item — turning
	 * O(items) fsyncs into O(chunks-touched). Caller guarantees every offset is in
	 * bounds. `writes` is sorted by offset so writes within a chunk go forward.
	 */
	async writeManyInto(writes: Array<{ offset: number; bytes: Uint8Array }>): Promise<void> {
		if (this._truncating) throw new Error("you cant writeInto while truncating");
		if (writes.length === 0) return;
		writes.sort((a, b) => a.offset - b.offset);

		let i = 0;
		while (i < writes.length) {
			const chunkIndex = Math.floor(writes[i]!.offset / this.maxChunkSize);
			using writer = await Deno.open(this._chunkPath(chunkIndex), { read: true, write: true });
			while (i < writes.length && Math.floor(writes[i]!.offset / this.maxChunkSize) === chunkIndex) {
				const { offset, bytes } = writes[i]!;
				if (offset + bytes.length > this.size) {
					throw new Error(`write out of bounds offset=${offset} length=${bytes.length} size=${this.size}`);
				}
				await writer.seek(offset % this.maxChunkSize, Deno.SeekMode.Start);
				await writeFile(writer, bytes);
				i++;
			}
			await writer.sync(); // one fsync per chunk
		}
	}

	/** Batched reads, grouped by chunk (one open per touched chunk, no fsync). */
	async readManyInto(reads: Array<{ offset: number; into: Uint8Array }>): Promise<void> {
		if (reads.length === 0) return;
		reads.sort((a, b) => a.offset - b.offset);

		let i = 0;
		while (i < reads.length) {
			const chunkIndex = Math.floor(reads[i]!.offset / this.maxChunkSize);
			using reader = await Deno.open(this._chunkPath(chunkIndex), { read: true });
			while (i < reads.length && Math.floor(reads[i]!.offset / this.maxChunkSize) === chunkIndex) {
				const { offset, into } = reads[i]!;
				if (offset + into.length > this.size) {
					throw new Error(`read out of bounds offset=${offset} length=${into.length} size=${this.size}`);
				}
				await reader.seek(offset % this.maxChunkSize, Deno.SeekMode.Start);
				await readFileInto(reader, into);
				i++;
			}
		}
	}

	private _truncating = false;
	async truncate(size: number) {
		if (this._appending) throw new Error("you cant truncate while appending");
		if (this._truncating) throw new Error("you are trying to truncate back to back, check your logic");
		this._truncating = true;

		const oldTail = this._appender.index;
		const newTail = Math.floor(size / this.maxChunkSize);

		this._appender.file.close();
		this._appender.index = -1;

		for (let index = newTail + 1; index <= oldTail; index++) {
			await Deno.remove(this._chunkPath(index));
		}

		const tailEnd = size % this.maxChunkSize;
		await Deno.truncate(this._chunkPath(newTail), tailEnd);

		this._appender.file = await Deno.open(this._chunkPath(newTail), { create: true, append: true });
		this._appender.index = newTail;

		this.size = size;

		// Should never fail to this point.
		// If it did data is corrupted.
		// So that's why we are not doing try/catch.
		this._truncating = false;
	}

	close() {
		this._appender.file.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}

/**
 * A keyed staging layer.
 *
 * `entries` holds both staged sets (index < disk length) and staged appends
 * (index >= disk length). `length` is the logical item count as of this layer.
 */
type Overlay = {
	entries: Map<number, Uint8Array>;
	length: number;
};

export interface IndexStoreBatch<T> extends Batch {
	set(index: number, item: T): void;
	push(item: T): number;
	get(index: number): Promise<T>;
	length(): number;
}

export type IndexStoreOptions<T extends FixedCodec<any>> = {
	path: string;
	codec: T;
	itemsPerChunk: number;
};

/**
 * A fixed-stride, settable array store with WAL durability.
 *
 * Like ArrayStore you can `append` and `get`, but unlike it you can also `set`
 * an existing slot in place. Staging is a keyed overlay rather than a positional
 * region stack, and rollback uses an undo log (old value per overwritten slot +
 * the pre-flush length) instead of a single truncate point.
 *
 * Durability contract (Atomic-orchestrated): pin → flush → (rocks) → end.
 *
 * - `pin()`  — freeze the staged overlay and write the durable undo log derived
 *              from *that* snapshot, BEFORE any disk mutation. Must run before flush.
 * - `flush()` — apply the frozen snapshot (appends extend, sets write in place).
 * - `rollback()` — replay the undo log: restore overwritten slots, truncate appends.
 *
 * Why pin freezes (unlike BlobStore, where flush freezes): IndexStore's undo is
 * value-based, so the snapshot the undo log describes and the snapshot flush
 * writes MUST be identical. Freezing at pin time pins both to the same overlay.
 */
export class IndexStore<T extends FixedCodec<any>> extends Store<IndexStoreBatch<Codec.InferOutput<T>>> implements Disposable {
	public readonly path: string;
	public readonly itemsPerChunk: number;

	private readonly _codec: T;
	private readonly _stride: number;
	private readonly _walPath: string;

	private _disk!: DiskRegion;
	private _staged!: Overlay;
	private _frozen: Overlay | null = null;
	private _batch: Overlay | null = null;

	private _pinning = false;
	private _flushing = false;
	private _truncating = false;

	private constructor(options: IndexStoreOptions<T>) {
		super();
		this.path = options.path;
		this.itemsPerChunk = options.itemsPerChunk;
		this._codec = options.codec;
		this._stride = options.codec.stride.size;
		this._walPath = join(this.path, "rollback.wal");
	}

	static async open<T extends FixedCodec<any>>(options: IndexStoreOptions<T>): Promise<IndexStore<T>> {
		const self = new IndexStore(options);
		const maxChunkSize = self._stride * options.itemsPerChunk;
		if (maxChunkSize % self._stride !== 0) {
			throw new Error("chunk size must be a multiple of the item stride");
		}
		self._disk = await DiskRegion.open({ path: options.path, maxChunkSize });
		self._staged = { entries: new Map(), length: self._disk.size / self._stride };
		return self;
	}

	/** Logical item count (does not include an open batch's pending appends). */
	length(): number {
		return this._staged.length;
	}

	private async _read(index: number, extra?: Map<number, Uint8Array>): Promise<Codec.InferOutput<T>> {
		// Freshness order: batch overlay > staged > frozen > disk.
		// Any slot being written by an in-progress flush lives in `_frozen`, so a
		// read for it routes to the overlay and never sees a half-written disk slot.
		const staged = extra?.get(index) ?? this._staged.entries.get(index) ?? this._frozen?.entries.get(index);
		if (staged) {
			const [decoded] = this._codec.decode(staged);
			return decoded;
		}

		const offset = index * this._stride;
		if (index < 0 || offset + this._stride > this._disk.size) {
			throw new RangeError(`get out of bounds index=${index} length=${this.length()}`);
		}

		const buffer = new Uint8Array(this._stride);
		await this._disk.readInto(offset, this._stride, buffer);
		const [decoded] = this._codec.decode(buffer);
		return decoded;
	}

	get(index: number): Promise<Codec.InferOutput<T>> {
		return this._read(index);
	}

	batch(): IndexStoreBatch<Codec.InferOutput<T>> {
		if (this._truncating) throw new Error("nah you can't batch while truncating");
		if (this._batch) throw new Error("can't have concurrent batches man, can't track length correctly");

		const codec = this._codec;
		const overlay: Overlay = this._batch = { entries: new Map(), length: this._staged.length };

		const length: IndexStoreBatch<Codec.InferOutput<T>>["length"] = () => overlay.length;

		const set: IndexStoreBatch<Codec.InferOutput<T>>["set"] = (index, item) => {
			if (index < 0 || index >= overlay.length) {
				throw new RangeError(`set out of bounds index=${index} length=${overlay.length}`);
			}
			overlay.entries.set(index, codec.encode(item));
		};

		const push: IndexStoreBatch<Codec.InferOutput<T>>["push"] = (item) => {
			const index = overlay.length;
			overlay.entries.set(index, codec.encode(item));
			overlay.length = index + 1;
			return index;
		};

		const get: IndexStoreBatch<Codec.InferOutput<T>>["get"] = (index) => this._read(index, overlay.entries);

		const apply: IndexStoreBatch<Codec.InferOutput<T>>["apply"] = () => {
			for (const [index, bytes] of overlay.entries) this._staged.entries.set(index, bytes);
			this._staged.length = overlay.length;
			this._batch = null;
		};

		const discard: IndexStoreBatch<Codec.InferOutput<T>>["discard"] = () => {
			this._batch = null;
		};

		return { length, set, push, get, apply, discard };
	}

	/**
	 * Synchronously snapshot the staged overlay into `_frozen` and install a fresh
	 * staged layer. No await, no disk — see Store.freeze. Atomic calls this on every
	 * store in one synchronous burst so they all capture the same height; a tick's
	 * apply() can't interleave because this never yields. Idempotent while a flush
	 * is pending, so a standalone pin()/flush() may trigger it lazily.
	 */
	freeze(): void {
		if (this._frozen) return;
		if (this._truncating) throw new Error("can't freeze while truncating");
		this._frozen = this._staged;
		this._staged = { entries: new Map(), length: this._frozen.length };
	}

	/**
	 * Durably record the undo log for the frozen snapshot, before any disk
	 * mutation. The snapshot itself is taken by {@link freeze} (Atomic freezes all
	 * stores first); a standalone caller freezes lazily here. Must run before flush.
	 */
	async pin(): Promise<void> {
		if (this._pinning) throw new Error("already pinning");
		if (this._flushing) throw new Error("can't pin while a flush is in progress");
		if (this._truncating) throw new Error("can't pin while truncating");
		this._pinning = true;
		try {
			if (!this._frozen) this.freeze();
			const frozen = this._frozen!;

			const stride = this._stride;
			const diskLength = this._disk.size / stride;

			// Partition frozen entries into in-place overwrites vs new appends.
			const sets: number[] = [];
			const appends: number[] = [];
			for (const index of frozen.entries.keys()) {
				if (index < diskLength) sets.push(index);
				else appends.push(index);
			}
			sets.sort((a, b) => a - b);
			appends.sort((a, b) => a - b);

			for (let i = 0; i < appends.length; i++) {
				if (appends[i] !== diskLength + i) {
					throw new Error(`appends are not contiguous, expected ${diskLength + i} got ${appends[i]}`);
				}
			}

			// Undo log: pre-flush length + old (on-disk) value of every overwritten slot.
			const wal = new Uint8Array(8 + 8 + sets.length * (8 + stride));
			const view = new Uint8ArrayView(wal);
			view.setBigUint64(0, BigInt(diskLength), true);
			view.setBigUint64(8, BigInt(sets.length), true);

			// Lay out the index headers, and read every old value straight into its WAL
			// slot in one chunk-grouped pass (one fd per touched chunk) rather than a
			// separate open+seek+read per overwritten slot.
			const reads: Array<{ offset: number; into: Uint8Array }> = [];
			let cursor = 16;
			for (const index of sets) {
				view.setBigUint64(cursor, BigInt(index), true);
				cursor += 8;
				reads.push({ offset: index * stride, into: wal.subarray(cursor, cursor + stride) });
				cursor += stride;
			}
			await this._disk.readManyInto(reads);

			// Durable BEFORE any mutation. truncate:true so a stale longer WAL can't
			// leave trailing garbage; a fresh write every round makes stale logs moot.
			using walFile = await Deno.open(this._walPath, { create: true, write: true, truncate: true });
			await writeFile(walFile, wal);
			await walFile.sync();
		} finally {
			this._pinning = false;
		}
	}

	/** Apply the frozen snapshot to disk. Requires a prior {@link pin}. */
	async flush(): Promise<void> {
		if (this._flushing) throw new Error("wtf are you doin man, you are already flushing");
		if (this._pinning) throw new Error("can't flush while pinning");
		if (this._truncating) throw new Error("can't flush while truncating");
		const frozen = this._frozen;
		if (!frozen) throw new Error("call pin() before flush()");
		this._flushing = true;
		try {
			const stride = this._stride;
			const diskLength = this._disk.size / stride;

			const sets: number[] = [];
			const appends: number[] = [];
			for (const index of frozen.entries.keys()) {
				if (index < diskLength) sets.push(index);
				else appends.push(index);
			}
			appends.sort((a, b) => a - b);

			// Appends extend the region; sets overwrite existing slots. Disjoint ranges,
			// so order is irrelevant — and a crash mid-flush is fully undone by the WAL.
			if (appends.length > 0) {
				const buffer = concat(appends.map((index) => frozen.entries.get(index)!));
				await this._disk.append(buffer);
			}
			if (sets.length > 0) {
				await this._disk.writeManyInto(
					sets.map((index) => ({ offset: index * stride, bytes: frozen.entries.get(index)! })),
				);
			}

			this._frozen = null;
		} finally {
			this._flushing = false;
		}
	}

	/**
	 * Delete the rollback WAL. Call only after every store in the atomic group
	 * has flushed successfully — i.e. from {@link Atomic.flush} after all
	 * flushes and the RocksDB commit are done, just before writing end.id.
	 * Until then the WAL must stay so {@link rollback} can undo a partial flush.
	 */
	async finalize(): Promise<void> {
		await Deno.remove(this._walPath).catch(() => {});
	}

	async rollback(): Promise<void> {
		const stride = this._stride;

		let wal: Uint8Array;
		try {
			wal = await Deno.readFile(this._walPath);
		} catch (e) {
			if (e instanceof Deno.errors.NotFound) return; // nothing was ever flushed
			throw e;
		}

		const view = new Uint8ArrayView(wal);
		const oldLength = Number(view.getBigUint64(0, true));
		const setCount = Number(view.getBigUint64(8, true));

		// Restore overwrites first; their offsets are < oldLength so they survive the truncate.
		// Use writeManyInto: one fd open + one fsync per touched chunk instead of
		// one fd + one fsync per slot (which is what parallel writeInto() calls do).
		let cursor = 16;
		const writes: Array<{ offset: number; bytes: Uint8Array }> = [];
		for (let i = 0; i < setCount; i++) {
			const index = Number(view.getBigUint64(cursor, true));
			cursor += 8;
			const oldValue = wal.subarray(cursor, cursor + stride);
			cursor += stride;
			writes.push({ offset: index * stride, bytes: oldValue });
		}
		await this._disk.writeManyInto(writes);
		await this._disk.truncate(oldLength * stride);

		this._batch = null;
		this._frozen = null;
		this._staged = { entries: new Map(), length: this._disk.size / stride };

		await Deno.remove(this._walPath).catch(() => {});
	}

	async truncate(length: number): Promise<void> {
		if (this._truncating) throw new Error("A truncate is already in progress");
		if (this._batch) throw new Error("Can't truncate while a batch is open");
		if (this._frozen) throw new Error("Can't truncate while a flush is pending/in progress");
		if (this._staged.entries.size > 0) throw new Error("Can't truncate while staged data is present; flush first");

		this._truncating = true;
		try {
			await this._disk.truncate(length * this._stride);
			this._staged.length = length;
		} finally {
			this._truncating = false;
		}
	}

	close(): void {
		this._disk.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
