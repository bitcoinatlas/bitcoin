import { type Codec, type FixedCodec, VarInt } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import type { Batch, Store, WAL } from "~/lib/storage/Store.ts";
import { readFile, writeFile } from "~/lib/utils/fs.ts";
import { Uint8ArrayView } from "~/lib/Uint8ArrayView.ts";

/**
 * Append-only array on disk, stride-fixed entries, with:
 *
 *   - batches: the only way to mutate. Sync, in-memory, all-or-nothing. Append-only.
 *   - non-blocking flush: a flush freezes the staged layer and hands new batches a
 *     fresh layer in the same tick, so writes never wait on disk I/O.
 *   - replayable flush: apply() truncates to the WAL's recorded base length then
 *     replays. Idempotent — safe to run repeatedly, self-heals a torn data.bin.
 *   - crash-safe truncate: a target-length sentinel is written before the physical
 *     shrink and replayed on open(), so an interrupted truncate completes on restart.
 *
 * Layering (newest wins on read):
 *
 *   batch layer  →  staged (committed batches, not flushed)  →  frozen (mid-flush)  →  disk
 *
 * There is no in-place update — entries are only appended — so reads are a clean range
 * partition with no overlay precedence.
 *
 * Why flush is non-blocking: createWAL() *replaces* the staged object with a fresh empty
 * one rather than mutating it. The old layer becomes `frozen` and is never touched again
 * (apply() reads the WAL file, not frozen). `frozen` serves reads of the in-flight range
 * from memory until discard(), and reads cap disk at `frozen.base`, so apply()'s disk
 * rewrite is never observed as a torn read.
 *
 * Truncate means "go back in time" (reorg): it discards ALL staged data and shrinks the
 * disk to `newLength`. Because staged is discarded, `newLength` is relative to the flushed
 * (disk) length — truncating into not-yet-flushed data throws; flush first if you need to.
 *
 * Consistency note: reads are snapshot-consistent with respect to flush/truncate. A single
 * writer is assumed (one batch at a time, enforced) — reads are not expected to race a
 * concurrent commit, which matches the IBD sync loop.
 */
export type ArrayStoreOptions<T extends FixedCodec> = {
	path: string;
	codec: T;
	/** Codec for the on-disk WAL counters. Defaults to VarInt. */
	counter?: Codec<number>;
};

export interface ArrayStoreBatch<T extends FixedCodec> extends Batch {
	get(index: number): Promise<Codec.InferOutput<T>>;
	push(value: Codec.InferInput<T>): number;
	length(): number;
}

/** An in-memory layer of pending appends, in order. */
type Layer = { appends: Uint8Array[] };

/** A frozen layer being flushed. `base` is the disk length (in items) it sits on top of. */
type Frozen = { base: number; appends: Uint8Array[] };

function emptyLayer(): Layer {
	return { appends: [] };
}

/** Snapshot of mutable read state, captured synchronously so reads stay consistent across an await. */
type ReadSnapshot = {
	frozen: Frozen | null;
	staged: Layer;
	diskLength: number;
};

const APPEND_WRITE_CHUNK_BYTES = 1 << 20; // 1 MiB — keeps apply() yielding instead of one giant write

export class ArrayStore<T extends FixedCodec> implements Store<ArrayStoreBatch<T>>, Disposable {
	readonly #file: Deno.FsFile;
	readonly #codec: T;
	readonly #counter: Codec<number>;
	readonly #walPath: string;
	readonly #truncatePath: string;

	/** Items physically on disk in data.bin. Authoritative file state. */
	#diskLength: number;
	/** Committed-but-unflushed appends. New batches merge in here. */
	#staged: Layer = emptyLayer();
	/** Set during a flush; serves reads of the in-flight range until discard(). null when clean. */
	#frozen: Frozen | null = null;

	/** Serializes raw seek+read / seek+write on the shared fd. Does NOT gate batches or flushes. */
	#io: Promise<unknown> = Promise.resolve();
	#batchOpen = false;
	#truncating = false;
	#closed = false;

	/** The current on-disk WAL, if any. Set by createWAL(), or by open() during recovery. */
	wal: WAL | null = null;

	private constructor(
		file: Deno.FsFile,
		codec: T,
		counter: Codec<number>,
		walPath: string,
		truncatePath: string,
		diskLength: number,
	) {
		this.#file = file;
		this.#codec = codec;
		this.#counter = counter;
		this.#walPath = walPath;
		this.#truncatePath = truncatePath;
		this.#diskLength = diskLength;
	}

	static async open<T extends FixedCodec>(options: ArrayStoreOptions<T>): Promise<ArrayStore<T>> {
		const { path, codec } = options;
		const counter = options.counter ?? VarInt;
		const binPath = join(path, "data.bin");
		const walPath = join(path, "data.wal");
		const truncatePath = join(path, "truncate.target");

		await Deno.mkdir(path, { recursive: true });
		const file = await Deno.open(binPath, { read: true, write: true, create: true });

		const size = (await file.stat()).size;
		if (size % codec.stride.size !== 0) {
			file.close();
			throw new Error("File size must be a multiple of codec stride");
		}

		const store = new ArrayStore(file, codec, counter, walPath, truncatePath, size / codec.stride.size);

		// Crash-safe truncate recovery: an interrupted truncate left a target-length sentinel.
		// Re-applying the shrink is idempotent. Done before WAL detection — the two are mutually
		// exclusive operations, so at most one sentinel/WAL should exist.
		if (await exists(truncatePath)) {
			const buf = await Deno.readFile(truncatePath);
			const target = Number(new Uint8ArrayView(buf).getBigUint64(0));
			const targetBytes = target * codec.stride.size;
			if (targetBytes < size) await file.truncate(targetBytes);
			store.#diskLength = target;
			await Deno.remove(truncatePath).catch(() => {});
		}

		// A WAL on disk means a flush was interrupted. Expose it so recover() can replay,
		// but reads are NOT valid until recover() runs (disk may be torn).
		if (await exists(walPath)) {
			store.wal = store.#makeWal();
		}

		return store;
	}

	#snapshot(): ReadSnapshot {
		return { frozen: this.#frozen, staged: this.#staged, diskLength: this.#diskLength };
	}

	#baseLength(snap: ReadSnapshot): number {
		return snap.frozen ? snap.frozen.base + snap.frozen.appends.length : snap.diskLength;
	}

	#total(snap: ReadSnapshot): number {
		return this.#baseLength(snap) + snap.staged.appends.length;
	}

	/** Resolve `index` from the in-memory layers, or null if it lives on disk. */
	#pick(snap: ReadSnapshot, index: number): Uint8Array | null {
		const base = this.#baseLength(snap);
		if (index >= base) return snap.staged.appends[index - base] ?? null;
		const f = snap.frozen;
		if (f && index >= f.base) return f.appends[index - f.base] ?? null;
		return null;
	}

	#assertOpen(): void {
		if (this.#closed) throw new Error("Store is closed");
	}

	length(): number {
		return this.#total(this.#snapshot());
	}

	async get(index: number): Promise<Codec.InferOutput<T>> {
		this.#assertOpen();
		if (index < 0) throw new Error("Index must be non-negative");
		const snap = this.#snapshot();
		if (index >= this.#total(snap)) throw new Error("Index out of bounds");

		const mem = this.#pick(snap, index);
		if (mem) return this.#codec.decode(mem)[0];
		return this.#readDisk(index);
	}

	async slice(start: number, length: number): Promise<Codec.InferOutput<T>[]> {
		this.#assertOpen();
		if (start < 0) throw new Error("start must be non-negative");
		if (length < 0) throw new Error("length must be non-negative");

		const snap = this.#snapshot();
		const size = Math.min(length, this.#total(snap) - start);
		if (size <= 0) return [];

		const stride = this.#codec.stride.size;
		const diskBase = snap.frozen ? snap.frozen.base : snap.diskLength;
		const diskEnd = Math.min(start + size, diskBase);
		const diskCount = Math.max(0, diskEnd - start);

		let bulk: Uint8Array | null = null;
		if (diskCount > 0) {
			bulk = await this.#enqueue(async () => {
				await this.#file.seek(start * stride, Deno.SeekMode.Start);
				return await readFile(this.#file, diskCount * stride);
			});
		}

		const out = new Array<Codec.InferOutput<T>>(size);
		for (let i = 0; i < size; i++) {
			const index = start + i;
			const mem = this.#pick(snap, index);
			if (mem) {
				out[i] = this.#codec.decode(mem)[0];
			} else {
				const o = (index - start) * stride;
				out[i] = this.#codec.decode(bulk!.subarray(o, o + stride))[0];
			}
		}
		return out;
	}

	#readDisk(index: number): Promise<Codec.InferOutput<T>> {
		const stride = this.#codec.stride.size;
		return this.#enqueue(async () => {
			await this.#file.seek(index * stride, Deno.SeekMode.Start);
			const data = await readFile(this.#file, stride);
			return this.#codec.decode(data)[0];
		});
	}

	batch(): ArrayStoreBatch<T> {
		this.#assertOpen();
		if (this.#batchOpen) throw new Error("A batch is already open");
		if (this.#truncating) throw new Error("Can't start a batch while a truncate is in progress");
		// A flush can't start while a batch is open (createWAL guards on this), so the store's
		// base length and staged layer are stable for the batch's whole lifetime.
		this.#batchOpen = true;

		const batchBaseLength = this.#total(this.#snapshot());
		const batchAppends: Uint8Array[] = [];
		let live = true;

		const close = () => {
			live = false;
			this.#batchOpen = false;
		};

		return {
			get: async (index: number): Promise<Codec.InferOutput<T>> => {
				if (!live) throw new Error("Batch already settled");
				if (index < 0) throw new Error("Index must be non-negative");
				if (index >= batchBaseLength + batchAppends.length) throw new Error("Index out of bounds");
				if (index >= batchBaseLength) return this.#codec.decode(batchAppends[index - batchBaseLength]!)[0];
				return this.get(index);
			},
			push: (value: Codec.InferInput<T>): number => {
				if (!live) throw new Error("Batch already settled");
				const index = batchBaseLength + batchAppends.length;
				batchAppends.push(this.#codec.encode(value));
				return index;
			},
			length: (): number => batchBaseLength + batchAppends.length,
			apply: (): void => {
				if (!live) throw new Error("Batch already settled");
				for (const value of batchAppends) this.#staged.appends.push(value);
				close();
			},
			discard: (): void => {
				if (!live) return;
				close();
			},
		};
	}

	async truncate(newLength: number): Promise<void> {
		this.#assertOpen();
		if (this.#batchOpen) throw new Error("Can't truncate while a batch is open");
		if (this.#frozen || this.wal) throw new Error("Can't truncate while a flush is in progress");
		if (this.#truncating) throw new Error("A truncate is already in progress");
		if (newLength < 0) throw new Error("newLength must be non-negative");
		if (this.#staged.appends.length > 0) {
			throw new Error("Can't truncate while staged data is present; flush first");
		}
		if (newLength > this.#diskLength) {
			throw new Error(
				`newLength (${newLength}) exceeds flushed length (${this.#diskLength}); flush before truncating into staged data`,
			);
		}

		this.#truncating = true;
		try {
			// Reorg discards all staged (future) data.
			this.#staged = emptyLayer();

			if (newLength < this.#diskLength) {
				// Crash safety: record the target so an interrupted shrink completes on open().
				await this.#writeTruncateTarget(newLength);
				// Shrink the logical length first so a concurrent read caps at newLength,
				// then do the physical truncate, then drop the sentinel.
				this.#diskLength = newLength;
				await this.#file.truncate(newLength * this.#codec.stride.size);
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

	async flush(): Promise<void> {
		const wal = await this.createWAL();
		await wal.apply();
		await wal.discard();
	}

	/**
	 * WAL format: [base_length][append_count][value * append_count]
	 * apply() truncates disk to base_length then replays appends — idempotent and self-healing.
	 */
	async createWAL(): Promise<WAL> {
		this.#assertOpen();
		if (this.#batchOpen) throw new Error("Can't start a flush while a batch is open");
		if (this.#truncating) throw new Error("Can't start a flush while a truncate is in progress");
		if (this.#frozen || this.wal) throw new Error("A flush is already in progress");

		const frozen: Frozen = { base: this.#diskLength, appends: this.#staged.appends };
		this.#frozen = frozen;
		this.#staged = emptyLayer();

		const buf = this.#encodeWal(frozen);
		await Deno.writeFile(this.#walPath, buf, { create: true });

		this.wal = this.#makeWal();
		return this.wal;
	}

	#encodeWal(frozen: Frozen): Uint8Array {
		const stride = this.#codec.stride.size;
		const baseBytes = this.#counter.encode(frozen.base);
		const countBytes = this.#counter.encode(frozen.appends.length);

		const buf = new Uint8Array(baseBytes.length + countBytes.length + frozen.appends.length * stride);
		let pos = 0;
		buf.set(baseBytes, pos);
		pos += baseBytes.length;
		buf.set(countBytes, pos);
		pos += countBytes.length;
		for (const value of frozen.appends) {
			buf.set(value, pos);
			pos += stride;
		}
		return buf;
	}

	#makeWal(): WAL {
		const stride = this.#codec.stride.size;

		const apply = async (): Promise<void> => {
			const buf = await Deno.readFile(this.#walPath);
			let pos = 0;

			const [base, baseLen] = this.#counter.decode(buf.subarray(pos));
			pos += baseLen;
			await this.#file.truncate(base * stride);
			this.#diskLength = base;

			const [count, countLen] = this.#counter.decode(buf.subarray(pos));
			pos += countLen;
			if (count > 0) {
				const appendBytes = buf.subarray(pos, pos + count * stride);
				const chunkStride = Math.max(1, Math.floor(APPEND_WRITE_CHUNK_BYTES / stride)) * stride;
				const totalBytes = appendBytes.length;
				let written = 0;
				while (written < totalBytes) {
					const end = Math.min(written + chunkStride, totalBytes);
					const slice = appendBytes.subarray(written, end);
					const at = base * stride + written;
					await this.#enqueue(async () => {
						await this.#file.seek(at, Deno.SeekMode.Start);
						await writeFile(this.#file, slice);
					});
					written = end;
				}
				this.#diskLength = base + count;
			}
		};

		const discard = async (): Promise<void> => {
			this.#frozen = null;
			this.wal = null;
			await Deno.remove(this.#walPath).catch(() => {});
		};

		return { apply, discard };
	}

	close(): void {
		if (this.#closed) return;
		if (this.#batchOpen) throw new Error("Can't close while a batch is open");
		if (this.#truncating) throw new Error("Can't close while a truncate is in progress");
		if (this.wal) throw new Error("Can't close while a flush is in progress");
		this.#closed = true;
		this.#file.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}

	#enqueue<R>(fn: () => Promise<R>): Promise<R> {
		const run = this.#io.then(fn, fn) as Promise<R>;
		this.#io = run.then(() => {}, () => {});
		return run;
	}
}
