import { type Codec, type FixedCodec, VarInt } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import type { Batch, Store, WAL } from "~/storage/Store.ts";
import { readFile, writeFile } from "~/utils/fs.ts";
import { Uint8ArrayView } from "~/utils/Uint8ArrayView.ts";

/**
 * Fixed-stride array on disk with random-access, same-length, in-place updates.
 *
 * IndexStore is ArrayStore plus `set(index, value)`. It is the "mutable index" shape:
 * a dense ordinal slot space (number → fixed bytes) where the index *is* the key. Unlike
 * a keyed store, lookups are pure arithmetic (index * stride); unlike BlobStore it never
 * holds variable-length records, so a `set` never shifts anything — pointers stay stable.
 *
 * The whole reason this exists separately from ArrayStore: ArrayStore's correctness rests
 * on "no overwrite, clean range partition, no overlay precedence". That guarantee is worth
 * keeping ironclad for genuinely append-only data (blocks, headers). IndexStore is the place
 * that *pays* for mutability, so ArrayStore doesn't have to.
 *
 * Use case (BitcoinAtlas): one slot per output ever created, holding its `spentBy` pointer
 * (SENTINEL = unspent). The txid index stores only the base slot of a tx's first output;
 * output (txid, vout) lives at base + vout by arithmetic. Marking-spent is a single
 * same-length `set`. This keeps output blobs immutable (append-only BlobStore) and the txid
 * value fixed-stride (KVStore needs no change) — the mutation lives only here.
 *
 * Layering (newest wins on read):
 *
 *   batch (pending push + set)  →  staged (committed, not flushed)  →  frozen (mid-flush)  →  disk
 *
 * Each in-memory layer carries two things:
 *   - appends: items beyond the layer's base length (index >= base), in order.
 *   - overwrites: a Map<index, bytes> for indices that live BELOW base (on disk or in frozen).
 *     A `set` to an appended slot mutates that append in place, so the overwrites map only
 *     ever holds index < base. This split is what keeps the WAL replay self-healing: every
 *     overwrite target is < base, so truncate-to-base never removes a slot an overwrite needs.
 *
 * Read precedence for index i: staged.overwrites > staged.appends (i >= base) >
 * frozen.overwrites > frozen.appends (i >= frozen.base) > disk. A same-length overwrite that
 * has been flushed is just bytes rewritten on disk, so once clean the read is a plain
 * partition again — the overlay cost is paid only during the staged/frozen window.
 *
 * Non-blocking flush (same shape as ArrayStore): createWAL() freezes the staged layer into an
 * immutable `frozen` and installs a fresh empty staged in the same tick. `frozen` serves reads
 * of the in-flight range until discard(); reads cap disk at `frozen.base`, so apply()'s disk
 * rewrite is never observed as a torn read.
 *
 * Truncate means "go back in time" (reorg tail removal): it discards ALL staged data and
 * shrinks disk to `newLength`, relative to the flushed (disk) length. Resetting spent slots
 * below the truncation point (outputs created earlier but spent in orphaned blocks) is done
 * with explicit `set(slot, SENTINEL)` calls — flush those first, then truncate the tail.
 * truncate(0) subsumes a "clear".
 *
 * Consistency note: reads are snapshot-consistent w.r.t. flush/truncate. A single writer is
 * assumed (one batch at a time, enforced), matching the IBD sync loop.
 *
 * Note: IndexStore does NOT enforce write-once semantics — it is a generic mutable array. If
 * a slot must only transition SENTINEL → value (and only back to SENTINEL on reorg), assert
 * that at the call site; it will catch indexer bugs before they corrupt the spent-index.
 */
export type IndexStoreOptions<T extends FixedCodec> = {
	path: string;
	codec: T;
	/** Codec for the on-disk WAL counters. Defaults to VarInt. */
	counter?: Codec<number>;
};

export interface IndexStoreBatch<T extends FixedCodec> extends Batch {
	get(index: number): Promise<Codec.InferOutput<T>>;
	push(value: Codec.InferInput<T>): number;
	/** Same-length in-place update at an existing index (< current length). Does not extend. */
	set(index: number, value: Codec.InferInput<T>): void;
	length(): number;
}

/** An in-memory layer: ordered appends beyond `base`, plus in-place overwrites below `base`. */
type Layer = { appends: Uint8Array[]; overwrites: Map<number, Uint8Array> };

/** A frozen layer being flushed. `base` is the disk length (in items) it sits on top of. */
type Frozen = { base: number; appends: Uint8Array[]; overwrites: Map<number, Uint8Array> };

function emptyLayer(): Layer {
	return { appends: [], overwrites: new Map() };
}

/** Snapshot of mutable read state, captured synchronously so reads stay consistent across an await. */
type ReadSnapshot = {
	frozen: Frozen | null;
	staged: Layer;
	diskLength: number;
};

const APPEND_WRITE_CHUNK_BYTES = 1 << 20; // 1 MiB — keeps apply() yielding instead of one giant write

export class IndexStore<T extends FixedCodec> implements Store<IndexStoreBatch<T>>, Disposable {
	readonly #file: Deno.FsFile;
	readonly #codec: T;
	readonly #counter: Codec<number>;
	readonly #walPath: string;
	readonly #truncatePath: string;

	/** Items physically on disk in data.bin. Authoritative file state. */
	#diskLength: number;
	/** Committed-but-unflushed appends + overwrites. New batches merge in here. */
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

	static async open<T extends FixedCodec>(options: IndexStoreOptions<T>): Promise<IndexStore<T>> {
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

		const store = new IndexStore(file, codec, counter, walPath, truncatePath, size / codec.stride.size);

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

	/** Resolve `index` from the in-memory layers (incl. overwrites), or null if it lives on disk. */
	#pick(snap: ReadSnapshot, index: number): Uint8Array | null {
		const base = this.#baseLength(snap);
		// Above base → a staged append (overwrites map never holds index >= base).
		if (index >= base) return snap.staged.appends[index - base] ?? null;
		// Below base → newest staged overwrite wins over frozen / disk.
		const sow = snap.staged.overwrites.get(index);
		if (sow !== undefined) return sow;
		const f = snap.frozen;
		if (f) {
			if (index >= f.base) return f.appends[index - f.base] ?? null;
			const fow = f.overwrites.get(index);
			if (fow !== undefined) return fow;
		}
		return null; // on disk
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

		// Bulk-read the disk range once. Indices that an in-memory layer overrides (appends or
		// overwrites) are filled from #pick below; their bulk bytes are simply ignored.
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

	batch(): IndexStoreBatch<T> {
		this.#assertOpen();
		if (this.#batchOpen) throw new Error("A batch is already open");
		if (this.#truncating) throw new Error("Can't start a batch while a truncate is in progress");
		// A flush can't start while a batch is open (createWAL guards on this), so the store's
		// base length and staged layer are stable for the batch's whole lifetime.
		this.#batchOpen = true;

		const batchBaseLength = this.#total(this.#snapshot());
		const batchAppends: Uint8Array[] = [];
		// Pending in-place sets to indices below batchBaseLength (i.e. not this batch's appends).
		const batchOverwrites = new Map<number, Uint8Array>();
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
				// This batch's own pending overwrite wins, else fall through to committed store state.
				const ow = batchOverwrites.get(index);
				if (ow !== undefined) return this.#codec.decode(ow)[0];
				return this.get(index);
			},
			push: (value: Codec.InferInput<T>): number => {
				if (!live) throw new Error("Batch already settled");
				const index = batchBaseLength + batchAppends.length;
				batchAppends.push(this.#codec.encode(value));
				return index;
			},
			set: (index: number, value: Codec.InferInput<T>): void => {
				if (!live) throw new Error("Batch already settled");
				if (index < 0) throw new Error("Index must be non-negative");
				if (index >= batchBaseLength + batchAppends.length) {
					throw new Error("Index out of bounds (set does not extend; use push)");
				}
				const bytes = this.#codec.encode(value);
				if (index >= batchBaseLength) {
					batchAppends[index - batchBaseLength] = bytes; // overwrite own append in place
					return;
				}
				batchOverwrites.set(index, bytes);
			},
			length: (): number => batchBaseLength + batchAppends.length,
			apply: (): void => {
				if (!live) throw new Error("Batch already settled");
				for (const value of batchAppends) this.#staged.appends.push(value);
				// Route overwrites: those landing in the (pre-existing) staged-append region mutate
				// that append in place; the rest (disk / frozen indices) go to staged.overwrites.
				// base is stable for the batch's lifetime, so index - base addresses the same append
				// that existed at batch open (this batch's pushes were appended after it).
				const base = this.#baseLength(this.#snapshot());
				for (const [index, bytes] of batchOverwrites) {
					if (index >= base) this.#staged.appends[index - base] = bytes;
					else this.#staged.overwrites.set(index, bytes);
				}
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
		if (this.#staged.appends.length > 0 || this.#staged.overwrites.size > 0) {
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
	 * WAL format: [base][ow_count]([ow_index][ow_value] * ow_count)[append_count][value * append_count]
	 * apply() truncates disk to base, replays overwrites (all index < base, so they survive the
	 * truncate), then replays appends — idempotent and self-healing (every write rewrites the same
	 * bytes at the same offset).
	 */
	async createWAL(): Promise<WAL> {
		this.#assertOpen();
		if (this.#batchOpen) throw new Error("Can't start a flush while a batch is open");
		if (this.#truncating) throw new Error("Can't start a flush while a truncate is in progress");
		if (this.#frozen || this.wal) throw new Error("A flush is already in progress");

		// When clean, base === diskLength, so every staged overwrite targets a disk index — the
		// frozen layer's overwrites are therefore all < frozen.base, preserving the WAL invariant.
		const frozen: Frozen = {
			base: this.#diskLength,
			appends: this.#staged.appends,
			overwrites: this.#staged.overwrites,
		};
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

		const owEntries = [...frozen.overwrites];
		const owIndexBytes = owEntries.map(([index]) => this.#counter.encode(index));
		const owCountBytes = this.#counter.encode(owEntries.length);

		const appendCountBytes = this.#counter.encode(frozen.appends.length);

		let total = baseBytes.length + owCountBytes.length + appendCountBytes.length;
		for (const idxBytes of owIndexBytes) total += idxBytes.length + stride;
		total += frozen.appends.length * stride;

		const buf = new Uint8Array(total);
		let pos = 0;
		buf.set(baseBytes, pos);
		pos += baseBytes.length;

		buf.set(owCountBytes, pos);
		pos += owCountBytes.length;
		for (let i = 0; i < owEntries.length; i++) {
			buf.set(owIndexBytes[i]!, pos);
			pos += owIndexBytes[i]!.length;
			buf.set(owEntries[i]![1], pos);
			pos += stride;
		}

		buf.set(appendCountBytes, pos);
		pos += appendCountBytes.length;
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

			// Overwrites: scattered single-stride writes, all at index < base (within the truncated region)
			// Rewriting the same bytes is idempotent, so a crash mid-replay self-heals.
			const [owCount, owCountLen] = this.#counter.decode(buf.subarray(pos));
			pos += owCountLen;
			for (let i = 0; i < owCount; i++) {
				const [index, idxLen] = this.#counter.decode(buf.subarray(pos));
				pos += idxLen;
				const value = buf.subarray(pos, pos + stride);
				pos += stride;
				const at = index * stride;
				await this.#enqueue(async () => {
					await this.#file.seek(at, Deno.SeekMode.Start);
					await writeFile(this.#file, value);
				});
			}

			// Appends at [base, base + count), written in 1 MiB chunks so apply() keeps yielding.
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
