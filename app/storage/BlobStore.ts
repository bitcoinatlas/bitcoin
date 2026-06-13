import { Codec } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import type { Batch, Store, WAL } from "~/storage/Store.ts";
import { writeFile } from "~/utils/fs.ts";
import { Uint8ArrayView } from "~/utils/Uint8ArrayView.ts";

/**
 * Append-only store for variable-size blobs, split across fixed-size chunk files.
 *
 * A blob is addressed by a logical byte pointer (its offset in the virtual stream
 * formed by concatenating chunk_0, chunk_1, ...). pointer → (floor(p/chunk), p%chunk).
 * There is no overwrite — only append — so reads are a clean range partition.
 *
 * Layering (in pointer order):
 *
 *   disk [0, base)  →  frozen [base, base+frozenBytes)  →  staged [base+frozenBytes, end)
 *
 * Non-blocking flush (same shape as ArrayStore): createWAL() freezes the staged blobs
 * into an immutable `frozen` and installs a fresh empty staged in the same tick, so new
 * batches proceed while the flush runs. `frozen` serves reads of the in-flight range from
 * memory until discard(), and reads cap disk at `frozen.base` (NOT the live disk length),
 * so apply()'s incremental disk growth is never observed as a torn read.
 *
 * WAL format: [u64 base_offset LE][u32 count LE]([u32 blob_length LE][bytes])...
 * apply() truncates disk back to base_offset then replays — idempotent and self-healing.
 *
 * Truncate means "go back in time" (reorg): it discards ALL staged data and shrinks the
 * disk to `newLength`. Because staged is discarded, `newLength` is relative to the flushed
 * (disk) length — truncating into not-yet-flushed data throws; flush first if you need to.
 * A target-length sentinel makes the shrink crash-safe: it is replayed on open().
 *
 * Consistency note: reads are snapshot-consistent with respect to flush/truncate. A single
 * writer is assumed (one batch at a time, enforced) — reads are not expected to race a
 * concurrent commit, which matches the IBD sync loop.
 */
export interface BlobStoreBatch extends Batch {
	/** Stage a blob for append. Returns its (tentative) pointer. */
	append(data: Uint8Array): number;
	get(pointer: number, length: number): Promise<Uint8Array>;
	// deno-lint-ignore no-explicit-any
	get<T>(pointer: number, codec: Codec<T, any>, options?: { readAheadSize?: number }): Promise<T>;
	size(): number;
}

export type BlobStoreOptions = {
	path: string;
	/** Max size per chunk file in bytes. Default 1 GiB. */
	chunkByteSize?: number;
};

type Blob = { pointer: number; data: Uint8Array };
type Frozen = { base: number; blobs: Blob[]; byteLength: number };
type Staged = { blobs: Blob[]; byteLength: number };

type ReadSnapshot = {
	frozen: Frozen | null;
	staged: Staged;
	diskLength: number;
};

function emptyStaged(): Staged {
	return { blobs: [], byteLength: 0 };
}

export class BlobStore implements Store<BlobStoreBatch> {
	readonly #path: string;
	readonly #chunkByteSize: number;
	readonly #walPath: string;
	readonly #truncatePath: string;

	/** Bytes physically on disk across chunk files. Authoritative file state. */
	#diskLength: number;
	/** Committed-but-unflushed blobs. New batches merge in here. */
	#staged: Staged = emptyStaged();
	/** Set during a flush; serves reads of the in-flight range until discard(). */
	#frozen: Frozen | null = null;

	#batchOpen = false;
	#truncating = false;

	wal: WAL | null = null;

	private constructor(
		path: string,
		chunkByteSize: number,
		walPath: string,
		truncatePath: string,
		diskLength: number,
	) {
		this.#path = path;
		this.#chunkByteSize = chunkByteSize;
		this.#walPath = walPath;
		this.#truncatePath = truncatePath;
		this.#diskLength = diskLength;
	}

	static async open(options: BlobStoreOptions): Promise<BlobStore> {
		const { path } = options;
		const chunkByteSize = options.chunkByteSize ?? 1 * 1024 * 1024 * 1024; // 1 GiB
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

		const store = new BlobStore(path, chunkByteSize, walPath, truncatePath, diskLength);

		// Crash-safe truncate recovery: an interrupted truncate left a target-length sentinel.
		// Re-applying the shrink is idempotent. Done before WAL detection (they are mutually exclusive).
		if (await exists(truncatePath)) {
			const buf = await Deno.readFile(truncatePath);
			const target = Number(new Uint8ArrayView(buf).getBigUint64(0));
			await store.#truncateDiskToOffset(target);
			await Deno.remove(truncatePath).catch(() => {});
		}

		// A WAL on disk means a flush was interrupted. Expose it for recovery,
		// but reads are NOT valid until it is applied + discarded.
		if (await exists(walPath)) {
			store.wal = store.#makeWal();
		}

		return store;
	}

	#snapshot(): ReadSnapshot {
		return { frozen: this.#frozen, staged: this.#staged, diskLength: this.#diskLength };
	}

	#baseLength(snap: ReadSnapshot): number {
		return snap.frozen ? snap.frozen.base + snap.frozen.byteLength : snap.diskLength;
	}

	length(): number {
		const snap = this.#snapshot();
		return this.#baseLength(snap) + snap.staged.byteLength;
	}

	async #readInto(
		snap: ReadSnapshot,
		pointer: number,
		buf: Uint8Array,
		allowEOF: boolean,
		extra?: Blob[],
	): Promise<number> {
		if (pointer < 0) throw new Error("pointer must be non-negative");

		let bytesRead = 0;
		let cur = pointer;
		const diskBoundary = snap.frozen ? snap.frozen.base : snap.diskLength;

		while (bytesRead < buf.length && cur < diskBoundary) {
			const chunkIndex = Math.floor(cur / this.#chunkByteSize);
			const offsetInChunk = cur % this.#chunkByteSize;
			const want = Math.min(
				buf.length - bytesRead,
				diskBoundary - cur,
				this.#chunkByteSize - offsetInChunk,
			);
			const chunkPath = join(this.#path, `chunk_${chunkIndex}`);

			let file: Deno.FsFile;
			try {
				file = await Deno.open(chunkPath, { read: true });
			} catch (e) {
				if (e instanceof Deno.errors.NotFound) {
					if (bytesRead === 0) throw new Error(`Chunk ${chunkIndex} not found for pointer ${pointer}`);
					break;
				}
				throw e;
			}
			try {
				await file.seek(offsetInChunk, Deno.SeekMode.Start);
				let got = 0;
				while (got < want) {
					const n = await file.read(buf.subarray(bytesRead + got, bytesRead + want));
					if (n === null) break;
					got += n;
				}
				bytesRead += got;
				cur += got;
				if (got < want) break;
			} finally {
				file.close();
			}
		}

		const layers: Blob[][] = [];
		if (snap.frozen) layers.push(snap.frozen.blobs);
		layers.push(snap.staged.blobs);
		if (extra) layers.push(extra);

		outer:
		for (const blobs of layers) {
			for (const entry of blobs) {
				if (bytesRead >= buf.length) break outer;
				const entryEnd = entry.pointer + entry.data.length;
				if (entryEnd <= cur) continue;
				if (entry.pointer > cur) break outer;
				const srcOffset = cur - entry.pointer;
				const take = Math.min(entry.data.length - srcOffset, buf.length - bytesRead);
				buf.set(entry.data.subarray(srcOffset, srcOffset + take), bytesRead);
				bytesRead += take;
				cur += take;
			}
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
			const buf = new Uint8Array(lengthOrCodec);
			await this.#readInto(snap, pointer, buf, false);
			return buf;
		}
		const codec = lengthOrCodec;
		const readAhead = options?.readAheadSize ?? (codec.stride.kind === "fixed" ? codec.stride.size : 4096);
		const buf = new Uint8Array(readAhead);
		const n = await this.#readInto(snap, pointer, buf, true);
		const [value] = codec.decode(buf.subarray(0, n));
		return value;
	}

	batch(): BlobStoreBatch {
		if (this.#batchOpen) throw new Error("A batch is already open");
		if (this.#truncating) throw new Error("Can't start a batch while a truncate is in progress");
		this.#batchOpen = true;

		const batchBase = this.length();
		const batchBlobs: Blob[] = [];
		let batchByteLength = 0;
		let live = true;

		const close = () => {
			live = false;
			this.#batchOpen = false;
		};

		return {
			append: (data: Uint8Array): number => {
				if (!live) throw new Error("Batch already settled");
				const pointer = batchBase + batchByteLength;
				batchBlobs.push({ pointer, data: new Uint8Array(data) }); // copy: caller may reuse the buffer
				batchByteLength += data.length;
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
					await this.#readInto(snap, pointer, buf, false, batchBlobs);
					return buf;
				}
				const codec = lengthOrCodec;
				const readAhead = options?.readAheadSize ?? (codec.stride.kind === "fixed" ? codec.stride.size : 4096);
				const buf = new Uint8Array(readAhead);
				const n = await this.#readInto(snap, pointer, buf, true, batchBlobs);
				const [value] = codec.decode(buf.subarray(0, n));
				return value;
			},
			size: (): number => batchBase + batchByteLength,
			apply: (): void => {
				if (!live) throw new Error("Batch already settled");
				for (const blob of batchBlobs) this.#staged.blobs.push(blob);
				this.#staged.byteLength += batchByteLength;
				close();
			},
			discard: (): void => {
				if (!live) return;
				close();
			},
		};
	}

	async truncate(newLength: number): Promise<void> {
		if (this.#batchOpen) throw new Error("Can't truncate while a batch is open");
		if (this.#frozen || this.wal) throw new Error("Can't truncate while a flush is in progress");
		if (this.#truncating) throw new Error("A truncate is already in progress");
		if (newLength < 0) throw new Error("newLength must be non-negative");
		if (this.#staged.blobs.length > 0) {
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
			this.#staged = emptyStaged();

			if (newLength < this.#diskLength) {
				await this.#writeTruncateTarget(newLength);
				await this.#truncateDiskToOffset(newLength); // sets #diskLength = newLength up front
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

	async createWAL(): Promise<WAL> {
		if (this.#batchOpen) throw new Error("Can't start a flush while a batch is open");
		if (this.#truncating) throw new Error("Can't start a flush while a truncate is in progress");
		if (this.#frozen || this.wal) throw new Error("A flush is already in progress");

		const frozen: Frozen = {
			base: this.#diskLength,
			blobs: this.#staged.blobs,
			byteLength: this.#staged.byteLength,
		};
		this.#frozen = frozen;
		this.#staged = emptyStaged();

		const buf = this.#encodeWal(frozen);
		await Deno.writeFile(this.#walPath, buf, { create: true });

		this.wal = this.#makeWal();
		return this.wal;
	}

	#encodeWal(frozen: Frozen): Uint8Array {
		let totalSize = 8 + 4;
		for (const { data } of frozen.blobs) totalSize += 4 + data.length;

		const buf = new Uint8Array(totalSize);
		const view = new Uint8ArrayView(buf);
		view.setBigUint64(0, BigInt(frozen.base));
		view.setUint32(8, frozen.blobs.length);
		let pos = 12;
		for (const { data } of frozen.blobs) {
			view.setUint32(pos, data.length);
			pos += 4;
			buf.set(data, pos);
			pos += data.length;
		}
		return buf;
	}

	#makeWal(): WAL {
		const apply = async (): Promise<void> => {
			const buf = await Deno.readFile(this.#walPath);
			const view = new Uint8ArrayView(buf);

			const baseOffset = Number(view.getBigUint64(0));
			await this.#truncateDiskToOffset(baseOffset);

			const count = view.getUint32(8);
			let pos = 12;
			for (let i = 0; i < count; i++) {
				const len = view.getUint32(pos);
				pos += 4;
				const data = buf.subarray(pos, pos + len);
				pos += len;
				await this.#appendBlobToDisk(data);
			}
		};

		const discard = async (): Promise<void> => {
			this.#frozen = null;
			this.wal = null;
			await Deno.remove(this.#walPath).catch(() => {});
		};

		return { apply, discard };
	}

	async #appendBlobToDisk(data: Uint8Array): Promise<void> {
		let written = 0;
		while (written < data.length) {
			const cur = this.#diskLength;
			const chunkIndex = Math.floor(cur / this.#chunkByteSize);
			const offsetInChunk = cur % this.#chunkByteSize;
			const spaceInChunk = this.#chunkByteSize - offsetInChunk;
			const take = Math.min(spaceInChunk, data.length - written);
			const slice = data.subarray(written, written + take);

			const chunkPath = join(this.#path, `chunk_${chunkIndex}`);
			const file = await Deno.open(chunkPath, { create: true, write: true });
			try {
				await file.seek(offsetInChunk, Deno.SeekMode.Start);
				await writeFile(file, slice);
			} finally {
				file.close();
			}
			written += take;
			this.#diskLength += take;
		}
	}

	/** Truncate chunk files so the stream ends at exactly `offset`. Sets #diskLength = offset. */
	async #truncateDiskToOffset(offset: number): Promise<void> {
		// Shrink the logical length first (synchronously, before any await) so concurrent
		// reads cap at `offset` and never open a chunk that's about to be removed.
		this.#diskLength = offset;
		for await (const entry of Deno.readDir(this.#path)) {
			if (!entry.isFile || !entry.name.startsWith("chunk_")) continue;
			const i = parseInt(entry.name.slice(6), 10);
			if (isNaN(i)) continue;
			const chunkStart = i * this.#chunkByteSize;
			if (chunkStart >= offset) {
				await Deno.remove(join(this.#path, entry.name));
			} else if (chunkStart + this.#chunkByteSize > offset) {
				// chunk straddles the boundary — trim it. (A boundary-aligned offset leaves
				// the preceding chunk untouched, since this is a strict ">".)
				await Deno.truncate(join(this.#path, entry.name), offset - chunkStart);
			}
			// else: chunk fully below offset — leave it
		}
	}
}
