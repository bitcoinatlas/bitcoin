import { join } from "@std/path";
import { exists } from "@std/fs";
import { readFileFull, writeFileFull } from "../utils/fs.ts";
import { Mutex } from "../Mutex.ts";
import type { Transaction, Transactionable } from "./Transaction.ts";

type CurrentChunk = {
	index: number;
	path: string;
	size: number;
};

export type ChunkedBlobStoreOptions = {
	/** Maximum size of each chunk in bytes. Default is 1 GB. */
	chunkByteSize?: number;
};

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

export class ChunkedBlobStoreTransaction implements Transaction {
	/** Staged blobs with their pre-computed tentative pointers. */
	private readonly staged: Array<{ data: Uint8Array; pointer: number }> = [];
	/** Running offset for tentative pointer computation. */
	private nextPointer: number;
	private committed = false;

	constructor(
		private readonly store: ChunkedBlobStore,
		initialPointer: number,
	) {
		this.nextPointer = initialPointer;
	}

	/**
	 * Stage a blob for appending. Returns the tentative pointer where the blob
	 * will land after finalize(). The pointer is computed deterministically
	 * based on the store's current end + already-staged bytes.
	 *
	 * Note: pointers do NOT account for chunk boundary splits — the value
	 * returned is the logical byte offset, same as ChunkedBlobStore.append().
	 */
	append(data: Uint8Array): number {
		const pointer = this.nextPointer;
		this.staged.push({ data: new Uint8Array(data), pointer });
		this.nextPointer += data.length;
		return pointer;
	}

	async commit(): Promise<void> {
		if (this.committed) throw new Error("Transaction already committed");
		await this.store.writeWal(this.staged.map((e) => e.data));
		this.committed = true;
	}

	rollback(): void {
		this.staged.length = 0;
		this.store.releaseTransaction();
	}
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/**
 * ChunkedBlobStore — append-only blob store split across chunk files.
 *
 * Mutations only through transactions. Reads available directly.
 *
 * Crash safety:
 *   commit()   — writes WAL atomically (temp file + rename).
 *   finalize() — appends each WAL blob to the store, then deletes WAL.
 */
export class ChunkedBlobStore implements Transactionable {
	private readonly chunkByteSize: number;
	private readonly path: string;
	private readonly mutex = new Mutex();

	private readonly walPath: string;
	private readonly walTmpPath: string;

	private currentChunk: CurrentChunk | null = null;
	private activeTx: ChunkedBlobStoreTransaction | null = null;

	constructor(directoryPath: string, options: ChunkedBlobStoreOptions = {}) {
		this.path = directoryPath;
		this.chunkByteSize = options.chunkByteSize ?? 1 * 1024 * 1024 * 1024;
		this.walPath = join(directoryPath, "store.wal");
		this.walTmpPath = join(directoryPath, "store.wal.tmp");
	}

	// -------------------------------------------------------------------------
	// Transactionable
	// -------------------------------------------------------------------------

	async transaction(): Promise<ChunkedBlobStoreTransaction> {
		if (this.activeTx !== null) throw new Error("A transaction is already open");
		const current = await this.prepare();
		const initialPointer = current.index * this.chunkByteSize + current.size;
		this.activeTx = new ChunkedBlobStoreTransaction(this, initialPointer);
		return this.activeTx;
	}

	/**
	 * Apply any pending WAL and delete it. Idempotent.
	 */
	async finalize(): Promise<void> {
		await this.prepare();
		this.activeTx = null;

		if (!await exists(this.walPath)) return;

		const blobs = await this.readWal();
		for (const blob of blobs) {
			await this.appendBlob(blob);
		}
		await this.deleteWal();
	}

	// -------------------------------------------------------------------------
	// Reads (public)
	// -------------------------------------------------------------------------

	async get(pointer: number, length: number): Promise<Uint8Array> {
		const index = Math.floor(pointer / this.chunkByteSize);
		const offset = pointer % this.chunkByteSize;
		const path = join(this.path, `chunk_${index}`);
		const buffer = new Uint8Array(length);

		if (!await exists(path)) {
			return buffer;
		}

		// Not locking is fine, because we only append data at the end.
		using file = await Deno.open(path, { read: true });
		try {
			await file.seek(offset, Deno.SeekMode.Start);
			await readFileFull(file, buffer);
			return buffer;
		} finally {
			file.close();
		}
	}

	async truncate(pointerExclusive: number): Promise<void> {
		const targetIndex = Math.floor(pointerExclusive / this.chunkByteSize);
		const targetOffset = pointerExclusive % this.chunkByteSize;
		const current = await this.prepare();
		const unlock = await this.mutex.lock();
		try {
			for (let index = current.index; index > targetIndex; index--) {
				const path = join(this.path, `chunk_${index}`);
				await Deno.remove(path);
			}

			const path = join(this.path, `chunk_${targetIndex}`);
			await Deno.truncate(path, targetOffset);

			this.currentChunk = {
				index: targetIndex,
				path,
				size: targetOffset,
			};
		} finally {
			unlock();
		}
	}

	// -------------------------------------------------------------------------
	// Internal: WAL
	// -------------------------------------------------------------------------

	async writeWal(blobs: Uint8Array[]): Promise<void> {
		// Format: [u32 count][(u32 length + bytes)...]
		let totalSize = 4;
		for (const blob of blobs) totalSize += 4 + blob.length;

		const buf = new Uint8Array(totalSize);
		const view = new DataView(buf.buffer);
		view.setUint32(0, blobs.length, true);

		let pos = 4;
		for (const blob of blobs) {
			view.setUint32(pos, blob.length, true);
			pos += 4;
			buf.set(blob, pos);
			pos += blob.length;
		}

		await Deno.writeFile(this.walTmpPath, buf);
		await Deno.rename(this.walTmpPath, this.walPath);
	}

	private async readWal(): Promise<Uint8Array[]> {
		const buf = await Deno.readFile(this.walPath);
		const view = new DataView(buf.buffer);
		const count = view.getUint32(0, true);

		const blobs: Uint8Array[] = [];
		let pos = 4;
		for (let i = 0; i < count; i++) {
			const length = view.getUint32(pos, true);
			pos += 4;
			blobs.push(new Uint8Array(buf.subarray(pos, pos + length)));
			pos += length;
		}
		return blobs;
	}

	private async deleteWal(): Promise<void> {
		await Deno.remove(this.walPath).catch(() => {});
		await Deno.remove(this.walTmpPath).catch(() => {});
	}

	// -------------------------------------------------------------------------
	// Internal: append single blob (chunk-aware)
	// -------------------------------------------------------------------------

	private async appendBlob(data: Uint8Array): Promise<number> {
		if (data.length > this.chunkByteSize) {
			throw new Error(`Data size (${data.length}) exceeds chunk limit (${this.chunkByteSize})`);
		}

		const unlock = await this.mutex.lock();
		const current = await this.prepare();

		let file: Deno.FsFile | undefined;
		try {
			if (current.size + data.length > this.chunkByteSize) {
				const index = current.index + 1;
				const path = join(this.path, `chunk_${index}`);
				this.currentChunk = { index, path, size: 0 };
				file = await Deno.create(path);
			} else {
				file = await Deno.open(current.path, { create: true, write: true });
			}

			const pointer = current.index * this.chunkByteSize + current.size;
			await file.seek(0, Deno.SeekMode.End);
			await writeFileFull(file, data);
			current.size += data.length;
			return pointer;
		} finally {
			file?.close();
			unlock();
		}
	}

	// -------------------------------------------------------------------------
	// Internal: init
	// -------------------------------------------------------------------------

	releaseTransaction(): void {
		this.activeTx = null;
	}

	private preparePromise: Promise<CurrentChunk> | null = null;
	private prepare(): Promise<CurrentChunk> {
		if (this.currentChunk) return Promise.resolve(this.currentChunk);
		return this.preparePromise ??= (async () => {
			try {
				await Deno.mkdir(this.path, { recursive: true });

				const entries = Deno.readDir(this.path);
				let maxIndex = -1;

				for await (const entry of entries) {
					if (entry.isFile && entry.name.startsWith("chunk_")) {
						const index = parseInt(entry.name.slice(6), 10);
						if (!isNaN(index) && index > maxIndex) {
							maxIndex = index;
						}
					}
				}

				const index = maxIndex === -1 ? 0 : maxIndex;
				const path = join(this.path, `chunk_${index}`);

				if (await exists(path, { isFile: true })) {
					const { size } = await Deno.stat(path);
					this.currentChunk = { index, path, size };
				} else {
					this.currentChunk = { index, path, size: 0 };
				}

				return this.currentChunk;
			} catch (err) {
				this.preparePromise = null;
				throw err;
			}
		})();
	}
}
