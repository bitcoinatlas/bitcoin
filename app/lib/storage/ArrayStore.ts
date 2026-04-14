import type { Codec } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { readFileFull, writeFileFull } from "../utils/fs.ts";
import { Mutex } from "../Mutex.ts";
import type { Transaction, Transactionable } from "./Transaction.ts";

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

export class ArrayStoreTransaction<T extends Codec<any>> implements Transaction {
	private readonly staged: Codec.Infer<T>[] = [];
	private committed = false;

	constructor(private readonly store: ArrayStore<T>) {}

	push(item: Codec.Infer<T>): void {
		this.staged.push(item);
	}

	concat(items: Codec.Infer<T>[]): void {
		for (const item of items) this.staged.push(item);
	}

	async commit(): Promise<void> {
		if (this.committed) throw new Error("Transaction already committed");
		await this.store.writeWal(this.staged);
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
 * ArrayStore — append-only array of fixed-size items.
 *
 * Mutations only through transactions. Reads available directly on the store.
 *
 * Crash safety:
 *   commit()   — writes WAL atomically (temp file + rename).
 *   finalize() — appends WAL entries to the file, then deletes WAL.
 *                If power dies mid-finalize, WAL still exists → replayed on next start.
 */
export class ArrayStore<T extends Codec<any>> implements Transactionable {
	private readonly path: string;
	readonly codec: T;
	private readonly mutex = new Mutex();
	private count = 0;

	private readonly walPath: string;
	private readonly walTmpPath: string;

	private activeTx: ArrayStoreTransaction<T> | null = null;

	constructor(path: string, codec: T) {
		this.path = path;
		this.codec = codec;
		this.walPath = path + ".wal";
		this.walTmpPath = path + ".wal.tmp";
		if (codec.stride < 0) throw new Error("Codec must have fixed stride");
	}

	// -------------------------------------------------------------------------
	// Transactionable
	// -------------------------------------------------------------------------

	transaction(): ArrayStoreTransaction<T> {
		if (this.activeTx !== null) throw new Error("A transaction is already open");
		this.activeTx = new ArrayStoreTransaction(this);
		return this.activeTx;
	}

	/**
	 * Apply any pending WAL to the file, then delete the WAL. Idempotent.
	 * Always reads from disk — no in-memory shortcut.
	 */
	async finalize(): Promise<void> {
		await this.prepare();
		this.activeTx = null;

		if (!await exists(this.walPath)) return;

		const entries = await this.readWal();
		if (entries.length > 0) await this.appendEntries(entries);
		await this.deleteWal();
	}

	// -------------------------------------------------------------------------
	// Reads (public)
	// -------------------------------------------------------------------------

	async length(): Promise<number> {
		await this.prepare();
		return this.count;
	}

	async get(index: number): Promise<Codec.Infer<T> | undefined> {
		await this.prepare();
		if (index < 0 || index >= this.count) return undefined;

		const buffer = new Uint8Array(this.codec.stride);
		const file = await Deno.open(this.path, { read: true });
		try {
			await file.seek(BigInt(index * this.codec.stride), Deno.SeekMode.Start);
			await readFileFull(file, buffer);
			return this.codec.decode(buffer)[0];
		} finally {
			file.close();
		}
	}

	async range(start: number, count: number): Promise<Codec.Infer<T>[]> {
		await this.prepare();
		if (start < 0) start = 0;
		if (start >= this.count || count <= 0) return [];

		const actualCount = Math.min(count, this.count - start);
		const buffer = new Uint8Array(actualCount * this.codec.stride);
		const file = await Deno.open(this.path, { read: true });
		try {
			await file.seek(BigInt(start * this.codec.stride), Deno.SeekMode.Start);
			await readFileFull(file, buffer);
		} finally {
			file.close();
		}

		const result = new Array(actualCount);
		for (let i = 0; i < actualCount; i++) {
			const bytes = buffer.subarray(i * this.codec.stride, (i + 1) * this.codec.stride);
			result[i] = this.codec.decode(bytes)[0];
		}
		return result;
	}

	// Legacy direct-mutation API (kept for backward compat with Blockchain.ts)
	async push(item: Codec.Infer<T>): Promise<number> {
		const encoded = this.codec.encode(item);
		if (encoded.length !== this.codec.stride) {
			throw new Error(`Encoded size ${encoded.length} != stride ${this.codec.stride}`);
		}

		const unlock = await this.mutex.lock();
		try {
			await this.prepare();
			const file = await Deno.open(this.path, { append: true });
			try {
				await writeFileFull(file, encoded);
			} finally {
				file.close();
			}
			return ++this.count;
		} finally {
			unlock();
		}
	}

	async concat(items: Codec.Infer<T>[]): Promise<number[]> {
		if (items.length === 0) return [];

		const totalSize = items.length * this.codec.stride;
		const buffer = new Uint8Array(totalSize);
		for (let i = 0; i < items.length; i++) {
			const encoded = this.codec.encode(items[i]);
			if (encoded.length !== this.codec.stride) {
				throw new Error(`Encoded size ${encoded.length} != stride ${this.codec.stride}`);
			}
			buffer.set(encoded, i * this.codec.stride);
		}

		const unlock = await this.mutex.lock();
		try {
			await this.prepare();
			const startIndex = this.count;
			const file = await Deno.open(this.path, { append: true });
			try {
				await writeFileFull(file, buffer);
			} finally {
				file.close();
			}
			this.count += items.length;
			return items.map((_, i) => startIndex + i);
		} finally {
			unlock();
		}
	}

	async truncate(newLength: number): Promise<void> {
		await this.prepare();
		if (newLength < 0 || newLength > this.count) {
			throw new Error(`Truncate newLength ${newLength} out of bounds`);
		}
		await Deno.truncate(this.path, newLength * this.codec.stride);
		this.count = newLength;
	}

	// -------------------------------------------------------------------------
	// Internal: WAL
	// -------------------------------------------------------------------------

	async writeWal(staged: Codec.Infer<T>[]): Promise<void> {
		// 4 bytes count + N * stride
		const buf = new Uint8Array(4 + staged.length * this.codec.stride);
		const view = new DataView(buf.buffer);
		view.setUint32(0, staged.length, true);

		let pos = 4;
		for (const item of staged) {
			const encoded = this.codec.encode(item);
			buf.set(encoded, pos);
			pos += this.codec.stride;
		}

		await Deno.writeFile(this.walTmpPath, buf);
		await Deno.rename(this.walTmpPath, this.walPath);
	}

	private async readWal(): Promise<Codec.Infer<T>[]> {
		const buf = await Deno.readFile(this.walPath);
		const view = new DataView(buf.buffer);
		const count = view.getUint32(0, true);

		const entries: Codec.Infer<T>[] = [];
		let pos = 4;
		for (let i = 0; i < count; i++) {
			const bytes = buf.subarray(pos, pos + this.codec.stride);
			entries.push(this.codec.decode(bytes)[0]);
			pos += this.codec.stride;
		}
		return entries;
	}

	private async deleteWal(): Promise<void> {
		await Deno.remove(this.walPath).catch(() => {});
	}

	// -------------------------------------------------------------------------
	// Internal: append to file
	// -------------------------------------------------------------------------

	private async appendEntries(entries: Codec.Infer<T>[]): Promise<void> {
		const buf = new Uint8Array(entries.length * this.codec.stride);
		let pos = 0;
		for (const item of entries) {
			const encoded = this.codec.encode(item);
			buf.set(encoded, pos);
			pos += this.codec.stride;
		}

		const unlock = await this.mutex.lock();
		try {
			const file = await Deno.open(this.path, { append: true });
			try {
				await writeFileFull(file, buf);
			} finally {
				file.close();
			}
			this.count += entries.length;
		} finally {
			unlock();
		}
	}

	// -------------------------------------------------------------------------
	// Internal: init
	// -------------------------------------------------------------------------

	releaseTransaction(): void {
		this.activeTx = null;
	}

	private preparePromise: Promise<void> | null = null;
	private prepare(): Promise<void> {
		if (this.preparePromise) return this.preparePromise;
		return this.preparePromise = (async () => {
			try {
				const file = await Deno.open(this.path, { create: true, read: true, write: true });
				const stat = await file.stat();
				file.close();
				if (stat.size % this.codec.stride !== 0) {
					throw new Error(`Corrupt file: size ${stat.size} not divisible by stride ${this.codec.stride}`);
				}
				this.count = stat.size / this.codec.stride;
			} catch (err) {
				this.preparePromise = null;
				throw err;
			}
		})();
	}
}
