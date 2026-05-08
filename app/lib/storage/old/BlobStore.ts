import { join } from "@std/path";
import { exists } from "@std/fs";
import { readFileFull, writeFileFull } from "../utils/fs.ts";
import type { Transaction, Transactionable } from "./Store.ts";

type CurrentChunk = { index: number; path: string; size: number };

export type BlobStoreOptions = {
	/** Maximum size of each chunk file in bytes. Default 1 GB. */
	chunkByteSize?: number;
	/** Number of blobs to keep in LRU cache keyed by pointer. Default 256. */
	cacheSize?: number;
	/** Number of chunk read file handles to keep open. Default 4. */
	fdCacheSize?: number;
};

export class BlobStoreTransaction implements Transaction {
	private readonly staged: Array<{ data: Uint8Array; pointer: number }> = [];
	private nextPointer: number;
	private committed = false;

	constructor(private readonly store: BlobStore, initialPointer: number) {
		this.nextPointer = initialPointer;
	}

	/** Stage a blob. Returns tentative byte offset (logical, ignores chunk splits). */
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

/**
 * BlobStore — append-only byte stream split across chunk files.
 * Access by logical byte offset + length. Mutations via transactions only.
 * Crash safety: commit() writes WAL atomically; finalize() flushes WAL then deletes it.
 */
export class BlobStore implements Transactionable {
	private readonly chunkByteSize: number;
	private readonly path: string;
	private readonly walPath: string;
	private readonly walTmpPath: string;
	private currentChunk: CurrentChunk | null = null;
	private activeTx: BlobStoreTransaction | null = null;
	private readonly maxCacheSize: number;
	private cache = new Map<number, Uint8Array>();
	// LRU cache of open read file handles, keyed by chunk index
	private readonly maxFdCacheSize: number;
	private fdCache = new Map<number, Deno.FsFile>();
	// Persistent write handle for the current chunk
	private writeHandle: Deno.FsFile | null = null;
	private writeHandleChunkIndex = -1;

	constructor(directoryPath: string, options: BlobStoreOptions = {}) {
		this.path = directoryPath;
		this.chunkByteSize = options.chunkByteSize ?? 1 * 1024 * 1024 * 1024;
		this.maxCacheSize = options.cacheSize ?? 256;
		this.maxFdCacheSize = options.fdCacheSize ?? 4;
		this.walPath = join(directoryPath, "store.wal");
		this.walTmpPath = join(directoryPath, "store.wal.tmp");
	}

	async transaction(): Promise<BlobStoreTransaction> {
		if (this.activeTx !== null) throw new Error("A transaction is already open");
		const current = await this.prepare();
		const initialPointer = current.index * this.chunkByteSize + current.size;
		this.activeTx = new BlobStoreTransaction(this, initialPointer);
		return this.activeTx;
	}

	async finalize(): Promise<void> {
		await this.prepare();
		this.activeTx = null;
		if (!await exists(this.walPath)) return;
		const blobs = await this.readWal();
		for (const blob of blobs) await this.appendBlob(blob);
		await this.deleteWal();
	}

	async get(pointer: number, length: number): Promise<Uint8Array> {
		const cached = this.cache.get(pointer);
		if (cached !== undefined && cached.length === length) {
			this.cache.delete(pointer);
			this.cache.set(pointer, cached);
			return cached;
		}
		const index = Math.floor(pointer / this.chunkByteSize);
		const offset = pointer % this.chunkByteSize;
		const path = join(this.path, `chunk_${index}`);
		const buffer = new Uint8Array(length);
		if (!await exists(path)) return buffer;
		const file = await this.getReadHandle(index, path);
		await file.seek(offset, Deno.SeekMode.Start);
		await readFileFull(file, buffer);
		this.addToCache(pointer, buffer);
		return buffer;
	}

	async truncate(pointerExclusive: number): Promise<void> {
		const targetIndex = Math.floor(pointerExclusive / this.chunkByteSize);
		const targetOffset = pointerExclusive % this.chunkByteSize;
		const current = await this.prepare();
		for (let i = current.index; i > targetIndex; i--) {
			this.closeFdCacheEntry(i);
			await Deno.remove(join(this.path, `chunk_${i}`));
		}
		// Close cached handle for target chunk before truncating
		this.closeFdCacheEntry(targetIndex);
		await this.closeWriteHandle();
		await Deno.truncate(join(this.path, `chunk_${targetIndex}`), targetOffset);
		for (const key of this.cache.keys()) {
			if (key >= pointerExclusive) this.cache.delete(key);
		}
		this.currentChunk = { index: targetIndex, path: join(this.path, `chunk_${targetIndex}`), size: targetOffset };
	}

	async writeWal(blobs: Uint8Array[]): Promise<void> {
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

	/** Close all cached file handles. Call before the store is discarded. */
	async close(): Promise<void> {
		for (const file of this.fdCache.values()) file.close();
		this.fdCache.clear();
		await this.closeWriteHandle();
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

	private async appendBlob(data: Uint8Array): Promise<number> {
		if (data.length > this.chunkByteSize) {
			throw new Error(`Data size (${data.length}) exceeds chunk limit (${this.chunkByteSize})`);
		}
		const current = await this.prepare();
		if (current.size + data.length > this.chunkByteSize) {
			// Roll over to next chunk — close old write handle and evict read handle
			await this.closeWriteHandle();
			this.closeFdCacheEntry(current.index);
			const index = current.index + 1;
			const path = join(this.path, `chunk_${index}`);
			this.currentChunk = current;
			current.index = index;
			current.path = path;
			current.size = 0;
			this.writeHandle = await Deno.create(path);
			this.writeHandleChunkIndex = index;
		} else if (this.writeHandle === null || this.writeHandleChunkIndex !== current.index) {
			await this.closeWriteHandle();
			this.writeHandle = await Deno.open(current.path, { create: true, write: true, append: true });
			this.writeHandleChunkIndex = current.index;
		}
		const pointer = current.index * this.chunkByteSize + current.size;
		await writeFileFull(this.writeHandle, data);
		current.size += data.length;
		this.addToCache(pointer, data);
		return pointer;
	}

	private async closeWriteHandle(): Promise<void> {
		if (this.writeHandle !== null) {
			this.writeHandle.close();
			this.writeHandle = null;
			this.writeHandleChunkIndex = -1;
		}
	}

	private async getReadHandle(index: number, path: string): Promise<Deno.FsFile> {
		const hit = this.fdCache.get(index);
		if (hit !== undefined) {
			this.fdCache.delete(index);
			this.fdCache.set(index, hit);
			return hit;
		}
		if (this.fdCache.size >= this.maxFdCacheSize) {
			const oldest = this.fdCache.keys().next().value!;
			this.fdCache.get(oldest)!.close();
			this.fdCache.delete(oldest);
		}
		const file = await Deno.open(path, { read: true });
		this.fdCache.set(index, file);
		return file;
	}

	private closeFdCacheEntry(index: number): void {
		const file = this.fdCache.get(index);
		if (file !== undefined) {
			file.close();
			this.fdCache.delete(index);
		}
	}

	releaseTransaction(): void {
		this.activeTx = null;
	}

	private addToCache(pointer: number, data: Uint8Array): void {
		if (this.maxCacheSize <= 0) return;
		if (this.cache.size >= this.maxCacheSize) {
			this.cache.delete(this.cache.keys().next().value!);
		}
		this.cache.set(pointer, new Uint8Array(data));
	}

	private preparePromise: Promise<CurrentChunk> | null = null;
	private prepare(): Promise<CurrentChunk> {
		if (this.currentChunk) return Promise.resolve(this.currentChunk);
		return this.preparePromise ??= (async () => {
			try {
				await Deno.mkdir(this.path, { recursive: true });
				let maxIndex = -1;
				for await (const entry of Deno.readDir(this.path)) {
					if (entry.isFile && entry.name.startsWith("chunk_")) {
						const i = parseInt(entry.name.slice(6), 10);
						if (!isNaN(i) && i > maxIndex) maxIndex = i;
					}
				}
				const index = maxIndex === -1 ? 0 : maxIndex;
				const path = join(this.path, `chunk_${index}`);
				const size = await exists(path, { isFile: true }) ? (await Deno.stat(path)).size : 0;
				return this.currentChunk = { index, path, size };
			} catch (err) {
				this.preparePromise = null;
				throw err;
			}
		})();
	}
}
