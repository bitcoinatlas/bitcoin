import { exists } from "@std/fs";
import { join } from "@std/path";
import { v7 } from "@std/uuid";
import { writeFile } from "~/lib/utils/fs.ts";
import type { Store, Transaction, WAL } from "./Store.ts";

/**
 * An append-only store for variable-size blobs, split across fixed-size chunk files.
 *
 * Each blob is addressed by a logical byte pointer (its offset in the virtual stream).
 * Reads reconstruct the blob from the appropriate chunk file.
 *
 * Staged appends are held in memory. On WAL save, blobs are written to a WAL file.
 * On WAL apply, blobs are appended to chunk files in order.
 */
export interface BlobStore extends Store<BlobStoreTransaction> {
	get(pointer: number, length: number): Promise<Uint8Array>;
	/** Current end-of-stream pointer (total bytes written). */
	length(): number;
}

export interface BlobStoreTransaction extends Transaction {
	/** Stage a blob for append. Returns tentative pointer. */
	append(data: Uint8Array): number;
	get(pointer: number, length: number): Promise<Uint8Array>;
	length(): number;
}

export type BlobStoreOptions = {
	name: string;
	path: string;
	/** Max size per chunk file in bytes. Default 1 GB. */
	chunkByteSize?: number;
};

export async function createBlobStore(options: BlobStoreOptions): Promise<BlobStore> {
	const { name, path } = options;
	const chunkByteSize = options.chunkByteSize ?? 1 * 1024 * 1024 * 1024;

	await Deno.mkdir(path, { recursive: true });

	// Find current total length from chunk files
	let totalLength = 0;
	{
		let maxIndex = -1;
		for await (const entry of Deno.readDir(path)) {
			if (entry.isFile && entry.name.startsWith("chunk_")) {
				const i = parseInt(entry.name.slice(6), 10);
				if (!isNaN(i) && i > maxIndex) maxIndex = i;
			}
		}
		if (maxIndex >= 0) {
			// Full chunks before last + last chunk size
			const lastChunkPath = join(path, `chunk_${maxIndex}`);
			const lastSize = (await Deno.stat(lastChunkPath)).size;
			totalLength = maxIndex * chunkByteSize + lastSize;
		}
	}

	// Staged appends: [{pointer, data}]
	const stagedAppends: Array<{ pointer: number; data: Uint8Array }> = [];
	let stagedLength = totalLength;

	async function getFromDisk(pointer: number, length: number): Promise<Uint8Array> {
		const chunkIndex = Math.floor(pointer / chunkByteSize);
		const offset = pointer % chunkByteSize;
		const chunkPath = join(path, `chunk_${chunkIndex}`);
		if (!await exists(chunkPath)) {
			throw new Error(`Chunk ${chunkIndex} not found for pointer ${pointer}`);
		}
		const file = await Deno.open(chunkPath, { read: true });
		try {
			await file.seek(offset, Deno.SeekMode.Start);
			const buf = new Uint8Array(length);
			let bytesRead = 0;
			while (bytesRead < length) {
				const n = await file.read(buf.subarray(bytesRead));
				if (n === null) throw new Error("Unexpected EOF reading blob");
				bytesRead += n;
			}
			return buf;
		} finally {
			file.close();
		}
	}

	async function get(pointer: number, length: number): Promise<Uint8Array> {
		// Check staged appends first
		for (const entry of stagedAppends) {
			if (entry.pointer === pointer && entry.data.length === length) {
				return entry.data;
			}
		}
		return getFromDisk(pointer, length);
	}

	let currentTransaction: BlobStoreTransaction | null = null;
	function assertNoTransaction(): void {
		if (currentTransaction) {
			throw new Error("Can't perform this operation while a transaction is in progress");
		}
	}

	function transaction(): BlobStoreTransaction {
		assertNoTransaction();

		const txAppends: Array<{ pointer: number; data: Uint8Array }> = [];
		let txLength = stagedLength;

		currentTransaction = {
			append(data: Uint8Array): number {
				if (data.length > chunkByteSize) {
					throw new Error(`Blob size (${data.length}) exceeds chunk limit (${chunkByteSize})`);
				}
				const pointer = txLength;
				txAppends.push({ pointer, data: new Uint8Array(data) });
				txLength += data.length;
				return pointer;
			},
			async get(pointer: number, length: number): Promise<Uint8Array> {
				for (const entry of txAppends) {
					if (entry.pointer === pointer && entry.data.length === length) {
						return entry.data;
					}
				}
				return get(pointer, length);
			},
			length(): number {
				return txLength;
			},
			apply(): void {
				for (const entry of txAppends) {
					stagedAppends.push(entry);
				}
				stagedLength = txLength;
				txAppends.length = 0;
				currentTransaction = null;
			},
			discard(): void {
				txAppends.length = 0;
				currentTransaction = null;
			},
		};

		return currentTransaction;
	}

	async function appendBlobToDisk(data: Uint8Array): Promise<number> {
		const chunkIndex = Math.floor(totalLength / chunkByteSize);
		const offsetInChunk = totalLength % chunkByteSize;
		const chunkPath = join(path, `chunk_${chunkIndex}`);

		// Roll to next chunk if blob doesn't fit
		if (offsetInChunk + data.length > chunkByteSize) {
			// Write remainder of current chunk as padding? No — just roll over.
			// BlobStore contract: a single blob must fit within one chunk.
			if (data.length > chunkByteSize) {
				throw new Error(`Blob size (${data.length}) exceeds chunk limit (${chunkByteSize})`);
			}
			const nextChunkIndex = chunkIndex + 1;
			const nextChunkPath = join(path, `chunk_${nextChunkIndex}`);
			// Advance totalLength to start of next chunk
			totalLength = nextChunkIndex * chunkByteSize;
			const file = await Deno.open(nextChunkPath, { create: true, write: true, append: true });
			try {
				await writeFile(file, data);
			} finally {
				file.close();
			}
			totalLength += data.length;
			return nextChunkIndex * chunkByteSize;
		}

		const file = await Deno.open(chunkPath, { create: true, write: true, append: true });
		try {
			await writeFile(file, data);
		} finally {
			file.close();
		}
		const pointer = totalLength;
		totalLength += data.length;
		return pointer;
	}

	async function WAL(options: { id: string }): Promise<WAL | null>;
	async function WAL(options?: { id?: undefined }): Promise<WAL>;
	async function WAL(options?: { id?: string }): Promise<WAL | null> {
		assertNoTransaction();

		const walId = options?.id ?? v7.generate();
		const walPath = join(path, `${walId}.wal`);

		if (options?.id && !await exists(walPath)) {
			return null;
		}

		return {
			id: walId,
			async save(): Promise<void> {
				// WAL format: [u32 count]([u32 length][bytes])...
				let totalSize = 4;
				for (const { data } of stagedAppends) totalSize += 4 + data.length;
				const buf = new Uint8Array(totalSize);
				const view = new DataView(buf.buffer);
				view.setUint32(0, stagedAppends.length, true);
				let pos = 4;
				for (const { data } of stagedAppends) {
					view.setUint32(pos, data.length, true);
					pos += 4;
					buf.set(data, pos);
					pos += data.length;
				}
				await Deno.writeFile(walPath, buf, { create: true });
				stagedAppends.length = 0;
			},
			async apply(): Promise<void> {
				const buf = await Deno.readFile(walPath);
				const view = new DataView(buf.buffer);
				const count = view.getUint32(0, true);
				let pos = 4;
				for (let i = 0; i < count; i++) {
					const len = view.getUint32(pos, true);
					pos += 4;
					const data = buf.subarray(pos, pos + len);
					pos += len;
					await appendBlobToDisk(data);
				}
			},
			async discard(): Promise<void> {
				await Deno.remove(walPath).catch(() => {/* ignore */});
			},
		};
	}

	return {
		name,
		get,
		length(): number {
			return stagedLength;
		},
		transaction,
		WAL,
	};
}
