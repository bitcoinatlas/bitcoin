import { Codec } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import type { Batch, Store, WAL } from "~/lib/storage/Store.ts";
import { writeFile } from "~/lib/utils/fs.ts";

/**
 * An append-only store for variable-size blobs, split across fixed-size chunk files.
 *
 * Each blob is addressed by a logical byte pointer (its offset in the virtual stream).
 * Reads reconstruct the blob from the appropriate chunk file.
 *
 * Staged appends are held in memory. On WAL create, blobs are written to a WAL file
 * and staged is cleared. On WAL apply, blobs are appended to chunk files in order.
 *
 * WAL format: [u32 count LE]([u32 blob_length LE][bytes])...
 */
export interface BlobStore extends Store<BlobStoreBatch> {
	get(pointer: number, length: number): Promise<Uint8Array>;
	get<T>(pointer: number, codec: Codec<T>, options?: { readAheadSize?: number }): Promise<T>;
	/** Current end-of-stream pointer (total bytes written). */
	length(): number;
	truncate(newLength: number): Promise<void>;
}

export interface BlobStoreBatch extends Batch {
	/** Stage a blob for append. Returns tentative pointer. */
	append(data: Uint8Array): number;
	get(pointer: number, length: number): Promise<Uint8Array>;
	get<T>(pointer: number, codec: Codec<T>, options?: { readAheadSize?: number }): Promise<T>;
	size(): number;
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
	const walPath = join(path, "data.wal");

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
			const lastChunkPath = join(path, `chunk_${maxIndex}`);
			const lastSize = (await Deno.stat(lastChunkPath)).size;
			totalLength = maxIndex * chunkByteSize + lastSize;
		}
	}

	// Staged appends: [{pointer, data}]
	const stagedAppends: Array<{ pointer: number; data: Uint8Array }> = [];
	let stagedLength = totalLength;

	async function readFromDisk(pointer: number, buf: Uint8Array, allowEOF: boolean): Promise<number> {
		let bytesRead = 0;
		let currentPointer = pointer;
		while (bytesRead < buf.length) {
			const chunkIndex = Math.floor(currentPointer / chunkByteSize);
			const offset = currentPointer % chunkByteSize;
			const chunkPath = join(path, `chunk_${chunkIndex}`);
			if (!await exists(chunkPath)) {
				if (bytesRead === 0) throw new Error(`Chunk ${chunkIndex} not found for pointer ${pointer}`);
				break;
			}
			const file = await Deno.open(chunkPath, { read: true });
			try {
				await file.seek(offset, Deno.SeekMode.Start);
				while (bytesRead < buf.length) {
					const n = await file.read(buf.subarray(bytesRead));
					if (n === null) break;
					bytesRead += n;
					currentPointer += n;
				}
			} finally {
				file.close();
			}
			// If we didn't reach the next chunk boundary, we hit EOF within this chunk
			if (currentPointer % chunkByteSize !== 0) break;
		}
		if (!allowEOF && bytesRead < buf.length) {
			throw new Error("Unexpected EOF reading blob");
		}
		return bytesRead;
	}

	async function getFromDisk(pointer: number, length: number): Promise<Uint8Array> {
		const buf = new Uint8Array(length);
		await readFromDisk(pointer, buf, false);
		return buf;
	}

	async function getFromDiskWithCodec<T>(pointer: number, codec: Codec<T>, readAheadSize: number): Promise<T> {
		const buf = new Uint8Array(readAheadSize);
		const bytesRead = await readFromDisk(pointer, buf, true);
		const [value] = codec.decode(buf.subarray(0, bytesRead));
		return value;
	}

	async function get(pointer: number, length: number): Promise<Uint8Array>;
	async function get<T>(pointer: number, codec: Codec<T>, options?: { readAheadSize?: number }): Promise<T>;
	async function get<T>(
		pointer: number,
		lengthOrCodec: number | Codec<T>,
		options?: { readAheadSize?: number },
	): Promise<Uint8Array | T> {
		if (typeof lengthOrCodec === "number") {
			const length = lengthOrCodec;
			for (const entry of stagedAppends) {
				if (entry.pointer === pointer && entry.data.length === length) {
					return entry.data;
				}
			}
			return await getFromDisk(pointer, length);
		} else {
			const codec = lengthOrCodec;
			const readAheadSize = options?.readAheadSize ?? (codec.stride.kind === "fixed" ? codec.stride.size : 4096);
			for (const entry of stagedAppends) {
				if (entry.pointer === pointer) {
					const [value] = codec.decode(entry.data);
					return value;
				}
			}
			return await getFromDiskWithCodec(pointer, codec, readAheadSize);
		}
	}

	function length(): number {
		return stagedLength;
	}

	let batch: BlobStoreBatch | null = null;
	function batchFn(): BlobStoreBatch {
		if (batch) throw new Error("Batch already in progress");
		if (self.wal) throw new Error("Can't start a batch while a WAL is in progress");

		const batchAppends: Array<{ pointer: number; data: Uint8Array }> = [];
		let batchLength = stagedLength;

		batch = {
			append(data: Uint8Array): number {
				const pointer = batchLength;
				batchAppends.push({ pointer, data: new Uint8Array(data) });
				batchLength = pointer + data.length;
				return pointer;
			},
			// deno-lint-ignore no-explicit-any
			async get(
				pointer: number,
				lengthOrCodec: number | Codec,
				options?: { readAheadSize?: number },
			): Promise<any> {
				if (typeof lengthOrCodec === "number") {
					const length = lengthOrCodec;
					for (const entry of batchAppends) {
						if (entry.pointer === pointer && entry.data.length === length) {
							return entry.data;
						}
					}
					return await get(pointer, length);
				} else {
					const codec = lengthOrCodec;
					for (const entry of batchAppends) {
						if (entry.pointer === pointer) {
							const [value] = codec.decode(entry.data);
							return value;
						}
					}
					return await get(pointer, codec, options);
				}
			},
			size(): number {
				return batchLength;
			},
			apply(): void {
				for (const entry of batchAppends) {
					stagedAppends.push(entry);
				}
				stagedLength = batchLength;
				batchAppends.length = 0;
				batch = null;
			},
			discard(): void {
				batchAppends.length = 0;
				batch = null;
			},
		};

		return batch;
	}

	async function appendBlobToDisk(data: Uint8Array): Promise<void> {
		const pointer = totalLength;
		let written = 0;
		while (written < data.length) {
			const currentPointer = pointer + written;
			const chunkIndex = Math.floor(currentPointer / chunkByteSize);
			const offsetInChunk = currentPointer % chunkByteSize;
			const spaceInChunk = chunkByteSize - offsetInChunk;
			const slice = data.subarray(written, written + spaceInChunk);
			const chunkPath = join(path, `chunk_${chunkIndex}`);
			const file = await Deno.open(chunkPath, { create: true, write: true, append: true });
			try {
				await writeFile(file, slice);
			} finally {
				file.close();
			}
			written += slice.length;
		}
		totalLength = pointer + data.length;
	}

	async function createWAL(): Promise<WAL> {
		if (self.wal) throw new Error("WAL already exists");
		if (batch) throw new Error("Can't create a WAL while a batch is in progress");

		// WAL format: [u32 count LE]([u32 blob_length LE][bytes])...
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
		// stagedAppends kept alive for reads during WAL window; cleared in apply/discard

		const wal = await getWAL();
		if (!wal) throw new Error("Failed to create WAL");
		self.wal = wal;
		return wal;
	}

	async function getWAL(): Promise<WAL | null> {
		if (!await exists(walPath)) return null;

		return {
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
				stagedLength = totalLength;
				stagedAppends.length = 0;
			},
			async discard(): Promise<void> {
				self.wal = null;
				stagedAppends.length = 0;
				await Deno.remove(walPath).catch(() => {/* ignore */});
			},
		};
	}

	async function truncate(newLength: number): Promise<void> {
		if (batch) throw new Error("Can't perform this operation while a batch is in progress");
		if (self.wal) throw new Error("Can't perform this operation while a WAL is in progress");
		if (newLength < 0) throw new Error("newLength must be non-negative");
		if (newLength > stagedLength) {
			throw new Error(`newLength (${newLength}) exceeds current length (${stagedLength})`);
		}

		// Drop staged appends whose pointer is >= newLength
		const kept = stagedAppends.filter((e) => e.pointer < newLength);
		stagedAppends.length = 0;
		for (const e of kept) stagedAppends.push(e);
		stagedLength = newLength;

		if (newLength < totalLength) {
			const newChunkIndex = newLength === 0 ? 0 : Math.floor((newLength - 1) / chunkByteSize);
			const newOffsetInChunk = newLength % chunkByteSize;

			for await (const entry of Deno.readDir(path)) {
				if (entry.isFile && entry.name.startsWith("chunk_")) {
					const i = parseInt(entry.name.slice(6), 10);
					if (!isNaN(i) && i > newChunkIndex) {
						await Deno.remove(join(path, entry.name));
					}
				}
			}

			if (newLength === 0) {
				await Deno.remove(join(path, "chunk_0")).catch(() => {/* may not exist */});
			} else {
				const lastChunkPath = join(path, `chunk_${newChunkIndex}`);
				if (await exists(lastChunkPath)) {
					await Deno.truncate(lastChunkPath, newOffsetInChunk);
				}
			}

			totalLength = newLength;
			stagedLength = newLength;
		}
	}

	const self: BlobStore = {
		name,
		wal: await getWAL(),
		get,
		length,
		batch: batchFn,
		createWAL,
		truncate,
	};

	return self;
}
