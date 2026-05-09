import { exists } from "@std/fs";
import { join } from "@std/path";
import { Codec } from "@nomadshiba/codec";
import { writeFile } from "~/lib/utils/fs.ts";
import type { Store, Transaction, WAL } from "~/lib/storage/Store.ts";

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
export interface BlobStore extends Store<BlobStoreTransaction> {
	get(pointer: number, length: number): Promise<Uint8Array>;
	get<T>(pointer: number, codec: Codec<T>, options?: { readAheadSize?: number }): Promise<T>;
	/** Current end-of-stream pointer (total bytes written). */
	length(): number;
	truncate(newLength: number): Promise<void>;
}

export interface BlobStoreTransaction extends Transaction {
	/** Stage a blob for append. Returns tentative pointer. */
	append(data: Uint8Array): number;
	get(pointer: number, length: number): Promise<Uint8Array>;
	get<T>(pointer: number, codec: Codec<T>, options?: { readAheadSize?: number }): Promise<T>;
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

	/**
	 * Compute the canonical pointer for a blob of `dataSize` bytes given the
	 * current virtual stream position `pos`. Mirrors the roll-to-next-chunk
	 * logic in appendBlobToDisk so tx.append and disk writes always agree.
	 */
	function nextPointer(pos: number, dataSize: number): number {
		const offsetInChunk = pos % chunkByteSize;
		if (offsetInChunk + dataSize > chunkByteSize) {
			const chunkIndex = Math.floor(pos / chunkByteSize);
			return (chunkIndex + 1) * chunkByteSize;
		}
		return pos;
	}

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

	async function getFromDiskWithCodec<T>(pointer: number, codec: Codec<T>, readAheadSize: number): Promise<T> {
		const chunkIndex = Math.floor(pointer / chunkByteSize);
		const offset = pointer % chunkByteSize;
		const chunkPath = join(path, `chunk_${chunkIndex}`);
		if (!await exists(chunkPath)) {
			throw new Error(`Chunk ${chunkIndex} not found for pointer ${pointer}`);
		}
		const file = await Deno.open(chunkPath, { read: true });
		try {
			await file.seek(offset, Deno.SeekMode.Start);
			const buf = new Uint8Array(readAheadSize);
			let bytesRead = 0;
			while (bytesRead < readAheadSize) {
				const n = await file.read(buf.subarray(bytesRead));
				if (n === null) break;
				bytesRead += n;
			}
			const [value] = codec.decode(buf.subarray(0, bytesRead));
			return value;
		} finally {
			file.close();
		}
	}

	async function get(pointer: number, length: number): Promise<Uint8Array>;
	async function get<T>(pointer: number, codec: Codec<T>, options?: { readAheadSize?: number }): Promise<T>;
	async function get<T>(
		pointer: number,
		lengthOrCodec: number | Codec<T>,
		options?: { readAheadSize?: number },
	): Promise<Uint8Array | T> {
		if (self.wal) {
			throw new Error("Can't perform this operation while a WAL is in progress");
		}
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
			const readAheadSize = options?.readAheadSize ?? (codec.stride > 0 ? codec.stride : 4096);
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
		if (self.wal) {
			throw new Error("Can't perform this operation while a WAL is in progress");
		}
		return stagedLength;
	}

	let tx: BlobStoreTransaction | null = null;
	function transaction(): BlobStoreTransaction {
		if (tx) throw new Error("Transaction already in progress");
		if (self.wal) throw new Error("Can't start a transaction while a WAL is in progress");

		const txAppends: Array<{ pointer: number; data: Uint8Array }> = [];
		let txLength = stagedLength;

		tx = {
			append(data: Uint8Array): number {
				if (data.length > chunkByteSize) {
					throw new Error(`Blob size (${data.length}) exceeds chunk limit (${chunkByteSize})`);
				}
				const pointer = nextPointer(txLength, data.length);
				txAppends.push({ pointer, data: new Uint8Array(data) });
				txLength = pointer + data.length;
				return pointer;
			},
			// deno-lint-ignore no-explicit-any
			async get(
				pointer: number,
				lengthOrCodec: number | Codec<any>,
				options?: { readAheadSize?: number },
			): Promise<any> {
				if (typeof lengthOrCodec === "number") {
					const length = lengthOrCodec;
					for (const entry of txAppends) {
						if (entry.pointer === pointer && entry.data.length === length) {
							return entry.data;
						}
					}
					return await get(pointer, length);
				} else {
					const codec = lengthOrCodec;
					for (const entry of txAppends) {
						if (entry.pointer === pointer) {
							const [value] = codec.decode(entry.data);
							return value;
						}
					}
					return await get(pointer, codec, options);
				}
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
				tx = null;
			},
			discard(): void {
				txAppends.length = 0;
				tx = null;
			},
		};

		return tx;
	}

	async function appendBlobToDisk(data: Uint8Array): Promise<void> {
		const pointer = nextPointer(totalLength, data.length);
		const chunkIndex = Math.floor(pointer / chunkByteSize);
		const chunkPath = join(path, `chunk_${chunkIndex}`);
		const file = await Deno.open(chunkPath, { create: true, write: true, append: true });
		try {
			await writeFile(file, data);
		} finally {
			file.close();
		}
		totalLength = pointer + data.length;
	}

	async function createWAL(): Promise<WAL> {
		if (self.wal) throw new Error("WAL already exists");
		if (tx) throw new Error("Can't create a WAL while a transaction is in progress");

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
		stagedAppends.length = 0;

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
			},
			async discard(): Promise<void> {
				self.wal = null;
				await Deno.remove(walPath).catch(() => {/* ignore */});
			},
		};
	}

	async function truncate(newLength: number): Promise<void> {
		if (tx) throw new Error("Can't perform this operation while a transaction is in progress");
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
		transaction,
		createWAL,
		truncate,
	};

	return self;
}
