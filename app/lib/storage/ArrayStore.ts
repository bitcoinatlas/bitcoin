import { type Codec, StructCodec, VarInt } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import type { Store, Transaction, WAL } from "~/lib/storage/Store.ts";
import { readFile, writeFile } from "~/lib/utils/fs.ts";

export interface ArrayStore<T> extends Store<ArrayStoreTransaction<T>>, Disposable {
	get(index: number): Promise<T>;
	slice(start: number, length: number): Promise<T[]>;
	length(): number;
	truncate(newLength: number): Promise<void>;
	close(): void;
}

export interface ArrayStoreTransaction<T> extends Transaction {
	get(index: number): Promise<T>;
	set(index: number, value: T): void;
	append(value: T): number;
	length(): number;
}

export type ArrayStoreOptions<T> = {
	name: string;
	path: string;
	codec: Codec<T>;
	countCodec?: Codec<number>;
};

/**
 * WAL binary format (two sections, appends first):
 *
 *   [appends_count: varint][value bytes * appends_count]
 *   [sets_count: varint][{index: varint, value: bytes} * sets_count]
 *
 * Appends have no index — they are packed raw bytes applied as a single bulk write.
 * Sets carry an explicit index and are applied after appends so index arithmetic is stable.
 */
export async function createArrayStore<T>(options: ArrayStoreOptions<T>): Promise<ArrayStore<T>> {
	if (options.codec.stride <= 0) {
		throw new Error("Codec must have a fixed byte length");
	}

	const { name, path, codec } = options;
	const countCodec = options.countCodec ?? VarInt;

	const binPath = join(path, "data.bin");
	const walPath = join(path, `data.wal`);

	await Deno.mkdir(path, { recursive: true });
	const file = await Deno.open(binPath, { read: true, write: true, create: true });

	let diskLengthCache = (await file.stat()).size / codec.stride;
	if (!Number.isInteger(diskLengthCache)) {
		throw new Error("File size must be a multiple of codec stride");
	}

	// Mutex for seek+read/write operations on the shared file handle
	let ioLock: Promise<void> = Promise.resolve();
	function withLock<T>(fn: () => Promise<T>): Promise<T> {
		const next = ioLock.then(fn);
		ioLock = next.then(() => {}, () => {});
		return next;
	}

	// Staged appends: new entries in order (index = length + i)
	const stagedAppends: T[] = [];
	// Staged sets: in-place updates to existing indices
	const stagedSets = new Map<number, T>();

	function length(): number {
		if (self.wal) {
			throw new Error("Can't perform this operation while a WAL is in progress");
		}

		return diskLengthCache + stagedAppends.length;
	}

	function close(): void {
		if (self.wal) {
			throw new Error("Can't perform this operation while a WAL is in progress");
		}
		file.close();
	}

	async function get(index: number): Promise<T> {
		if (self.wal) {
			throw new Error("Can't perform this operation while a WAL is in progress");
		}

		if (index < 0) {
			throw new Error("Index must be non-negative");
		}
		const totalLength = diskLengthCache + stagedAppends.length;
		if (index >= totalLength) {
			throw new Error("Index out of bounds");
		}
		// Check sets first
		if (stagedSets.has(index)) return stagedSets.get(index)!;
		// Check staged appends
		if (index >= diskLengthCache) return stagedAppends[index - diskLengthCache]!;
		const offset = index * codec.stride;
		return await withLock(async () => {
			await file.seek(offset, Deno.SeekMode.Start);
			const data = await readFile(file, codec.stride);
			const [value] = codec.decode(data);
			return value;
		});
	}

	async function slice(start: number, length: number): Promise<T[]> {
		if (self.wal) {
			throw new Error("Can't perform this operation while a WAL is in progress");
		}
		if (start < 0) throw new Error("start must be non-negative");
		if (length < 0) throw new Error("length must be non-negative");

		const totalLength = diskLengthCache + stagedAppends.length;
		const size = Math.min(length, totalLength - start);
		if (size <= 0) return [];

		const results = new Array<T>(size);

		// How many entries of this slice come from disk vs staged
		const diskEnd = Math.min(start + size, diskLengthCache);
		const diskCount = Math.max(0, diskEnd - start);

		// Bulk read contiguous disk entries in one seek
		if (diskCount > 0) {
			const bulk = await withLock(async () => {
				await file.seek(start * codec.stride, Deno.SeekMode.Start);
				return await readFile(file, diskCount * codec.stride);
			});
			for (let i = 0; i < diskCount; i++) {
				const index = start + i;
				// stagedSets overrides disk
				if (stagedSets.has(index)) {
					results[i] = stagedSets.get(index)!;
				} else {
					results[i] = codec.decode(bulk.subarray(i * codec.stride, (i + 1) * codec.stride))[0];
				}
			}
		}

		// Staged appends fill the remainder
		for (let i = diskCount; i < size; i++) {
			const index = start + i;
			// stagedSets can also override staged appends
			if (stagedSets.has(index)) {
				results[i] = stagedSets.get(index)!;
			} else {
				results[i] = stagedAppends[index - diskLengthCache]!;
			}
		}

		return results;
	}

	async function truncate(newLength: number): Promise<void> {
		if (tx) {
			throw new Error("Can't perform this operation while a transaction is in progress");
		}
		if (self.wal) {
			throw new Error("Can't perform this operation while a WAL is in progress");
		}

		if (newLength < 0) throw new Error("newLength must be non-negative");
		const totalLength = diskLengthCache + stagedAppends.length;
		if (newLength > totalLength) {
			throw new Error(`newLength (${newLength}) exceeds current length (${totalLength})`);
		}

		// Drop staged appends past newLength
		const maxStagedAppends = Math.max(0, newLength - diskLengthCache);
		stagedAppends.length = maxStagedAppends;

		// Drop staged sets whose index no longer exists
		for (const index of stagedSets.keys()) {
			if (index >= newLength) stagedSets.delete(index);
		}

		// Truncate on-disk committed entries if newLength < length
		if (newLength < diskLengthCache) {
			await file.truncate(newLength * codec.stride);
			diskLengthCache = newLength;
		}
	}

	let tx: ArrayStoreTransaction<T> | null = null;
	function transaction(): ArrayStoreTransaction<T> {
		if (tx) {
			throw new Error("Transaction already in progress");
		}
		if (self.wal) {
			throw new Error("Can't start a transaction while a WAL is in progress");
		}

		const txSets = new Map<number, T>();
		const txAppends: T[] = [];
		// snapshot of length at tx open (includes already-staged appends)
		const txBaseLength = diskLengthCache + stagedAppends.length;

		tx = {
			async get(index: number): Promise<T> {
				if (index < 0) throw new Error("Index must be non-negative");
				if (index >= txBaseLength + txAppends.length) throw new Error("Index out of bounds");
				if (txSets.has(index)) return txSets.get(index)!;
				if (index >= txBaseLength) return txAppends[index - txBaseLength]!;
				return await get(index);
			},
			set(index: number, value: T): void {
				if (index < 0) throw new Error("Index must be non-negative");
				if (index >= txBaseLength + txAppends.length) throw new Error("Index out of bounds");
				if (index >= txBaseLength) {
					// Overwrite a within-tx append
					txAppends[index - txBaseLength] = value;
				} else {
					txSets.set(index, value);
				}
			},
			append(value: T): number {
				const index = txBaseLength + txAppends.length;
				txAppends.push(value);
				return index;
			},
			length(): number {
				return txBaseLength + txAppends.length;
			},
			apply(): void {
				for (const [index, value] of txSets) {
					stagedSets.set(index, value);
				}
				for (const value of txAppends) {
					stagedAppends.push(value);
				}
				// length stays as on-disk count; stagedAppends tracks the rest
				txSets.clear();
				txAppends.length = 0;
				tx = null;
			},
			discard(): void {
				txSets.clear();
				txAppends.length = 0;
				tx = null;
			},
		};

		return tx;
	}

	const setCodec = new StructCodec({ index: countCodec, value: codec });

	async function createWAL(): Promise<WAL> {
		if (self.wal) {
			throw new Error("WAL already exists");
		}
		if (tx) {
			throw new Error("Can't create a WAL while a transaction is in progress");
		}

		// Section 1: appends — count + packed raw bytes
		const appendCount = stagedAppends.length;
		const appendCountBytes = countCodec.encode(appendCount);
		const appendDataSize = appendCount * codec.stride;

		// Section 2: sets — count + [{index, value}...]
		const setEntries = Array.from(stagedSets.entries());
		const setCount = setEntries.length;
		const setCountBytes = countCodec.encode(setCount);
		const setData: Uint8Array[] = setEntries.map(([index, value]) => setCodec.encode({ index, value }));
		const setDataSize = setData.reduce((s, b) => s + b.length, 0);

		const buf = new Uint8Array(
			appendCountBytes.length + appendDataSize +
				setCountBytes.length + setDataSize,
		);
		let pos = 0;

		buf.set(appendCountBytes, pos);
		pos += appendCountBytes.length;
		for (const value of stagedAppends) {
			buf.set(codec.encode(value), pos);
			pos += codec.stride;
		}

		buf.set(setCountBytes, pos);
		pos += setCountBytes.length;
		for (const chunk of setData) {
			buf.set(chunk, pos);
			pos += chunk.length;
		}

		await Deno.writeFile(walPath, buf, { create: true });
		stagedAppends.length = 0;
		stagedSets.clear();

		const wal = await getWAL();
		if (!wal) {
			throw new Error("Failed to create WAL");
		}
		self.wal = wal;
		return wal;
	}

	async function getWAL(): Promise<WAL | null> {
		if (!await exists(walPath)) {
			return null;
		}

		return {
			async apply(): Promise<void> {
				const buf = await Deno.readFile(walPath);
				let pos = 0;

				// Section 1: appends
				const [appendCount, appendCountLen] = countCodec.decode(buf.subarray(pos));
				pos += appendCountLen;

				if (appendCount > 0) {
					const appendBytes = buf.subarray(pos, pos + appendCount * codec.stride);
					await withLock(async () => {
						await file.seek(diskLengthCache * codec.stride, Deno.SeekMode.Start);
						await writeFile(file, appendBytes);
					});
					diskLengthCache += appendCount;
					pos += appendCount * codec.stride;
				}

				// Section 2: sets
				const [setCount, setCountLen] = countCodec.decode(buf.subarray(pos));
				pos += setCountLen;

				for (let i = 0; i < setCount; i++) {
					const [entry, entryLen] = setCodec.decode(buf.subarray(pos));
					pos += entryLen;
					const offset = entry.index * codec.stride;
					await withLock(async () => {
						await file.seek(offset, Deno.SeekMode.Start);
						await writeFile(file, codec.encode(entry.value));
					});
					if (entry.index >= diskLengthCache) diskLengthCache = entry.index + 1;
				}
			},
			async discard() {
				self.wal = null;
				return await Deno.remove(walPath).catch(() => {/* ignore */});
			},
		};
	}

	const self: ArrayStore<T> = {
		name,
		get,
		slice,
		transaction,
		wal: await getWAL(),
		createWAL,
		truncate,
		length,
		close,
		[Symbol.dispose]() {
			self.close();
		},
	};

	return self;
}
