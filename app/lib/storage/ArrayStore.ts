import { type Codec, StructCodec, VarInt } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import { v7 } from "@std/uuid";
import { readFile, writeFile } from "~/lib/utils/fs.ts";
import type { Store, Transaction, WAL } from "./Store.ts";

export interface ArrayStore<T> extends Store<ArrayStoreTransaction<T>> {
	get(index: number): Promise<T>;
	getMany(indices: number[]): Promise<T[]>;
	length(): number;
	close(): void;
}

export interface ArrayStoreTransaction<T> extends Transaction {
	get(index: number): Promise<T>;
	getMany(indices: number[]): Promise<T[]>;
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
	await Deno.mkdir(path, { recursive: true });
	const dataPath = join(path, "data.bin");
	const file = await Deno.open(dataPath, { read: true, write: true, create: true });

	let length = (await file.stat()).size / codec.stride;
	if (!Number.isInteger(length)) {
		throw new Error("File size must be a multiple of codec stride");
	}

	// Staged appends: new entries in order (index = length + i)
	const stagedAppends: T[] = [];
	// Staged sets: in-place updates to existing indices
	const stagedSets = new Map<number, T>();

	async function get(index: number): Promise<T> {
		if (index < 0) {
			throw new Error("Index must be non-negative");
		}
		const totalLength = length + stagedAppends.length;
		if (index >= totalLength) {
			throw new Error("Index out of bounds");
		}
		// Check sets first
		if (stagedSets.has(index)) return stagedSets.get(index)!;
		// Check staged appends
		if (index >= length) return stagedAppends[index - length]!;
		const offset = index * codec.stride;
		await file.seek(offset, Deno.SeekMode.Start);
		const data = await readFile(file, codec.stride);
		const [value] = codec.decode(data);
		return value;
	}

	async function getMany(indices: number[]): Promise<T[]> {
		const results = new Array<T>(indices.length);
		type DiskLookup = { resultIdx: number; fileIndex: number };
		const diskLookups: DiskLookup[] = [];

		for (let i = 0; i < indices.length; i++) {
			const index = indices[i]!;
			if (index < 0 || index >= length + stagedAppends.length) {
				throw new Error(`Index ${index} out of bounds`);
			}
			if (stagedSets.has(index)) {
				results[i] = stagedSets.get(index)!;
			} else if (index >= length) {
				results[i] = stagedAppends[index - length]!;
			} else {
				diskLookups.push({ resultIdx: i, fileIndex: index });
			}
		}

		// Sort by file index → sequential reads, then coalesce contiguous runs into bulk reads
		diskLookups.sort((a, b) => a.fileIndex - b.fileIndex);
		let i = 0;
		while (i < diskLookups.length) {
			// Find end of contiguous run
			let j = i + 1;
			while (j < diskLookups.length && diskLookups[j]!.fileIndex === diskLookups[j - 1]!.fileIndex + 1) j++;

			const runLen = j - i;
			const startIndex = diskLookups[i]!.fileIndex;
			await file.seek(startIndex * codec.stride, Deno.SeekMode.Start);
			const bulk = await readFile(file, runLen * codec.stride);
			for (let k = 0; k < runLen; k++) {
				const { resultIdx } = diskLookups[i + k]!;
				results[resultIdx] = codec.decode(bulk.subarray(k * codec.stride, (k + 1) * codec.stride))[0];
			}
			i = j;
		}

		return results;
	}

	let currentTransaction: ArrayStoreTransaction<T> | null = null;
	function assertNoTransaction(): void {
		if (currentTransaction) {
			throw new Error("Can't perform this operation while a transaction is in progress");
		}
	}

	function transaction(): ArrayStoreTransaction<T> {
		assertNoTransaction();

		const txSets = new Map<number, T>();
		const txAppends: T[] = [];
		// snapshot of length at tx open (includes already-staged appends)
		const txBaseLength = length + stagedAppends.length;

		currentTransaction = {
			async get(index: number): Promise<T> {
				if (index < 0) throw new Error("Index must be non-negative");
				if (index >= txBaseLength + txAppends.length) throw new Error("Index out of bounds");
				if (txSets.has(index)) return txSets.get(index)!;
				if (index >= txBaseLength) return txAppends[index - txBaseLength]!;
				return await get(index);
			},
			async getMany(indices: number[]): Promise<T[]> {
				return Promise.all(indices.map((i) => this.get(i)));
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
				currentTransaction = null;
			},
			discard(): void {
				txSets.clear();
				txAppends.length = 0;
				currentTransaction = null;
			},
		};

		return currentTransaction;
	}

	const setCodec = new StructCodec({ index: VarInt, value: codec });

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
				// Section 1: appends — count + packed raw bytes
				const appendCount = stagedAppends.length;
				const appendCountBytes = VarInt.encode(appendCount);
				const appendDataSize = appendCount * codec.stride;

				// Section 2: sets — count + [{index, value}...]
				const setEntries = Array.from(stagedSets.entries());
				const setCount = setEntries.length;
				const setCountBytes = VarInt.encode(setCount);
				const setData: Uint8Array[] = setEntries.map(([index, value]) =>
					setCodec.encode({ index, value })
				);
				const setDataSize = setData.reduce((s, b) => s + b.length, 0);

				const buf = new Uint8Array(
					appendCountBytes.length + appendDataSize +
					setCountBytes.length + setDataSize,
				);
				let pos = 0;

				buf.set(appendCountBytes, pos); pos += appendCountBytes.length;
				for (const value of stagedAppends) {
					buf.set(codec.encode(value), pos);
					pos += codec.stride;
				}

				buf.set(setCountBytes, pos); pos += setCountBytes.length;
				for (const chunk of setData) {
					buf.set(chunk, pos);
					pos += chunk.length;
				}

				await Deno.writeFile(walPath, buf, { create: true });
				stagedAppends.length = 0;
				stagedSets.clear();
			},
			async apply(): Promise<void> {
				const buf = await Deno.readFile(walPath);
				let pos = 0;

				// Section 1: appends
				const [appendCount, appendCountLen] = VarInt.decode(buf.subarray(pos));
				pos += appendCountLen;

				if (appendCount > 0) {
					const appendBytes = buf.subarray(pos, pos + appendCount * codec.stride);
					await file.seek(length * codec.stride, Deno.SeekMode.Start);
					await writeFile(file, appendBytes);
					length += appendCount;
					pos += appendCount * codec.stride;
				}

				// Section 2: sets
				const [setCount, setCountLen] = VarInt.decode(buf.subarray(pos));
				pos += setCountLen;

				for (let i = 0; i < setCount; i++) {
					const [entry, entryLen] = setCodec.decode(buf.subarray(pos));
					pos += entryLen;
					const offset = entry.index * codec.stride;
					await file.seek(offset, Deno.SeekMode.Start);
					await writeFile(file, codec.encode(entry.value));
					if (entry.index >= length) length = entry.index + 1;
				}
			},
			async discard() {
				return await Deno.remove(walPath).catch(() => {/* ignore */});
			},
		};
	}

	return {
		name,
		get,
		getMany,
		transaction,
		WAL,
		length(): number {
			return length + stagedAppends.length;
		},
		close(): void {
			file.close();
		},
	};
}
