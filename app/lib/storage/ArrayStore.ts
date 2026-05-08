import { ArrayCodec, type Codec, StructCodec, VarInt } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import { v7 } from "@std/uuid";
import { readFile, writeFile } from "~/lib/utils/fs.ts";
import type { Store, Transaction, WAL } from "./Store.ts";

export interface ArrayStore<T> extends Store<ArrayStoreTransaction<T>> {
	get(index: number): Promise<T>;
	length(): number;
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

export async function createArrayStore<T>(options: ArrayStoreOptions<T>): Promise<ArrayStore<T>> {
	if (options.codec.stride <= 0) {
		throw new Error("Codec must have a fixed byte length");
	}

	const { name, path, codec } = options;
	const countCodec = options.countCodec ?? VarInt;
	await Deno.mkdir(path, { recursive: true });
	const dataPath = join(path, "data.bin");
	const file = await Deno.open(dataPath, { read: true, write: true, create: true });

	let length = (await file.stat()).size / codec.stride;
	if (!Number.isInteger(length)) {
		throw new Error("File size must be a multiple of codec stride");
	}

	const stagedChanges = new Map<number, T>();

	async function get(index: number): Promise<T> {
		if (index < 0) {
			throw new Error("Index must be non-negative");
		}
		if (index >= length) {
			throw new Error("Index out of bounds");
		}
		if (stagedChanges.has(index)) {
			return stagedChanges.get(index)!;
		}
		const offset = index * codec.stride;
		await file.seek(offset, Deno.SeekMode.Start);
		const data = await readFile(file, codec.stride);
		const [value] = codec.decode(data);
		return value;
	}

	let currentTransaction: ArrayStoreTransaction<T> | null = null;
	function assertNoTransaction(): void {
		if (currentTransaction) {
			throw new Error("Can't perform this operation while a transaction is in progress");
		}
	}
	function transaction(): ArrayStoreTransaction<T> {
		assertNoTransaction();

		const transactionChanges = new Map<number, T>();
		let transactionLength = length;

		currentTransaction = {
			async get(index: number): Promise<T> {
				if (index < 0) {
					throw new Error("Index must be non-negative");
				}
				if (index >= transactionLength) {
					throw new Error("Index out of bounds");
				}
				if (transactionChanges.has(index)) {
					return transactionChanges.get(index)!;
				}
				return await get(index);
			},
			set(index: number, value: T): void {
				if (index < 0) {
					throw new Error("Index must be non-negative");
				}
				if (index >= transactionLength) {
					throw new Error("Index out of bounds");
				}
				transactionChanges.set(index, value);
			},
			append(value: T): number {
				const index = transactionLength;
				transactionChanges.set(index, value);
				transactionLength++;
				return index;
			},
			length(): number {
				return transactionLength;
			},
			apply(): void {
				for (const [index, value] of transactionChanges.entries()) {
					stagedChanges.set(index, value);
				}
				length = transactionLength;
				transactionChanges.clear();
				currentTransaction = null;
			},
			discard(): void {
				transactionChanges.clear();
				currentTransaction = null;
			},
		};

		return currentTransaction;
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

		const walCodec = new ArrayCodec(new StructCodec({ index: countCodec, value: codec }));

		return {
			id: walId,
			async apply(): Promise<void> {
				const walFile = await Deno.open(walPath, { read: true });
				const walData = await readFile(walFile, (await walFile.stat()).size);
				const [entries] = walCodec.decode(walData);
				for (const { index, value } of entries) {
					const offset = index * codec.stride;
					const data = codec.encode(value);
					await file.seek(offset, Deno.SeekMode.Start);
					await writeFile(file, data);
					if (index >= length) length = index + 1;
				}
				walFile.close();
			},
			async discard() {
				return await Deno.remove(walPath).catch(() => {/* ignore */});
			},
			async save() {
				const entries = Array.from(stagedChanges.entries()).map(([index, value]) => ({ index, value }));
				const walData = walCodec.encode(entries);
				await Deno.writeFile(walPath, walData, { create: true });
				stagedChanges.clear();
			},
		};
	}

	return {
		name,
		get,
		transaction,
		WAL,
		length(): number {
			return length;
		},
		close(): void {
			file.close();
		},
	};
}
