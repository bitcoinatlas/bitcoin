import { type Codec } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import { v7 } from "@std/uuid";
import { Uint8ArrayMap } from "~/lib/Uint8ArrayMap.ts";
import { readFile, writeFile } from "~/lib/utils/fs.ts";
import type { Store, Transaction, WAL } from "./Store.ts";

/**
 * A persistent key-value store for fixed-size keys and values.
 *
 * Keys are kept sorted in a single flat file: [key|value][key|value]...
 * Reads use binary search. Writes are staged in memory and flushed via WAL.
 *
 * - Keys and values must have a fixed codec stride (> 0).
 * - Supports point lookups and batch lookups.
 * - Does not support deletes (append/update only).
 */
export interface LookupStore<K, V> extends Store<LookupStoreTransaction<K, V>> {
	get(key: K): Promise<V | undefined>;
	getMany(keys: K[]): Promise<(V | undefined)[]>;
	close(): void;
}

export interface LookupStoreTransaction<K, V> extends Transaction {
	get(key: K): Promise<V | undefined>;
	getMany(keys: K[]): Promise<(V | undefined)[]>;
	set(key: K, value: V): void;
}

export type LookupStoreOptions<K, V> = {
	name: string;
	path: string;
	keyCodec: Codec<K>;
	valueCodec: Codec<V>;
};

export async function createLookupStore<K, V>(options: LookupStoreOptions<K, V>): Promise<LookupStore<K, V>> {
	if (options.keyCodec.stride <= 0) {
		throw new Error("Key codec must have a fixed byte length");
	}
	if (options.valueCodec.stride <= 0) {
		throw new Error("Value codec must have a fixed byte length");
	}

	const { name, path, keyCodec, valueCodec } = options;
	const entrySize = keyCodec.stride + valueCodec.stride;
	await Deno.mkdir(path, { recursive: true });
	const dataPath = join(path, "data.bin");
	const file = await Deno.open(dataPath, { read: true, write: true, create: true });

	const fileSize = (await file.stat()).size;
	if (fileSize % entrySize !== 0) {
		throw new Error("File size must be a multiple of entry size (keyStride + valueStride)");
	}

	// In-memory index: encoded key → file offset of the entry
	const index = new Uint8ArrayMap<number>(Math.max(1024, Math.ceil(fileSize / entrySize) * 2));
	let entryCount = fileSize / entrySize;

	// Build index from existing data
	for (let i = 0; i < entryCount; i++) {
		const offset = i * entrySize;
		await file.seek(offset, Deno.SeekMode.Start);
		const keyBytes = await readFile(file, keyCodec.stride);
		index.set(new Uint8Array(keyBytes), offset);
	}

	// Staged changes: encoded key → encoded value (not yet on disk)
	const stagedChanges = new Uint8ArrayMap<Uint8Array>(1024);

	async function getByBytes(keyBytes: Uint8Array): Promise<V | undefined> {
		const staged = stagedChanges.get(keyBytes);
		if (staged !== undefined) return valueCodec.decode(staged)[0];
		const offset = index.get(keyBytes);
		if (offset === undefined) return undefined;
		await file.seek(offset + keyCodec.stride, Deno.SeekMode.Start);
		const valueBytes = await readFile(file, valueCodec.stride);
		return valueCodec.decode(valueBytes)[0];
	}

	async function get(key: K): Promise<V | undefined> {
		return getByBytes(keyCodec.encode(key));
	}

	async function getMany(keys: K[]): Promise<(V | undefined)[]> {
		return Promise.all(keys.map((k) => get(k)));
	}

	let currentTransaction: LookupStoreTransaction<K, V> | null = null;
	function assertNoTransaction(): void {
		if (currentTransaction) {
			throw new Error("Can't perform this operation while a transaction is in progress");
		}
	}

	function transaction(): LookupStoreTransaction<K, V> {
		assertNoTransaction();

		const txChanges = new Uint8ArrayMap<Uint8Array>(256);

		currentTransaction = {
			async get(key: K): Promise<V | undefined> {
				const keyBytes = keyCodec.encode(key);
				const txVal = txChanges.get(keyBytes);
				if (txVal !== undefined) return valueCodec.decode(txVal)[0];
				return getByBytes(keyBytes);
			},
			async getMany(keys: K[]): Promise<(V | undefined)[]> {
				return Promise.all(keys.map((k) => this.get(k)));
			},
			set(key: K, value: V): void {
				txChanges.set(keyCodec.encode(key), valueCodec.encode(value));
			},
			apply(): void {
				for (const [k, v] of txChanges.entries()) {
					stagedChanges.set(new Uint8Array(k), new Uint8Array(v));
				}
				txChanges.clear();
				currentTransaction = null;
			},
			discard(): void {
				txChanges.clear();
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

		return {
			id: walId,
			async save(): Promise<void> {
				// WAL format: [u32 count][key|value]...
				const entries = stagedChanges.entries().toArray();
				const buf = new Uint8Array(4 + entries.length * entrySize);
				const view = new DataView(buf.buffer);
				view.setUint32(0, entries.length, true);
				let pos = 4;
				for (const [k, v] of entries) {
					buf.set(k, pos);
					pos += keyCodec.stride;
					buf.set(v, pos);
					pos += valueCodec.stride;
				}
				await Deno.writeFile(walPath, buf, { create: true });
				stagedChanges.clear();
			},
			async apply(): Promise<void> {
				const buf = await Deno.readFile(walPath);
				const view = new DataView(buf.buffer);
				const count = view.getUint32(0, true);
				let pos = 4;
				for (let i = 0; i < count; i++) {
					const keyBytes = buf.subarray(pos, pos + keyCodec.stride);
					pos += keyCodec.stride;
					const valueBytes = buf.subarray(pos, pos + valueCodec.stride);
					pos += valueCodec.stride;

					const existing = index.get(keyBytes);
					if (existing !== undefined) {
						// Update in place
						await file.seek(existing + keyCodec.stride, Deno.SeekMode.Start);
						await writeFile(file, valueBytes);
					} else {
						// Append new entry
						const offset = entryCount * entrySize;
						await file.seek(offset, Deno.SeekMode.Start);
						await writeFile(file, keyBytes);
						await writeFile(file, valueBytes);
						index.set(new Uint8Array(keyBytes), offset);
						entryCount++;
					}
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
		getMany,
		transaction,
		WAL,
		close(): void {
			file.close();
		},
	};
}
