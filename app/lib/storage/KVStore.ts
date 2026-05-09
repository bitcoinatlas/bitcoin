import { type Codec } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import { Uint8ArrayMap } from "~/lib/Uint8ArrayMap.ts";
import { writeFile } from "~/lib/utils/fs.ts";
import type { Store, Transaction, WAL } from "~/lib/storage/Store.ts";

/**
 * A persistent key-value store for fixed-size keys and values.
 *
 * Keys are kept sorted in a single flat file: [key|value][key|value]...
 * The entire file is cached in memory; reads are synchronous slices.
 * Writes are staged in memory and flushed via WAL.
 *
 * - Keys and values must have a fixed codec stride (> 0).
 * - Supports point lookups and batch lookups.
 * - Does not support deletes (append/update only).
 *
 * WAL format: [u32 count LE][key|value]...
 */
export interface KVStore<K, V> extends Store<KVStoreTransaction<K, V>> {
	get(key: K): Promise<V | undefined>;
	getMany(keys: K[]): Promise<(V | undefined)[]>;
	clear(): Promise<void>;
	close(): void;
}

export interface KVStoreTransaction<K, V> extends Transaction {
	get(key: K): Promise<V | undefined>;
	getMany(keys: K[]): Promise<(V | undefined)[]>;
	set(key: K, value: V): void;
}

export type KVStoreOptions<K, V> = {
	name: string;
	path: string;
	keyCodec: Codec<K>;
	valueCodec: Codec<V>;
};

export async function createKVStore<K, V>(options: KVStoreOptions<K, V>): Promise<KVStore<K, V>> {
	if (options.keyCodec.stride <= 0) {
		throw new Error("Key codec must have a fixed byte length");
	}
	if (options.valueCodec.stride <= 0) {
		throw new Error("Value codec must have a fixed byte length");
	}

	const { name, path, keyCodec, valueCodec } = options;
	const entrySize = keyCodec.stride + valueCodec.stride;
	const walPath = join(path, "data.wal");

	await Deno.mkdir(path, { recursive: true });
	const dataPath = join(path, "data.bin");
	const file = await Deno.open(dataPath, { read: true, write: true, create: true });

	const fileSize = (await file.stat()).size;
	if (fileSize % entrySize !== 0) {
		throw new Error("File size must be a multiple of entry size (keyStride + valueStride)");
	}

	// In-memory cache of entire data file — reads are synchronous slices
	let dataBuf: Uint8Array = fileSize > 0 ? await Deno.readFile(dataPath) : new Uint8Array(0);

	// In-memory index: encoded key → byte offset in dataBuf
	const index = new Uint8ArrayMap<number>(Math.max(1024, Math.ceil(fileSize / entrySize) * 2));
	let entryCount = fileSize / entrySize;

	for (let i = 0; i < entryCount; i++) {
		const offset = i * entrySize;
		index.set(dataBuf.subarray(offset, offset + keyCodec.stride), offset);
	}

	// Staged changes: encoded key → encoded value (not yet on disk)
	const stagedChanges = new Uint8ArrayMap<Uint8Array>(1024);

	function getByBytes(keyBytes: Uint8Array): V | undefined {
		const staged = stagedChanges.get(keyBytes);
		if (staged !== undefined) return valueCodec.decode(staged)[0];
		const offset = index.get(keyBytes);
		if (offset === undefined) return undefined;
		return valueCodec.decode(dataBuf.subarray(offset + keyCodec.stride, offset + entrySize))[0];
	}

	async function get(key: K): Promise<V | undefined> {
		if (self.wal) throw new Error("Can't perform this operation while a WAL is in progress");
		return getByBytes(keyCodec.encode(key));
	}

	async function getMany(keys: K[]): Promise<(V | undefined)[]> {
		if (self.wal) throw new Error("Can't perform this operation while a WAL is in progress");
		return keys.map((k) => getByBytes(keyCodec.encode(k)));
	}

	function close(): void {
		if (self.wal) throw new Error("Can't perform this operation while a WAL is in progress");
		file.close();
	}

	async function clear(): Promise<void> {
		if (self.wal) throw new Error("Can't perform this operation while a WAL is in progress");
		if (tx) throw new Error("Can't clear while a transaction is in progress");
		await file.truncate(0);
		dataBuf = new Uint8Array(0);
		index.clear();
		stagedChanges.clear();
		entryCount = 0;
	}

	let tx: KVStoreTransaction<K, V> | null = null;
	function transaction(): KVStoreTransaction<K, V> {
		if (tx) throw new Error("Transaction already in progress");
		if (self.wal) throw new Error("Can't start a transaction while a WAL is in progress");

		const txChanges = new Uint8ArrayMap<Uint8Array>(256);

		tx = {
			async get(key: K): Promise<V | undefined> {
				const keyBytes = keyCodec.encode(key);
				const txVal = txChanges.get(keyBytes);
				if (txVal !== undefined) return valueCodec.decode(txVal)[0];
				return getByBytes(keyBytes);
			},
			async getMany(keys: K[]): Promise<(V | undefined)[]> {
				return keys.map((k) => {
					const keyBytes = keyCodec.encode(k);
					const txVal = txChanges.get(keyBytes);
					if (txVal !== undefined) return valueCodec.decode(txVal)[0];
					return getByBytes(keyBytes);
				});
			},
			set(key: K, value: V): void {
				txChanges.set(keyCodec.encode(key), valueCodec.encode(value));
			},
			apply(): void {
				for (const [k, v] of txChanges.entries()) {
					stagedChanges.set(new Uint8Array(k), new Uint8Array(v));
				}
				txChanges.clear();
				tx = null;
			},
			discard(): void {
				txChanges.clear();
				tx = null;
			},
		};

		return tx;
	}

	async function createWAL(): Promise<WAL> {
		if (self.wal) throw new Error("WAL already exists");
		if (tx) throw new Error("Can't create a WAL while a transaction is in progress");

		// WAL format: [u32 count LE][key|value]...
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

				type UpdateEntry = { offset: number; valueBytes: Uint8Array };
				const updates: UpdateEntry[] = [];
				const appendBuf = new Uint8Array(count * entrySize);
				let appendCount = 0;

				let pos = 4;
				for (let i = 0; i < count; i++) {
					const keyBytes = buf.subarray(pos, pos + keyCodec.stride);
					pos += keyCodec.stride;
					const valueBytes = buf.subarray(pos, pos + valueCodec.stride);
					pos += valueCodec.stride;

					const existing = index.get(keyBytes);
					if (existing !== undefined) {
						updates.push({ offset: existing, valueBytes });
					} else {
						const entryOffset = appendCount * entrySize;
						appendBuf.set(keyBytes, entryOffset);
						appendBuf.set(valueBytes, entryOffset + keyCodec.stride);
						index.set(new Uint8Array(keyBytes), (entryCount + appendCount) * entrySize);
						appendCount++;
					}
				}

				// Apply updates to in-memory buffer and disk (sorted for sequential I/O)
				updates.sort((a, b) => a.offset - b.offset);
				for (const { offset, valueBytes } of updates) {
					dataBuf.set(valueBytes, offset + keyCodec.stride);
					await file.seek(offset + keyCodec.stride, Deno.SeekMode.Start);
					await writeFile(file, valueBytes);
				}

				// Batch append new entries
				if (appendCount > 0) {
					const appendSlice = appendBuf.subarray(0, appendCount * entrySize);
					const newBuf = new Uint8Array(dataBuf.length + appendSlice.length);
					newBuf.set(dataBuf);
					newBuf.set(appendSlice, dataBuf.length);
					dataBuf = newBuf;
					await file.seek(entryCount * entrySize, Deno.SeekMode.Start);
					await writeFile(file, appendSlice);
					entryCount += appendCount;
				}
			},
			async discard(): Promise<void> {
				self.wal = null;
				await Deno.remove(walPath).catch(() => {/* ignore */});
			},
		};
	}

	const self: KVStore<K, V> = {
		name,
		wal: await getWAL(),
		get,
		getMany,
		clear,
		transaction,
		createWAL,
		close,
	};

	return self;
}
