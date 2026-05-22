import { type Codec, Stride } from "@nomadshiba/codec";
import { join } from "@std/path";
import { exists } from "@std/fs";
import { readFile, writeFile } from "~/lib/utils/fs.ts";
import type { Store, Transaction, WAL } from "~/lib/storage/Store.ts";
import { Uint8ArrayMap } from "~/lib/Uint8ArrayMap.ts";

/**
 * A persistent key-value store backed by a sharded on-disk open-addressing hash table.
 *
 * 256 shards keyed by first byte of encoded key. Each shard is a flat file of fixed-size slots:
 *   [occupied: u8][key: keyStride bytes][value: valueStride bytes]
 *
 * Reads:  hash(key) % slotCount → seek → read slot → linear probe on collision. O(1), 1-3 seeks.
 * Writes: buffered in memory (raw key+value pairs), slot resolution + disk write on WAL flush.
 *
 * Growth: each shard grows independently by SLOTS_GROWTH_PER_SHARD when load > 0.75.
 *
 * Shard meta format: [u32 slotCount LE][u32 liveCount LE]
 * WAL format: [u8 shardCount]([u8 shardIdx][u32 entryCount LE]([u32 slotIdx LE][slotBytes])...)
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
	keyCodec: Codec<K> & { stride: Stride<"fixed"> };
	valueCodec: Codec<V> & { stride: Stride<"fixed"> };
};

const NUM_SHARDS = 256;
const OCCUPIED_EMPTY = 0;
const OCCUPIED_LIVE = 1;
const SLOTS_GROWTH_PER_SHARD = 4096;
const INITIAL_SLOTS_PER_SHARD = SLOTS_GROWTH_PER_SHARD;
const LOAD_FACTOR_THRESHOLD = 0.75;
const META_SIZE = 8; // u32 slotCount + u32 liveCount

type ShardState = {
	file: Deno.FsFile;
	slotCount: number;
	liveCount: number;
};

export async function createKVStore<K, V>(options: KVStoreOptions<K, V>): Promise<KVStore<K, V>> {
	const { name, path, keyCodec, valueCodec } = options;
	const keyStride = keyCodec.stride.size;
	const valueStride = valueCodec.stride.size;
	const slotSize = 1 + keyStride + valueStride;

	await Deno.mkdir(path, { recursive: true });

	// Open / init all 256 shards
	const shards: ShardState[] = [];
	for (let s = 0; s < NUM_SHARDS; s++) {
		const shardDir = join(path, `shard_${s}`);
		await Deno.mkdir(shardDir, { recursive: true });

		const dataPath = join(shardDir, "data.bin");
		const metaPath = join(shardDir, "meta.bin");

		let slotCount: number;
		let liveCount: number;

		if (await exists(metaPath)) {
			const buf = await Deno.readFile(metaPath);
			const view = new DataView(buf.buffer);
			slotCount = view.getUint32(0, true);
			liveCount = view.getUint32(4, true);
		} else {
			slotCount = INITIAL_SLOTS_PER_SHARD;
			liveCount = 0;
			await writeMeta(metaPath, slotCount, liveCount);
		}

		const file = await Deno.open(dataPath, { read: true, write: true, create: true });
		const expectedSize = slotCount * slotSize;
		const actualSize = (await file.stat()).size;
		if (actualSize < expectedSize) await file.truncate(expectedSize);

		shards.push({ file, slotCount, liveCount });
	}

	// Per-shard I/O mutex
	const ioLocks: Promise<void>[] = shards.map(() => Promise.resolve());
	function withLock<T>(s: number, fn: () => Promise<T>): Promise<T> {
		const next = ioLocks[s]!.then(fn);
		ioLocks[s] = next.then(() => {}, () => {});
		return next;
	}

	// --- disk helpers ---

	async function readSlot(s: number, slotIdx: number): Promise<Uint8Array> {
		const shard = shards[s]!;
		return withLock(s, async () => {
			await shard.file.seek(slotIdx * slotSize, Deno.SeekMode.Start);
			return readFile(shard.file, slotSize);
		});
	}

	async function writeSlotDirect(s: number, slotIdx: number, buf: Uint8Array): Promise<void> {
		const shard = shards[s]!;
		await withLock(s, async () => {
			await shard.file.seek(slotIdx * slotSize, Deno.SeekMode.Start);
			await writeFile(shard.file, buf);
		});
	}

	/**
	 * Find slot for keyBytes in shard s.
	 * walSlots: slots being written in current WAL (slot index → slotBuf), checked before disk.
	 * Returns { slotIdx, found }.
	 */
	async function findSlot(
		s: number,
		keyBytes: Uint8Array,
		walSlots: Map<number, Uint8Array> | null,
	): Promise<{ slotIdx: number; found: boolean }> {
		const shard = shards[s]!;
		const start = slotHash(keyBytes, shard.slotCount);
		let i = start;
		do {
			if (walSlots?.has(i)) {
				const buf = walSlots.get(i)!;
				if (buf[0] === OCCUPIED_EMPTY) return { slotIdx: i, found: false };
				if (bytesEqual(buf.subarray(1, 1 + keyStride), keyBytes)) return { slotIdx: i, found: true };
				i = (i + 1) % shard.slotCount;
				continue;
			}
			const buf = await readSlot(s, i);
			if (buf[0] === OCCUPIED_EMPTY) return { slotIdx: i, found: false };
			if (buf[0] === OCCUPIED_LIVE && bytesEqual(buf.subarray(1, 1 + keyStride), keyBytes)) {
				return { slotIdx: i, found: true };
			}
			i = (i + 1) % shard.slotCount;
		} while (i !== start);
		throw new Error(`[KVStore:${name}] shard ${s} is full`);
	}

	async function getByBytes(
		keyBytes: Uint8Array,
		stagedPairs: Uint8ArrayMap<Uint8Array> | null,
	): Promise<V | undefined> {
		if (stagedPairs) {
			const staged = stagedPairs.get(keyBytes);
			if (staged !== undefined) return valueCodec.decode(staged)[0];
		}
		const s = shardIndex(keyBytes);
		const { found, slotIdx } = await findSlot(s, keyBytes, null);
		if (!found) return undefined;
		const buf = await readSlot(s, slotIdx);
		return valueCodec.decode(buf.subarray(1 + keyStride))[0];
	}

	// Committed on tx.apply(), flushed to disk on createWAL()
	const staged: Uint8ArrayMap<Uint8Array>[] = Array.from({ length: NUM_SHARDS }, () => new Uint8ArrayMap(64));

	// --- grow shard if needed (called during createWAL before slot resolution) ---
	async function maybeGrow(s: number, newEntryCount: number): Promise<void> {
		const shard = shards[s]!;
		const projected = shard.liveCount + newEntryCount;
		if (projected / shard.slotCount < LOAD_FACTOR_THRESHOLD) return;

		console.log(`[KVStore:${name}] shard ${s} growing: live=${projected} slots=${shard.slotCount}`);

		const oldSlotCount = shard.slotCount;
		const newSlotCount = oldSlotCount + SLOTS_GROWTH_PER_SHARD;

		// Read all live entries
		const entries: Array<{ key: Uint8Array; value: Uint8Array }> = [];
		for (let i = 0; i < oldSlotCount; i++) {
			const buf = await readSlot(s, i);
			if (buf[0] === OCCUPIED_LIVE) {
				entries.push({
					key: new Uint8Array(buf.subarray(1, 1 + keyStride)),
					value: new Uint8Array(buf.subarray(1 + keyStride)),
				});
			}
		}

		// Expand file (zeros = empty slots)
		const newSize = newSlotCount * slotSize;
		await withLock(s, async () => {
			await shard.file.truncate(0);
			await shard.file.truncate(newSize);
		});
		shard.slotCount = newSlotCount;
		shard.liveCount = 0;

		// Re-insert all old entries
		for (const { key, value } of entries) {
			const { slotIdx } = await findSlot(s, key, null);
			const buf = new Uint8Array(slotSize);
			buf[0] = OCCUPIED_LIVE;
			buf.set(key, 1);
			buf.set(value, 1 + keyStride);
			await writeSlotDirect(s, slotIdx, buf);
			shard.liveCount++;
		}

		const shardDir = join(path, `shard_${s}`);
		await writeMeta(join(shardDir, "meta.bin"), newSlotCount, shard.liveCount);
		console.log(`[KVStore:${name}] shard ${s} grown to ${newSlotCount} slots`);
	}

	// --- public get/getMany ---

	async function get(key: K): Promise<V | undefined> {
		const keyBytes = keyCodec.encode(key);
		return getByBytes(keyBytes, staged[shardIndex(keyBytes)]!);
	}

	async function getMany(keys: K[]): Promise<(V | undefined)[]> {
		return Promise.all(keys.map((k) => {
			const keyBytes = keyCodec.encode(k);
			return getByBytes(keyBytes, staged[shardIndex(keyBytes)]!);
		}));
	}

	async function clear(): Promise<void> {
		if (self.wal) throw new Error("Can't clear while WAL is in progress");
		if (tx) throw new Error("Can't clear while transaction is in progress");
		for (let s = 0; s < NUM_SHARDS; s++) {
			const shard = shards[s]!;
			staged[s]!.clear();
			await withLock(s, async () => {
				await shard.file.truncate(0);
				await shard.file.truncate(shard.slotCount * slotSize);
			});
			shard.liveCount = 0;
			const shardDir = join(path, `shard_${s}`);
			await writeMeta(join(shardDir, "meta.bin"), shard.slotCount, 0);
		}
	}

	function close(): void {
		if (self.wal) throw new Error("Can't close while WAL is in progress");
		for (const shard of shards) shard.file.close();
	}

	// --- transaction: buffers raw pairs, no disk I/O ---

	let tx: KVStoreTransaction<K, V> | null = null;

	function transaction(): KVStoreTransaction<K, V> {
		if (tx) throw new Error("Transaction already in progress");
		if (self.wal) throw new Error("Can't start transaction while WAL is in progress");

		const txStaged: Uint8ArrayMap<Uint8Array>[] = Array.from({ length: NUM_SHARDS }, () => new Uint8ArrayMap(64));

		tx = {
			async get(key: K): Promise<V | undefined> {
				const keyBytes = keyCodec.encode(key);
				const s = shardIndex(keyBytes);
				// tx staged → store staged → disk
				const txVal = txStaged[s]!.get(keyBytes);
				if (txVal !== undefined) return valueCodec.decode(txVal)[0];
				return getByBytes(keyBytes, staged[s]!);
			},
			async getMany(keys: K[]): Promise<(V | undefined)[]> {
				return Promise.all(keys.map((k) => tx!.get(k)));
			},
			set(key: K, value: V): void {
				const keyBytes = keyCodec.encode(key);
				const valueBytes = valueCodec.encode(value);
				txStaged[shardIndex(keyBytes)]!.set(keyBytes, valueBytes);
			},
			apply(): void {
				// Move tx staged → store staged. No disk I/O.
				for (let s = 0; s < NUM_SHARDS; s++) {
					for (const [keyBytes, valueBytes] of txStaged[s]!) {
						staged[s]!.set(keyBytes, valueBytes);
					}
					txStaged[s]!.clear();
				}
				tx = null;
			},
			discard(): void {
				for (let s = 0; s < NUM_SHARDS; s++) txStaged[s]!.clear();
				tx = null;
			},
		};

		return tx;
	}

	// --- WAL: resolve slots for all staged pairs, write WAL file ---

	const walPath = join(path, "data.wal");

	async function createWAL(): Promise<WAL> {
		if (self.wal) throw new Error("WAL already exists");
		if (tx) throw new Error("Can't create WAL while transaction is in progress");

		// Per-shard: resolve slot indices for all staged pairs
		// walSlots[s]: slotIdx → slotBuf (what to write to disk)
		const walSlots: Map<number, Uint8Array>[] = Array.from({ length: NUM_SHARDS }, () => new Map());

		for (let s = 0; s < NUM_SHARDS; s++) {
			const shardStaged = staged[s]!;
			if (shardStaged.size === 0) continue;

			// Count new entries (not already on disk) for growth check
			let newCount = 0;
			for (const [keyBytes] of shardStaged) {
				const { found } = await findSlot(s, keyBytes, walSlots[s]!);
				if (!found) newCount++;
			}
			await maybeGrow(s, newCount);

			// Resolve slots
			for (const [keyBytes, valueBytes] of shardStaged) {
				const { slotIdx } = await findSlot(s, keyBytes, walSlots[s]!);
				const buf = new Uint8Array(slotSize);
				buf[0] = OCCUPIED_LIVE;
				buf.set(keyBytes, 1);
				buf.set(valueBytes, 1 + keyStride);
				walSlots[s]!.set(slotIdx, buf);
			}
		}

		// Serialize WAL
		const parts: Uint8Array[] = [];
		let shardCount = 0;
		for (let s = 0; s < NUM_SHARDS; s++) {
			if (walSlots[s]!.size === 0) continue;
			shardCount++;
			const header = new Uint8Array(5);
			header[0] = s;
			new DataView(header.buffer).setUint32(1, walSlots[s]!.size, true);
			parts.push(header);
			for (const [slotIdx, buf] of walSlots[s]!) {
				const entry = new Uint8Array(4 + slotSize);
				new DataView(entry.buffer).setUint32(0, slotIdx, true);
				entry.set(buf, 4);
				parts.push(entry);
			}
		}

		const totalSize = 1 + parts.reduce((a, p) => a + p.length, 0);
		const walBuf = new Uint8Array(totalSize);
		walBuf[0] = shardCount;
		let pos = 1;
		for (const p of parts) {
			walBuf.set(p, pos);
			pos += p.length;
		}

		await Deno.writeFile(walPath, walBuf, { create: true });

		const wal = await getWAL();
		if (!wal) throw new Error("Failed to read WAL after write");
		self.wal = wal;
		return wal;
	}

	async function getWAL(): Promise<WAL | null> {
		if (!await exists(walPath)) return null;
		return {
			async apply(): Promise<void> {
				const buf = await Deno.readFile(walPath);
				const view = new DataView(buf.buffer);
				let pos = 0;
				const shardCount = buf[pos++]!;
				for (let si = 0; si < shardCount; si++) {
					const s = buf[pos++]!;
					const entryCount = view.getUint32(pos, true);
					pos += 4;
					const shard = shards[s]!;
					for (let e = 0; e < entryCount; e++) {
						const slotIdx = view.getUint32(pos, true);
						pos += 4;
						const slotBuf = buf.subarray(pos, pos + slotSize);
						pos += slotSize;
						await writeSlotDirect(s, slotIdx, slotBuf);
						if (slotBuf[0] === OCCUPIED_LIVE) shard.liveCount++;
					}
					const shardDir = join(path, `shard_${s}`);
					await writeMeta(join(shardDir, "meta.bin"), shard.slotCount, shard.liveCount);
				}
				for (let s = 0; s < NUM_SHARDS; s++) staged[s]!.clear();
			},
			async discard(): Promise<void> {
				self.wal = null;
				for (let s = 0; s < NUM_SHARDS; s++) staged[s]!.clear();
				await Deno.remove(walPath).catch(() => {});
			},
		};
	}

	const self: KVStore<K, V> = {
		name,
		wal: await getWAL(),
		get,
		getMany,
		clear,
		close,
		transaction,
		createWAL,
	};

	return self;
}

// --- helpers ---

function shardIndex(keyBytes: Uint8Array): number {
	return keyBytes[0]!;
}

function slotHash(keyBytes: Uint8Array, slotCount: number): number {
	let h = 2166136261;
	for (let i = 0; i < keyBytes.length; i++) {
		h ^= keyBytes[i]!;
		h = (Math.imul(h, 16777619)) >>> 0;
	}
	return h % slotCount;
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
	return true;
}

async function writeMeta(metaPath: string, slotCount: number, liveCount: number): Promise<void> {
	const buf = new Uint8Array(META_SIZE);
	const view = new DataView(buf.buffer);
	view.setUint32(0, slotCount, true);
	view.setUint32(4, liveCount, true);
	await Deno.writeFile(metaPath, buf, { create: true });
}
