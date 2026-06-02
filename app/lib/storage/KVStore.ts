import { RocksDatabase, Store as RocksStore } from "@harperfast/rocksdb-js";
import { FixedCodec } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import type { Batch, Store, WAL } from "~/lib/storage/Store.ts";
import { Uint8ArrayMap } from "~/lib/Uint8ArrayMap.ts";

/**
 * A persistent key-value store backed by RocksDB.
 *
 * Stage layout:
 *
 *   stagedMap: Uint8ArrayMap<Uint8Array>  — key → raw value bytes
 *
 *   Entries are upserted into stagedMap on batch.apply().  Duplicate keys are
 *   overwritten in-place by the map, so no stale entries accumulate and reads
 *   are always O(1) regardless of how many batches have been applied.
 *
 *   pendingBuf: flat Uint8Array  — active batch entries only (reset on apply/discard)
 *   pendingBuf layout: [key0|val0][key1|val1]…  (no header, offset 0)
 *
 * batch.apply()  → upsert pendingBuf entries into stagedMap; reset pendingBuf
 * batch.discard() → reset pendingBuf
 * createWAL()    → serialize stagedMap into a contiguous buffer; write file
 */

export interface KVStore<K, V> extends Store<KVStoreBatch<K, V>> {
	get(key: K): Promise<V | undefined>;
	getMany(keys: K[]): Promise<(V | undefined)[]>;
	clear(): Promise<void>;
	close(): void;
}

export interface KVStoreBatch<K, V> extends Batch {
	get(key: K): Promise<V | undefined>;
	getMany(keys: K[]): Promise<(V | undefined)[]>;
	set(key: K, value: V): void;
}

export type KVStoreOptions<K, V> = {
	name: string;
	path: string;
	keyCodec: FixedCodec<K>;
	valueCodec: FixedCodec<V>;
	/** Kept for backward compatibility; no longer used (RocksDB manages sharding internally). */
	shards?: number;
};

const WAL_HEADER = 4; // bytes reserved at the front of the WAL buffer for the u32 entryCount

/**
 * Custom Store that passes raw binary keys/values straight to RocksDB without
 * any serialization.  The default Store encodes values through its own
 * serializer (msgpack-like), which is expensive and wasteful for Uint8Array
 * payloads that are already encoded by our codecs.
 */
// deno-lint-ignore no-explicit-any
class BinaryStore extends RocksStore {
	override encodeKey(key: Uint8Array): any {
		return key;
	}
	override decodeKey(key: any): Uint8Array {
		return key;
	}
	override encodeValue(value: Uint8Array): any {
		return value;
	}
	override decodeValue(value: any): Uint8Array {
		return value;
	}
}

export async function createKVStore<K, V>(options: KVStoreOptions<K, V>): Promise<KVStore<K, V>> {
	const { name, path, keyCodec, valueCodec } = options;

	const keyStride = keyCodec.stride.size;
	const valueStride = valueCodec.stride.size;
	const entryStride = keyStride + valueStride;

	await Deno.mkdir(path, { recursive: true });

	const walPath = join(path, "data.wal");

	// Use a binary passthrough store so keys/values are stored as raw bytes
	// without any serialization overhead.
	const store = new BinaryStore(join(path, "rocksdb"), {
		// Keep RocksDB's own WAL enabled so memtable writes survive a crash/kill
		// before they are flushed to SST files.  Our data.wal lives at the parent
		// path level, so there is no naming conflict.
		// Allow more background flush/compaction threads.
		parallelismThreads: 4,
	});
	const db = RocksDatabase.open(store);

	// ── Stage: map-based, always O(1) reads ────────────────────────────────────
	//
	// stagedMap holds all committed-but-not-yet-flushed entries as key → value
	// bytes.  Duplicate keys are overwritten in-place on batch.apply(), so the
	// map never accumulates stale entries and no index rebuild is ever needed.

	const stagedMap = new Uint8ArrayMap<Uint8Array>(256);

	// ── Pending buffer: active batch entries only ───────────────────────────────
	//
	// Layout: [key0|val0][key1|val1]…  (offset 0, no header)
	// Reset to a fresh allocation on every apply() / discard().

	let pendingBuf = new Uint8Array(entryStride * 64);
	let pendingCount = 0;

	async function getByBytes(
		keyBytes: Uint8Array,
		batchBufRef: Uint8Array | null,
		batchIdx: Uint8ArrayMap<number> | null,
	): Promise<V | undefined> {
		// 1. Pending batch entries
		if (batchIdx !== null && batchBufRef !== null) {
			const off = batchIdx.get(keyBytes);
			if (off !== undefined) {
				return valueCodec.decode(batchBufRef.subarray(off + keyStride, off + entryStride))[0];
			}
		}
		// 2. Committed staged entries — always O(1), no rebuild needed
		const stagedVal = stagedMap.get(keyBytes);
		if (stagedVal !== undefined) {
			return valueCodec.decode(stagedVal)[0];
		}
		// 3. RocksDB
		const raw = await db.get(keyBytes) as Uint8Array | undefined;
		if (raw == null) return undefined;
		return valueCodec.decode(raw)[0];
	}

	async function get(key: K): Promise<V | undefined> {
		return await getByBytes(keyCodec.encode(key), null, null);
	}

	async function getMany(keys: K[]): Promise<(V | undefined)[]> {
		return await Promise.all(keys.map((k) => getByBytes(keyCodec.encode(k), null, null)));
	}

	async function clear(): Promise<void> {
		if (self.wal) throw new Error("Can't clear while WAL is in progress");
		if (batch) throw new Error("Can't clear while batch is in progress");
		stagedMap.clear();
		pendingBuf = new Uint8Array(entryStride * 64);
		pendingCount = 0;
		await db.clear();
	}

	function close(): void {
		if (self.wal) throw new Error("Can't close while WAL is in progress");
		db.close();
	}

	// --- batch ---

	let batch: KVStoreBatch<K, V> | null = null;

	function batchFn(): KVStoreBatch<K, V> {
		if (batch) throw new Error("Batch already in progress");
		if (self.wal) throw new Error("Can't start batch while WAL is in progress");

		// batchIndex: key → byte offset in pendingBuf.
		// Used for intra-batch get() and in-place value updates on duplicate set().
		const batchIndex = new Uint8ArrayMap<number>(64);

		batch = {
			async get(key: K): Promise<V | undefined> {
				return getByBytes(keyCodec.encode(key), pendingBuf, batchIndex);
			},
			async getMany(keys: K[]): Promise<(V | undefined)[]> {
				return Promise.all(keys.map((k) => batch!.get(k)));
			},
			set(key: K, value: V): void {
				const kBytes = keyCodec.encode(key);
				const vBytes = valueCodec.encode(value);

				// If key already pending in this batch, overwrite value in-place — no new entry.
				const existing = batchIndex.get(kBytes);
				if (existing !== undefined) {
					pendingBuf.set(vBytes, existing + keyStride);
					return;
				}

				// Grow pendingBuf if needed (2× amortised).
				const needed = (pendingCount + 1) * entryStride;
				if (needed > pendingBuf.length) {
					const next = new Uint8Array(Math.max(needed, pendingBuf.length * 2));
					next.set(pendingBuf);
					pendingBuf = next;
				}

				const off = pendingCount * entryStride;
				pendingBuf.set(kBytes, off);
				pendingBuf.set(vBytes, off + keyStride);
				batchIndex.set(kBytes, off);
				pendingCount++;
			},
			apply(): void {
				// Upsert all pending entries into stagedMap.
				// Duplicate keys overwrite in-place — no stale entries accumulate.
				for (let i = 0; i < pendingCount; i++) {
					const off = i * entryStride;
					const k = pendingBuf.subarray(off, off + keyStride);
					const v = pendingBuf.subarray(off + keyStride, off + entryStride);
					stagedMap.set(k, v.slice());
				}
				pendingBuf = new Uint8Array(entryStride * 64);
				pendingCount = 0;
				batchIndex.clear();
				batch = null;
			},
			discard(): void {
				pendingBuf = new Uint8Array(entryStride * 64);
				pendingCount = 0;
				batchIndex.clear();
				batch = null;
			},
		};

		return batch;
	}

	// --- WAL ---

	async function applyBuffer(buffer: Uint8Array): Promise<void> {
		const view = new DataView(buffer.buffer, buffer.byteOffset);
		const entryCount = view.getUint32(0, true);
		if (entryCount === 0) return;

		// Write all entries in a single RocksDB transaction for atomicity.
		await db.transaction(async (txn) => {
			let pos = WAL_HEADER;
			for (let i = 0; i < entryCount; i++) {
				const keySlice = buffer.subarray(pos, pos + keyStride);
				const valSlice = buffer.subarray(pos + keyStride, pos + entryStride);
				await txn.put(keySlice, valSlice);
				pos += entryStride;
			}
		});
	}

	async function createWAL(): Promise<WAL> {
		if (self.wal) throw new Error("WAL already exists");
		if (batch) throw new Error("Can't create WAL while batch is in progress");

		// Serialize stagedMap into a contiguous WAL buffer.
		const entryCount = stagedMap.size;
		const walBuf = new Uint8Array(WAL_HEADER + entryCount * entryStride);
		new DataView(walBuf.buffer).setUint32(0, entryCount, true);
		let pos = WAL_HEADER;
		for (const [k, v] of stagedMap) {
			walBuf.set(k, pos);
			walBuf.set(v, pos + keyStride);
			pos += entryStride;
		}

		await Deno.writeFile(walPath, walBuf, { create: true });

		const wal = makeWAL(walBuf);
		self.wal = wal;
		return wal;
	}

	function makeWAL(buffer: Uint8Array): WAL {
		return {
			async apply(): Promise<void> {
				await applyBuffer(buffer);
				stagedMap.clear();
				pendingBuf = new Uint8Array(entryStride * 64);
				pendingCount = 0;
				self.wal = null;
			},
			async discard(): Promise<void> {
				self.wal = null;
				stagedMap.clear();
				pendingBuf = new Uint8Array(entryStride * 64);
				pendingCount = 0;
				await Deno.remove(walPath).catch(() => {});
			},
		};
	}

	async function getWAL(): Promise<WAL | null> {
		if (!await exists(walPath)) return null;
		const buf = await Deno.readFile(walPath);
		return makeWAL(buf);
	}

	const self: KVStore<K, V> = {
		name,
		wal: await getWAL(),
		get,
		getMany,
		clear,
		close,
		batch: batchFn,
		createWAL,
	};

	return self;
}
