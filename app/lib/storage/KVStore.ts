import { Database } from "@db/sqlite";
import { FixedCodec } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import type { Batch, Store, WAL } from "~/lib/storage/Store.ts";
import { Uint8ArrayMap } from "~/lib/Uint8ArrayMap.ts";

function fnv1a32(bytes: Uint8Array): number {
	let h = 0x811c9dc5;
	for (let i = 0; i < bytes.length; i++) {
		h ^= bytes[i]!;
		h = Math.imul(h, 0x01000193) >>> 0;
	}
	return h;
}

/**
 * A persistent key-value store backed by N SQLite databases (shards).
 *
 * Keys are routed to shards by `fnv1a32(keyBytes) % shardCount`. Default shard
 * count is 16; override with `options.shards`. Shard count is persisted in
 * `meta.json` so reopening with the wrong value is caught at startup.
 *
 * Each shard is a separate SQLite file (`shard-{i}.db`) with schema:
 *   CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)
 *
 * All shards run in rollback-journal mode (DELETE) so they do not interfere
 * with our own WAL protocol.
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
	/** Number of SQLite shard files. Must be 1–256. */
	shards: number;
};

type ShardDB = {
	db: Database;
	stmtGet: ReturnType<Database["prepare"]>;
	stmtInsert: ReturnType<Database["prepare"]>;
	stmtClear: ReturnType<Database["prepare"]>;
};

const WAL_HEADER = 4; // bytes reserved at the front of the WAL buffer for the u32 entryCount

export async function createKVStore<K, V>(options: KVStoreOptions<K, V>): Promise<KVStore<K, V>> {
	const { name, path, keyCodec, valueCodec } = options;
	const shardCount = options.shards;

	if (shardCount < 1 || shardCount > 256 || !Number.isInteger(shardCount)) {
		throw new Error(`shards must be an integer 1–256, got ${shardCount}`);
	}

	const keyStride = keyCodec.stride.size;
	const valueStride = valueCodec.stride.size;
	const entryStride = keyStride + valueStride;

	await Deno.mkdir(path, { recursive: true });

	// Persist shard count in meta.json so reopening with a different value is caught.
	const metaPath = join(path, "meta.json");
	if (await exists(metaPath)) {
		const meta = JSON.parse(await Deno.readTextFile(metaPath)) as { shards: number };
		if (meta.shards !== shardCount) {
			throw new Error(
				`KVStore at ${path} was created with shards=${meta.shards}, ` +
					`but reopened with shards=${shardCount}`,
			);
		}
	} else {
		await Deno.writeTextFile(metaPath, JSON.stringify({ shards: shardCount }));
	}

	const walPath = join(path, "data.wal");

	function openShard(i: number): ShardDB {
		const db = new Database(join(path, `shard-${i}.db`));
		db.exec(`CREATE TABLE IF NOT EXISTS kv (key BLOB PRIMARY KEY, value BLOB)`);
		db.exec(`PRAGMA journal_mode=DELETE`);
		db.exec(`PRAGMA synchronous=NORMAL`);
		// 16MB cache per shard (× 16 shards = 256MB total, same as before).
		db.exec(`PRAGMA cache_size=-16384`);
		const stmtGet = db.prepare(`SELECT value FROM kv WHERE key = ?`);
		const stmtInsert = db.prepare(`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`);
		const stmtClear = db.prepare(`DELETE FROM kv`);
		return { db, stmtGet, stmtInsert, stmtClear };
	}

	const shards: ShardDB[] = Array.from({ length: shardCount }, (_, i) => openShard(i));

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

	function shardIndexOf(keyBytes: Uint8Array): number {
		return fnv1a32(keyBytes) % shardCount;
	}

	function shardOf(keyBytes: Uint8Array): ShardDB {
		return shards[shardIndexOf(keyBytes)]!;
	}

	function getByBytes(
		keyBytes: Uint8Array,
		batchBufRef: Uint8Array | null,
		batchIdx: Uint8ArrayMap<number> | null,
	): V | undefined {
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
		// 3. SQLite
		const row = shardOf(keyBytes).stmtGet.get<{ value: Uint8Array }>(keyBytes);
		if (!row) return undefined;
		return valueCodec.decode(row.value)[0];
	}

	async function get(key: K): Promise<V | undefined> {
		return getByBytes(keyCodec.encode(key), null, null);
	}

	async function getMany(keys: K[]): Promise<(V | undefined)[]> {
		return keys.map((k) => getByBytes(keyCodec.encode(k), null, null));
	}

	async function clear(): Promise<void> {
		if (self.wal) throw new Error("Can't clear while WAL is in progress");
		if (batch) throw new Error("Can't clear while batch is in progress");
		stagedMap.clear();
		pendingBuf = new Uint8Array(entryStride * 64);
		pendingCount = 0;
		for (const shard of shards) shard.stmtClear.run();
	}

	function close(): void {
		if (self.wal) throw new Error("Can't close while WAL is in progress");
		for (const shard of shards) shard.db.close();
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

	function applyBuffer(buffer: Uint8Array): void {
		const view = new DataView(buffer.buffer, buffer.byteOffset);
		const entryCount = view.getUint32(0, true);
		if (entryCount === 0) return;

		// One pass: open transactions lazily per shard, insert via subarray views, commit all.
		const active = new Uint8Array(shardCount); // 1 = transaction open for this shard
		let pos = WAL_HEADER;
		for (let i = 0; i < entryCount; i++) {
			const keySlice = buffer.subarray(pos, pos + keyStride);
			const s = shardIndexOf(keySlice);
			const shard = shards[s]!;
			if (!active[s]) {
				shard.db.exec("BEGIN");
				active[s] = 1;
			}
			shard.stmtInsert.run(
				keySlice,
				buffer.subarray(pos + keyStride, pos + entryStride),
			);
			pos += entryStride;
		}

		for (let s = 0; s < shardCount; s++) {
			if (active[s]) shards[s]!.db.exec("COMMIT");
		}
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
				applyBuffer(buffer);
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
