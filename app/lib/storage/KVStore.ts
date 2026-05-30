import { FixedCodec } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import { Database } from "@db/sqlite";
import type { Batch, Store, WAL } from "~/lib/storage/Store.ts";
import { Uint8ArrayMap } from "~/lib/Uint8ArrayMap.ts";

/**
 * A persistent key-value store backed by N SQLite databases (shards).
 *
 * Keys are routed to shards by `keyBytes[0] % shardCount`. Default shard
 * count is 16; override with `options.shards`. Shard count is persisted in
 * `meta.json` so reopening with the wrong value is caught at startup.
 *
 * Each shard is a separate SQLite file (`shard-{i}.db`) with schema:
 *   CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)
 *
 * All shards run in rollback-journal mode (DELETE) so they do not interfere
 * with our own WAL protocol.
 *
 * WAL file format: [u32 entryCount LE]([keyBytes][valueBytes])...
 * Both key and value are fixed-size (keyStride / valueStride bytes) — no
 * per-entry length prefix needed. Shard routing is re-derived from keyBytes[0]
 * at apply time, so no shard index is stored in the WAL.
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
	/** Number of SQLite shard files. Must be 1–256. Default: 16. */
	shards?: number;
};

type ShardDB = {
	db: Database;
	stmtGet: ReturnType<Database["prepare"]>;
	stmtInsert: ReturnType<Database["prepare"]>;
	stmtClear: ReturnType<Database["prepare"]>;
};

export async function createKVStore<K, V>(options: KVStoreOptions<K, V>): Promise<KVStore<K, V>> {
	const { name, path, keyCodec, valueCodec } = options;
	const shardCount = options.shards ?? 16;

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

	// Committed on batch.apply(), flushed to disk on createWAL()
	const staged = new Uint8ArrayMap<Uint8Array>(256);

	function shardOf(keyBytes: Uint8Array): ShardDB {
		return shards[(keyBytes[0] as number) % shardCount]!;
	}

	function getByBytes(keyBytes: Uint8Array, stagedPairs: Uint8ArrayMap<Uint8Array> | null): V | undefined {
		if (stagedPairs) {
			const s = stagedPairs.get(keyBytes);
			if (s !== undefined) return valueCodec.decode(s)[0];
		}
		const row = shardOf(keyBytes).stmtGet.get<{ value: Uint8Array }>(keyBytes);
		if (!row) return undefined;
		return valueCodec.decode(row.value)[0];
	}

	async function get(key: K): Promise<V | undefined> {
		return getByBytes(keyCodec.encode(key), staged);
	}

	async function getMany(keys: K[]): Promise<(V | undefined)[]> {
		return keys.map((k) => getByBytes(keyCodec.encode(k), staged));
	}

	async function clear(): Promise<void> {
		if (self.wal) throw new Error("Can't clear while WAL is in progress");
		if (batch) throw new Error("Can't clear while batch is in progress");
		staged.clear();
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

		const batchStaged = new Uint8ArrayMap<Uint8Array>(64);

		batch = {
			async get(key: K): Promise<V | undefined> {
				const keyBytes = keyCodec.encode(key);
				const b = batchStaged.get(keyBytes);
				if (b !== undefined) return valueCodec.decode(b)[0];
				return getByBytes(keyBytes, staged);
			},
			async getMany(keys: K[]): Promise<(V | undefined)[]> {
				return Promise.all(keys.map((k) => batch!.get(k)));
			},
			set(key: K, value: V): void {
				batchStaged.set(keyCodec.encode(key), valueCodec.encode(value));
			},
			apply(): void {
				for (const [k, v] of batchStaged) staged.set(k, v);
				batchStaged.clear();
				batch = null;
			},
			discard(): void {
				batchStaged.clear();
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
		let pos = 4;
		for (let i = 0; i < entryCount; i++) {
			const s = (buffer[pos] as number) % shardCount;
			const shard = shards[s]!;
			if (!active[s]) {
				shard.db.exec("BEGIN");
				active[s] = 1;
			}
			shard.stmtInsert.run(
				buffer.subarray(pos, pos + keyStride),
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

		// Serialize staged entries: [u32 entryCount LE]([keyBytes][valueBytes])...
		const entryCount = staged.size;
		const buf = new Uint8Array(4 + entryCount * entryStride);
		new DataView(buf.buffer).setUint32(0, entryCount, true);
		let pos = 4;
		for (const [keyBytes, valueBytes] of staged) {
			buf.set(keyBytes, pos);
			buf.set(valueBytes, pos + keyStride);
			pos += entryStride;
		}

		await Deno.writeFile(walPath, buf, { create: true });

		const wal = makeWAL(buf);
		self.wal = wal;
		return wal;
	}

	function makeWAL(buffer: Uint8Array): WAL {
		return {
			async apply(): Promise<void> {
				applyBuffer(buffer);
				staged.clear();
				self.wal = null;
			},
			async discard(): Promise<void> {
				self.wal = null;
				staged.clear();
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
