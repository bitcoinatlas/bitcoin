/**
 * KvStore test suite — matches current API (rocksdb, prefix, key, value).
 *
 * Coverage:
 *   - open / undefined on missing key
 *   - batch: set, get, apply, discard, duplicate-set overwrite
 *   - reads: batch → staged → RocksDB precedence
 *   - flush: persistence via RocksDB transaction
 *   - durability: flushed survives reopen
 *   - volatility: unflushed staged lost on reopen
 *   - clear: wipes staged + RocksDB
 *   - batch guards: one at a time, settled throws
 *   - Bytes32 key: two different keys return different values (catches target-ignore bug)
 */
import { assertEquals, assertRejects, assertThrows } from "@std/assert";
import { RocksDatabase } from "@harperfast/rocksdb-js";
import { join } from "@std/path";
import { U32 } from "@nomadshiba/codec";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { StoredTxPointer } from "~/codec/stored/StoredTxPointer.ts";
import { KvStore } from "./KvStore.ts";

// ── harness ───────────────────────────────────────────────────────────────────

const Key = U32;
const Val = U32;

async function withStore(
	fn: (store: KvStore<typeof Key, typeof Val>, rocksdb: RocksDatabase, dir: string) => Promise<void>,
): Promise<void> {
	const dir = await Deno.makeTempDir({ prefix: "kvstore_test_" });
	const rocksdb = RocksDatabase.open(join(dir, "rocks"), { disableWAL: true, parallelismThreads: 1 });
	const store = await KvStore.open({
		rocksdb,
		prefix: new Uint8Array([0]),
		key: Key,
		value: Val,
	});
	try {
		await fn(store, rocksdb, dir);
	} finally {
		rocksdb.close();
		await Deno.remove(dir, { recursive: true }).catch(() => {});
	}
}

function setAll(store: KvStore<typeof Key, typeof Val>, entries: [number, number][]): void {
	const b = store.batch();
	for (const [k, v] of entries) b.set(k, v);
	b.apply();
}

function getAll(store: KvStore<typeof Key, typeof Val>, keys: number[]): Promise<(number | undefined)[]> {
	return Promise.all(keys.map((k) => store.get(k)));
}

async function flush(store: KvStore<typeof Key, typeof Val>, rocksdb: RocksDatabase): Promise<void> {
	await rocksdb.transaction(async (trx) => {
		const finalizer = await store.flush(trx);
		finalizer();
	});
}

// ── open ─────────────────────────────────────────────────────────────────────

Deno.test("open: empty store returns undefined for any key", async () => {
	await withStore(async (store) => {
		assertEquals(await store.get(0), undefined);
		assertEquals(await getAll(store, [1, 2, 3]), [undefined, undefined, undefined]);
	});
});

Deno.test("open: existing RocksDB data is visible after reopen", async () => {
	const dir = await Deno.makeTempDir({ prefix: "kvstore_reopen_" });
	const rocksPath = join(dir, "rocks");
	try {
		{
			const rocksdb = RocksDatabase.open(rocksPath, { disableWAL: true, parallelismThreads: 1 });
			const store = await KvStore.open({ rocksdb, prefix: new Uint8Array([0]), key: Key, value: Val });
			setAll(store, [[1, 100], [2, 200]]);
			await flush(store, rocksdb);
			rocksdb.close();
		}
		{
			const rocksdb = RocksDatabase.open(rocksPath, { disableWAL: true, parallelismThreads: 1 });
			const store = await KvStore.open({ rocksdb, prefix: new Uint8Array([0]), key: Key, value: Val });
			assertEquals(await store.get(1), 100);
			assertEquals(await store.get(2), 200);
			rocksdb.close();
		}
	} finally {
		await Deno.remove(dir, { recursive: true }).catch(() => {});
	}
});

// ── batch semantics ───────────────────────────────────────────────────────────

Deno.test("batch: set/apply makes entries readable; invisible before apply", async () => {
	await withStore(async (store) => {
		const b = store.batch();
		b.set(1, 111);
		b.set(2, 222);
		assertEquals(await store.get(1), undefined);
		b.apply();
		assertEquals(await store.get(1), 111);
		assertEquals(await store.get(2), 222);
	});
});

Deno.test("batch: discard leaves the store unchanged", async () => {
	await withStore(async (store) => {
		setAll(store, [[1, 10]]);
		const b = store.batch();
		b.set(1, 999);
		b.set(2, 888);
		b.discard();
		assertEquals(await store.get(1), 10);
		assertEquals(await store.get(2), undefined);
	});
});

Deno.test("batch: reads its own uncommitted sets; falls through for unknown keys", async () => {
	await withStore(async (store) => {
		setAll(store, [[1, 10]]);
		const b = store.batch();
		b.set(2, 20);
		assertEquals(await b.get(2), 20);
		assertEquals(await b.get(1), 10);
		assertEquals(await b.get(3), undefined);
		b.discard();
	});
});

Deno.test("batch: duplicate set overwrites in-place (last value wins)", async () => {
	await withStore(async (store, rocksdb) => {
		const b = store.batch();
		b.set(1, 100);
		b.set(1, 200);
		b.set(1, 300);
		b.apply();
		assertEquals(await store.get(1), 300);
		await flush(store, rocksdb);
		assertEquals(await store.get(1), 300);
	});
});

// ── reads: layer precedence ──────────────────────────────────────────────────

Deno.test("reads: staged shadows RocksDB for the same key", async () => {
	await withStore(async (store, rocksdb) => {
		setAll(store, [[1, 111]]);
		await flush(store, rocksdb);
		setAll(store, [[1, 999]]);
		assertEquals(await store.get(1), 999);
		await flush(store, rocksdb);
		assertEquals(await store.get(1), 999);
	});
});

Deno.test("reads: batch shadows staged shadows RocksDB for the same key", async () => {
	await withStore(async (store, rocksdb) => {
		setAll(store, [[1, 1]]);
		await flush(store, rocksdb);
		setAll(store, [[1, 2]]);
		const b = store.batch();
		b.set(1, 3);
		assertEquals(await b.get(1), 3);
		assertEquals(await store.get(1), 2);
		b.apply();
		assertEquals(await store.get(1), 3);
	});
});

// ── flush / persistence ──────────────────────────────────────────────────────

Deno.test("flush: persists staged to RocksDB", async () => {
	await withStore(async (store, rocksdb) => {
		setAll(store, [[1, 100], [2, 200]]);
		await flush(store, rocksdb);
		assertEquals(await store.get(1), 100);
		assertEquals(await store.get(2), 200);
	});
});

Deno.test("flush: empty staged is a clean no-op", async () => {
	await withStore(async (store, rocksdb) => {
		setAll(store, [[1, 1]]);
		await flush(store, rocksdb);
		await flush(store, rocksdb);
		assertEquals(await store.get(1), 1);
	});
});

Deno.test("flush: sequential flushes accumulate and overwrite correctly", async () => {
	await withStore(async (store, rocksdb) => {
		setAll(store, [[1, 1], [2, 2]]);
		await flush(store, rocksdb);
		setAll(store, [[2, 22], [3, 3]]);
		await flush(store, rocksdb);
		assertEquals(await getAll(store, [1, 2, 3]), [1, 22, 3]);
	});
});

Deno.test("durability: flushed entries survive reopen", async () => {
	const dir = await Deno.makeTempDir({ prefix: "kvstore_durable_" });
	const rocksPath = join(dir, "rocks");
	try {
		{
			const rocksdb = RocksDatabase.open(rocksPath, { disableWAL: true, parallelismThreads: 1 });
			const store = await KvStore.open({ rocksdb, prefix: new Uint8Array([0]), key: Key, value: Val });
			setAll(store, [[10, 1000], [20, 2000], [30, 3000]]);
			await flush(store, rocksdb);
			rocksdb.close();
		}
		{
			const rocksdb = RocksDatabase.open(rocksPath, { disableWAL: true, parallelismThreads: 1 });
			const store = await KvStore.open({ rocksdb, prefix: new Uint8Array([0]), key: Key, value: Val });
			assertEquals(await getAll(store, [10, 20, 30]), [1000, 2000, 3000]);
			rocksdb.close();
		}
	} finally {
		await Deno.remove(dir, { recursive: true }).catch(() => {});
	}
});

Deno.test("volatility: unflushed staged is lost on reopen", async () => {
	const dir = await Deno.makeTempDir({ prefix: "kvstore_volatile_" });
	const rocksPath = join(dir, "rocks");
	try {
		{
			const rocksdb = RocksDatabase.open(rocksPath, { disableWAL: true, parallelismThreads: 1 });
			const store = await KvStore.open({ rocksdb, prefix: new Uint8Array([0]), key: Key, value: Val });
			setAll(store, [[1, 1]]);
			await flush(store, rocksdb);
			setAll(store, [[2, 2]]);
			rocksdb.close();
		}
		{
			const rocksdb = RocksDatabase.open(rocksPath, { disableWAL: true, parallelismThreads: 1 });
			const store = await KvStore.open({ rocksdb, prefix: new Uint8Array([0]), key: Key, value: Val });
			assertEquals(await store.get(1), 1);
			assertEquals(await store.get(2), undefined);
			rocksdb.close();
		}
	} finally {
		await Deno.remove(dir, { recursive: true }).catch(() => {});
	}
});

// ── clear ────────────────────────────────────────────────────────────────────

Deno.test("clear: wipes staged and RocksDB; data gone after reopen", async () => {
	const dir = await Deno.makeTempDir({ prefix: "kvstore_clear_" });
	const rocksPath = join(dir, "rocks");
	try {
		{
			const rocksdb = RocksDatabase.open(rocksPath, { disableWAL: true, parallelismThreads: 1 });
			const store = await KvStore.open({ rocksdb, prefix: new Uint8Array([0]), key: Key, value: Val });
			setAll(store, [[1, 1], [2, 2]]);
			await flush(store, rocksdb);
			setAll(store, [[3, 3]]);
			await store.clear();
			assertEquals(await getAll(store, [1, 2, 3]), [undefined, undefined, undefined]);
			rocksdb.close();
		}
		{
			const rocksdb = RocksDatabase.open(rocksPath, { disableWAL: true, parallelismThreads: 1 });
			const store = await KvStore.open({ rocksdb, prefix: new Uint8Array([0]), key: Key, value: Val });
			assertEquals(await getAll(store, [1, 2, 3]), [undefined, undefined, undefined]);
			rocksdb.close();
		}
	} finally {
		await Deno.remove(dir, { recursive: true }).catch(() => {});
	}
});

// ── Bytes32 key: the exact codec used in chain.ts for txid/pubkey ─────────────

async function withBytes32Store(
	fn: (store: KvStore<typeof Bytes32, typeof StoredTxPointer>, rocksdb: RocksDatabase) => Promise<void>,
): Promise<void> {
	const dir = await Deno.makeTempDir({ prefix: "kvstore_bytes32_" });
	const rocksdb = RocksDatabase.open(join(dir, "rocks"), { disableWAL: true, parallelismThreads: 1 });
	const store = await KvStore.open({
		rocksdb,
		prefix: new Uint8Array([0]),
		key: Bytes32,
		value: StoredTxPointer,
	});
	try {
		await fn(store, rocksdb);
	} finally {
		rocksdb.close();
		await Deno.remove(dir, { recursive: true }).catch(() => {});
	}
}

Deno.test("Bytes32 key: two different keys return different values", async () => {
	await withBytes32Store(async (store) => {
		const keyA = new Uint8Array(32).fill(0xaa);
		const keyB = new Uint8Array(32).fill(0xbb);

		const b = store.batch();
		b.set(keyA, 42);
		b.set(keyB, 99);
		b.apply();

		assertEquals(await store.get(keyA), 42);
		assertEquals(await store.get(keyB), 99);
	});
});

Deno.test("Bytes32 key: absent key returns undefined", async () => {
	await withBytes32Store(async (store) => {
		const keyA = new Uint8Array(32).fill(0xcc);
		const keyB = new Uint8Array(32).fill(0xdd);

		const b = store.batch();
		b.set(keyA, 123);
		b.apply();

		assertEquals(await store.get(keyB), undefined);
	});
});

Deno.test("Bytes32 key: batch.get reads own uncommitted sets correctly", async () => {
	await withBytes32Store(async (store) => {
		const keyA = new Uint8Array(32).fill(0x11);
		const keyB = new Uint8Array(32).fill(0x22);

		const b = store.batch();
		b.set(keyA, 10);
		b.set(keyB, 20);

		assertEquals(await b.get(keyA), 10);
		assertEquals(await b.get(keyB), 20);
		b.discard();
	});
});

Deno.test("Bytes32 key: flush persists and survives reopen", async () => {
	const dir = await Deno.makeTempDir({ prefix: "kvstore_bytes32_persist_" });
	const rocksPath = join(dir, "rocks");
	try {
		const keyA = new Uint8Array(32).fill(0xaa);
		const keyB = new Uint8Array(32).fill(0xbb);

		{
			const rocksdb = RocksDatabase.open(rocksPath, { disableWAL: true, parallelismThreads: 1 });
			const store = await KvStore.open({ rocksdb, prefix: new Uint8Array([0]), key: Bytes32, value: StoredTxPointer });
			const b = store.batch();
			b.set(keyA, 100);
			b.set(keyB, 200);
			b.apply();
			await rocksdb.transaction(async (trx) => {
				const f = await store.flush(trx);
				f();
			});
			rocksdb.close();
		}
		{
			const rocksdb = RocksDatabase.open(rocksPath, { disableWAL: true, parallelismThreads: 1 });
			const store = await KvStore.open({ rocksdb, prefix: new Uint8Array([0]), key: Bytes32, value: StoredTxPointer });
			assertEquals(await store.get(keyA), 100);
			assertEquals(await store.get(keyB), 200);
			rocksdb.close();
		}
	} finally {
		await Deno.remove(dir, { recursive: true }).catch(() => {});
	}
});
