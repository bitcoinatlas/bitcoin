/**
 * KvStore test with Bytes32 key — the exact codec used in chain.ts for txid/pubkey.
 *
 * Catches the bug where Bytes32.encode ignored the `target` parameter,
 * causing every RocksDB key to be all-zeros and every lookup to return
 * the same entry.
 */
import { assertEquals } from "@std/assert";
import { RocksDatabase } from "@harperfast/rocksdb-js";
import { join } from "@std/path";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { StoredTxPointer } from "~/codec/stored/StoredTxPointer.ts";
import { KvStore } from "./KvStore.ts";

async function withStore(fn: (store: KvStore<typeof Bytes32, typeof StoredTxPointer>) => Promise<void>): Promise<void> {
	const dir = await Deno.makeTempDir({ prefix: "kvstore_bytes32_" });
	const rocksdb = RocksDatabase.open(join(dir, "rocks"), { disableWAL: true, parallelismThreads: 1 });
	const store = await KvStore.open({
		rocksdb,
		prefix: new Uint8Array([0]),
		key: Bytes32,
		value: StoredTxPointer,
	});
	try {
		await fn(store);
	} finally {
		rocksdb.close();
		await Deno.remove(dir, { recursive: true }).catch(() => {});
	}
}

Deno.test("KvStore with Bytes32 key: two different keys return different values", async () => {
	// This is the exact scenario that broke: txid store with Bytes32 key.
	// If Bytes32.encode ignores target, both keys encode as all-zeros
	// and both lookups return the same value.
	await withStore(async (store) => {
		const keyA = new Uint8Array(32).fill(0xaa);
		const keyB = new Uint8Array(32).fill(0xbb);
		const valA = 42;
		const valB = 99;

		const b = store.batch();
		b.set(keyA, valA);
		b.set(keyB, valB);
		b.apply();

		const gotA = await store.get(keyA);
		const gotB = await store.get(keyB);

		assertEquals(gotA, valA, "key A returned wrong value — Bytes32.encode may ignore target");
		assertEquals(gotB, valB, "key B returned wrong value — Bytes32.encode may ignore target");
	});
});

Deno.test("KvStore with Bytes32 key: absent key returns undefined", async () => {
	await withStore(async (store) => {
		const keyA = new Uint8Array(32).fill(0xcc);
		const keyB = new Uint8Array(32).fill(0xdd);

		const b = store.batch();
		b.set(keyA, 123);
		b.apply();

		const gotB = await store.get(keyB);
		assertEquals(gotB, undefined, "absent key should return undefined, not another key's value");
	});
});

Deno.test("KvStore with Bytes32 key: batch.get reads own uncommitted sets correctly", async () => {
	await withStore(async (store) => {
		const keyA = new Uint8Array(32).fill(0x11);
		const keyB = new Uint8Array(32).fill(0x22);

		const b = store.batch();
		b.set(keyA, 10);
		b.set(keyB, 20);

		const gotA = await b.get(keyA);
		const gotB = await b.get(keyB);

		assertEquals(gotA, 10, "batch.get key A wrong");
		assertEquals(gotB, 20, "batch.get key B wrong");

		b.discard();
	});
});

Deno.test("KvStore with Bytes32 key: overwrite same key, last value wins", async () => {
	await withStore(async (store) => {
		const key = new Uint8Array(32).fill(0x77);

		const b = store.batch();
		b.set(key, 1);
		b.set(key, 2);
		b.set(key, 3);
		b.apply();

		assertEquals(await store.get(key), 3, "last set should win");
	});
});

Deno.test("KvStore with Bytes32 key: flush persists and survives reopen", async () => {
	// Full durability test with Bytes32 keys.
	const dir = await Deno.makeTempDir({ prefix: "kvstore_bytes32_persist_" });
	const rocksPath = join(dir, "rocks");

	const keyA = new Uint8Array(32).fill(0xaa);
	const keyB = new Uint8Array(32).fill(0xbb);

	{
		const rocksdb = RocksDatabase.open(rocksPath, { disableWAL: true, parallelismThreads: 1 });
		const store = await KvStore.open({
			rocksdb,
			prefix: new Uint8Array([0]),
			key: Bytes32,
			value: StoredTxPointer,
		});
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
		const store = await KvStore.open({
			rocksdb,
			prefix: new Uint8Array([0]),
			key: Bytes32,
			value: StoredTxPointer,
		});
		try {
			assertEquals(await store.get(keyA), 100, "key A lost after reopen");
			assertEquals(await store.get(keyB), 200, "key B lost after reopen");
		} finally {
			rocksdb.close();
		}
	}

	await Deno.remove(dir, { recursive: true }).catch(() => {});
});
