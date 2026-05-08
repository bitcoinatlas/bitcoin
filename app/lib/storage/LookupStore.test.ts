import { BytesCodec } from "@nomadshiba/codec";
import { assertEquals, assertThrows } from "@std/assert";
import { createLookupStore, type LookupStore } from "~/lib/storage/LookupStore.ts";

const KEY_SIZE = 16;
const VALUE_SIZE = 32;
const KEY_CODEC = new BytesCodec({ size: KEY_SIZE });
const VALUE_CODEC = new BytesCodec({ size: VALUE_SIZE });

function makeKey(n: number): Uint8Array {
	const k = new Uint8Array(KEY_SIZE);
	new DataView(k.buffer).setUint32(0, n, true);
	return k;
}

function makeValue(n: number): Uint8Array {
	const v = new Uint8Array(VALUE_SIZE);
	for (let i = 0; i < VALUE_SIZE; i++) v[i] = (n * 31 + i) % 256;
	return v;
}

async function withStore<T>(
	testFn: (store: LookupStore<Uint8Array, Uint8Array>) => Promise<T>,
): Promise<T> {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	const store = await createLookupStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
	try {
		return await testFn(store);
	} finally {
		store.close();
		await Deno.remove(dir, { recursive: true }).catch(() => {});
	}
}

// Basic operations

Deno.test("LookupStore - set and get", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.apply();
		assertEquals(await store.get(makeKey(1)), makeValue(1));
	});
});

Deno.test("LookupStore - get returns undefined for missing key", async () => {
	await withStore(async (store) => {
		assertEquals(await store.get(makeKey(999)), undefined);
	});
});

Deno.test("LookupStore - getMany returns correct values", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.set(makeKey(2), makeValue(2));
		tx.set(makeKey(3), makeValue(3));
		tx.apply();

		const results = await store.getMany([makeKey(1), makeKey(2), makeKey(3)]);
		assertEquals(results[0], makeValue(1));
		assertEquals(results[1], makeValue(2));
		assertEquals(results[2], makeValue(3));
	});
});

Deno.test("LookupStore - getMany with missing keys returns undefined", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.apply();

		const results = await store.getMany([makeKey(1), makeKey(999)]);
		assertEquals(results[0], makeValue(1));
		assertEquals(results[1], undefined);
	});
});

Deno.test("LookupStore - overwrite returns latest value", async () => {
	await withStore(async (store) => {
		const key = makeKey(1);
		const tx1 = store.transaction();
		tx1.set(key, makeValue(1));
		tx1.apply();

		const tx2 = store.transaction();
		tx2.set(key, makeValue(2));
		tx2.apply();

		assertEquals(await store.get(key), makeValue(2));
	});
});

Deno.test("LookupStore - last write wins within same tx", async () => {
	await withStore(async (store) => {
		const key = makeKey(1);
		const tx = store.transaction();
		tx.set(key, makeValue(1));
		tx.set(key, makeValue(2));
		tx.set(key, makeValue(3));
		tx.apply();
		assertEquals(await store.get(key), makeValue(3));
	});
});

Deno.test("LookupStore - discard throws away staged changes", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.discard();
		assertEquals(await store.get(makeKey(1)), undefined);
	});
});

Deno.test("LookupStore - tx.get sees staged writes before apply", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.set(makeKey(1), makeValue(1));
		assertEquals(await tx.get(makeKey(1)), makeValue(1));
		assertEquals(await store.get(makeKey(1)), undefined);
		tx.apply();
		assertEquals(await store.get(makeKey(1)), makeValue(1));
	});
});

Deno.test("LookupStore - tx.getMany sees staged writes", async () => {
	await withStore(async (store) => {
		const tx1 = store.transaction();
		tx1.set(makeKey(1), makeValue(1));
		tx1.apply();

		const tx2 = store.transaction();
		tx2.set(makeKey(2), makeValue(2));

		const results = await tx2.getMany([makeKey(1), makeKey(2), makeKey(999)]);
		assertEquals(results[0], makeValue(1));  // from store
		assertEquals(results[1], makeValue(2));  // staged in tx
		assertEquals(results[2], undefined);
		tx2.discard();
	});
});

Deno.test("LookupStore - second transaction throws while one is open", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		assertThrows(() => store.transaction());
		tx.discard();
	});
});

Deno.test("LookupStore - handles many keys", async () => {
	await withStore(async (store) => {
		const count = 500;
		const tx = store.transaction();
		for (let i = 0; i < count; i++) tx.set(makeKey(i), makeValue(i));
		tx.apply();

		for (let i = 0; i < count; i++) {
			assertEquals(await store.get(makeKey(i)), makeValue(i), `Mismatch at key ${i}`);
		}
	});
});

// WAL + persistence

Deno.test("LookupStore - WAL save and apply persists to disk", async () => {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	try {
		const store1 = await createLookupStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		const tx = store1.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.set(makeKey(2), makeValue(2));
		tx.apply();

		const wal = await store1.WAL();
		await wal.save();
		await wal.apply();

		const store2 = await createLookupStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		assertEquals(await store2.get(makeKey(1)), makeValue(1));
		assertEquals(await store2.get(makeKey(2)), makeValue(2));
		store1.close();
		store2.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("LookupStore - WAL lookup by id returns null if not found", async () => {
	await withStore(async (store) => {
		const wal = await store.WAL({ id: "nonexistent-id" });
		assertEquals(wal, null);
	});
});

Deno.test("LookupStore - WAL apply is idempotent (overwrite same key twice)", async () => {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	try {
		const store1 = await createLookupStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		const tx = store1.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.apply();

		const wal = await store1.WAL();
		await wal.save();
		await wal.apply();
		await wal.apply(); // second apply — same key, same value, no error

		const store2 = await createLookupStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		assertEquals(await store2.get(makeKey(1)), makeValue(1));
		store1.close();
		store2.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("LookupStore - crash recovery: WAL apply replays changes", async () => {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	try {
		const store1 = await createLookupStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		const tx = store1.transaction();
		tx.set(makeKey(42), makeValue(42));
		tx.apply();

		const wal = await store1.WAL();
		await wal.save();
		// crash before apply
		store1.close();

		const store2 = await createLookupStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		const recovered = await store2.WAL({ id: wal.id });
		assertEquals(recovered !== null, true);
		await recovered!.apply();

		assertEquals(await store2.get(makeKey(42)), makeValue(42));
		store2.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("LookupStore - persists data across reopen", async () => {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	try {
		const store1 = await createLookupStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		const tx = store1.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.set(makeKey(2), makeValue(2));
		tx.apply();
		const wal = await store1.WAL();
		await wal.save();
		await wal.apply();
		await wal.discard();
		store1.close();

		const store2 = await createLookupStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		assertEquals(await store2.get(makeKey(1)), makeValue(1));
		assertEquals(await store2.get(makeKey(2)), makeValue(2));
		store2.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});
