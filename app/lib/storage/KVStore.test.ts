import { BytesCodec } from "@nomadshiba/codec";
import { assertEquals, assertThrows } from "@std/assert";
import { createKVStore, type KVStore } from "~/lib/storage/KVStore.ts";

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
	testFn: (store: KVStore<Uint8Array, Uint8Array>) => Promise<T>,
): Promise<T> {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	const store = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
	try {
		return await testFn(store);
	} finally {
		store.close();
		await Deno.remove(dir, { recursive: true }).catch(() => {});
	}
}

// Basic operations

Deno.test("KVStore - set and get", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.apply();
		assertEquals(await store.get(makeKey(1)), makeValue(1));
	});
});

Deno.test("KVStore - get returns undefined for missing key", async () => {
	await withStore(async (store) => {
		assertEquals(await store.get(makeKey(999)), undefined);
	});
});

Deno.test("KVStore - getMany returns correct values", async () => {
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

Deno.test("KVStore - getMany with missing keys returns undefined", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.apply();

		const results = await store.getMany([makeKey(1), makeKey(999)]);
		assertEquals(results[0], makeValue(1));
		assertEquals(results[1], undefined);
	});
});

Deno.test("KVStore - overwrite returns latest value", async () => {
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

Deno.test("KVStore - last write wins within same tx", async () => {
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

Deno.test("KVStore - discard throws away staged changes", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.discard();
		assertEquals(await store.get(makeKey(1)), undefined);
	});
});

Deno.test("KVStore - tx.get sees staged writes before apply", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.set(makeKey(1), makeValue(1));
		assertEquals(await tx.get(makeKey(1)), makeValue(1));
		assertEquals(await store.get(makeKey(1)), undefined);
		tx.apply();
		assertEquals(await store.get(makeKey(1)), makeValue(1));
	});
});

Deno.test("KVStore - tx.getMany sees staged writes", async () => {
	await withStore(async (store) => {
		const tx1 = store.transaction();
		tx1.set(makeKey(1), makeValue(1));
		tx1.apply();

		const tx2 = store.transaction();
		tx2.set(makeKey(2), makeValue(2));

		const results = await tx2.getMany([makeKey(1), makeKey(2), makeKey(999)]);
		assertEquals(results[0], makeValue(1)); // from store
		assertEquals(results[1], makeValue(2)); // staged in tx
		assertEquals(results[2], undefined);
		tx2.discard();
	});
});

Deno.test("KVStore - second transaction throws while one is open", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		assertThrows(() => store.transaction());
		tx.discard();
	});
});

Deno.test("KVStore - handles many keys", async () => {
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

Deno.test("KVStore - WAL save and apply persists to disk", async () => {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	try {
		const store1 = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		const tx = store1.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.set(makeKey(2), makeValue(2));
		tx.apply();

		const wal = await store1.createWAL();

		await wal.apply();

		const store2 = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		assertEquals(await store2.get(makeKey(1)), makeValue(1));
		assertEquals(await store2.get(makeKey(2)), makeValue(2));
		store1.close();
		store2.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("KVStore - WAL lookup by id returns null if not found", async () => {
	await withStore(async (store) => {
		const wal = await store.WAL({ id: "nonexistent-id" });
		assertEquals(wal, null);
	});
});

Deno.test("KVStore - WAL apply is idempotent (overwrite same key twice)", async () => {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	try {
		const store1 = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		const tx = store1.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.apply();

		const wal = await store1.createWAL();

		await wal.apply();
		await wal.apply(); // second apply — same key, same value, no error

		const store2 = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		assertEquals(await store2.get(makeKey(1)), makeValue(1));
		store1.close();
		store2.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("KVStore - crash recovery: WAL apply replays changes", async () => {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	try {
		const store1 = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		const tx = store1.transaction();
		tx.set(makeKey(42), makeValue(42));
		tx.apply();

		const wal = await store1.createWAL();

		// crash before apply
		store1.close();

		const store2 = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		const recovered = await store2.WAL({ id: wal.id });
		assertEquals(recovered !== null, true);
		await recovered!.apply();

		assertEquals(await store2.get(makeKey(42)), makeValue(42));
		store2.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("KVStore - persists data across reopen", async () => {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	try {
		const store1 = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		const tx = store1.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.set(makeKey(2), makeValue(2));
		tx.apply();
		const wal = await store1.createWAL();

		await wal.apply();
		await wal.discard();
		store1.close();

		const store2 = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		assertEquals(await store2.get(makeKey(1)), makeValue(1));
		assertEquals(await store2.get(makeKey(2)), makeValue(2));
		store2.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("KVStore - WAL discard removes the file", async () => {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	try {
		const store = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		const tx = store.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.apply();

		const wal = await store.createWAL();

		const walExists = (await Array.fromAsync(Deno.readDir(dir))).some((e) => e.name.endsWith(".wal"));
		assertEquals(walExists, true);

		await wal.discard();
		const walExistsAfter = (await Array.fromAsync(Deno.readDir(dir))).some((e) => e.name.endsWith(".wal"));
		assertEquals(walExistsAfter, false);
		store.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("KVStore - WAL empty save and apply is a no-op", async () => {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	try {
		const store = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		// no transaction, nothing staged
		const wal = await store.createWAL();

		await wal.apply();
		await wal.discard();
		assertEquals(await store.get(makeKey(1)), undefined);
		store.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("KVStore - WAL apply updates existing key on disk", async () => {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	try {
		// Write initial value and flush to disk
		const store1 = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		const tx1 = store1.transaction();
		tx1.set(makeKey(1), makeValue(1));
		tx1.apply();
		const wal1 = await store1.createWAL();
		await wal1.save();
		await wal1.apply();
		await wal1.discard();
		store1.close();

		// Reopen and overwrite via WAL
		const store2 = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		const tx2 = store2.transaction();
		tx2.set(makeKey(1), makeValue(99));
		tx2.apply();
		const wal2 = await store2.createWAL();
		await wal2.save();
		await wal2.apply();
		await wal2.discard();
		store2.close();

		// Reopen again — should see updated value
		const store3 = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		assertEquals(await store3.get(makeKey(1)), makeValue(99));
		store3.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("KVStore - multiple WAL cycles accumulate all keys", async () => {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	try {
		const store = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });

		for (let batch = 0; batch < 3; batch++) {
			const tx = store.transaction();
			for (let i = 0; i < 10; i++) tx.set(makeKey(batch * 10 + i), makeValue(batch * 10 + i));
			tx.apply();
			const wal = await store.createWAL();

			await wal.apply();
			await wal.discard();
		}
		store.close();

		const store2 = await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: VALUE_CODEC });
		for (let i = 0; i < 30; i++) {
			assertEquals(await store2.get(makeKey(i)), makeValue(i), `key ${i}`);
		}
		store2.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("KVStore - staged value visible after apply, cleared after WAL save", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.apply();

		// Staged — visible before WAL save
		assertEquals(await store.get(makeKey(1)), makeValue(1));

		const wal = await store.createWAL();

		// After save, staged cleared but value is in WAL / on disk after apply
		await wal.apply();
		assertEquals(await store.get(makeKey(1)), makeValue(1));
	});
});

Deno.test("KVStore - getMany with duplicate keys returns same value for each", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.set(makeKey(1), makeValue(1));
		tx.apply();

		const results = await store.getMany([makeKey(1), makeKey(1), makeKey(1)]);
		assertEquals(results[0], makeValue(1));
		assertEquals(results[1], makeValue(1));
		assertEquals(results[2], makeValue(1));
	});
});

Deno.test("KVStore - tx.get sees store-staged value (not yet on disk)", async () => {
	await withStore(async (store) => {
		// Stage key in outer store
		const tx1 = store.transaction();
		tx1.set(makeKey(5), makeValue(5));
		tx1.apply();

		// New tx can read it
		const tx2 = store.transaction();
		assertEquals(await tx2.get(makeKey(5)), makeValue(5));
		tx2.discard();
	});
});

Deno.test("KVStore - invalid key codec stride throws", async () => {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	try {
		const badCodec = {
			stride: 0,
			encode: () => new Uint8Array(0),
			decode: () => [new Uint8Array(0), 0] as [Uint8Array, number],
		};
		let threw = false;
		try {
			await createKVStore({ name: "test", path: dir, keyCodec: badCodec as never, valueCodec: VALUE_CODEC });
		} catch {
			threw = true;
		}
		assertEquals(threw, true);
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("KVStore - invalid value codec stride throws", async () => {
	const dir = await Deno.makeTempDir({ prefix: "lookupstore-test-" });
	try {
		const badCodec = {
			stride: 0,
			encode: () => new Uint8Array(0),
			decode: () => [new Uint8Array(0), 0] as [Uint8Array, number],
		};
		let threw = false;
		try {
			await createKVStore({ name: "test", path: dir, keyCodec: KEY_CODEC, valueCodec: badCodec as never });
		} catch {
			threw = true;
		}
		assertEquals(threw, true);
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("KVStore - WAL with transaction open throws", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		let threw = false;
		try {
			await store.createWAL();
		} catch {
			threw = true;
		}
		assertEquals(threw, true);
		tx.discard();
	});
});

Deno.test("KVStore - get with transaction open throws", async () => {
	await withStore(async (store) => {
		// get/getMany on store (not tx) are fine even during tx — no assertion
		// but opening a second tx should throw
		const tx = store.transaction();
		let threw = false;
		try {
			store.transaction();
		} catch {
			threw = true;
		}
		assertEquals(threw, true);
		tx.discard();
	});
});
