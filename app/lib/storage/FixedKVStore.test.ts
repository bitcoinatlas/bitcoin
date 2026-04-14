import { BytesCodec } from "@nomadshiba/codec";
import { assertEquals, assertRejects, assertThrows } from "@std/assert";
import { FixedKVStore } from "~/lib/storage/FixedKVStore.ts";

const KEY_SIZE = 16;
const VALUE_SIZE = 64;
const KEY_CODEC = new BytesCodec({ size: KEY_SIZE });
const VALUE_CODEC = new BytesCodec({ size: VALUE_SIZE });

const DEFAULT_CODECS = [KEY_CODEC, VALUE_CODEC] as const;

// Test helpers
function createKey(n: number, size = KEY_SIZE): Uint8Array {
	const key = new Uint8Array(size);
	for (let i = 0; i < size; i++) {
		key[i] = (n >> (i * 8)) & 0xFF;
	}
	return key;
}

function createValue(n: number, size = VALUE_SIZE): Uint8Array {
	const value = new Uint8Array(size);
	for (let i = 0; i < size; i++) {
		value[i] = (n * 31 + i) % 256;
	}
	return value;
}

async function withStore<T>(
	testFn: (store: FixedKVStore<Uint8Array, Uint8Array>) => Promise<T>,
): Promise<T> {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;
	const store = new FixedKVStore(dataPath, DEFAULT_CODECS);

	try {
		await store.prepare();
		return await testFn(store);
	} finally {
		await store.close().catch(() => {});
		await Deno.remove(testDir, { recursive: true }).catch(() => {});
	}
}

/** Commit and finalize a transaction in one step. */
async function commitAndFinalize(store: FixedKVStore<any, any>, fn: (tx: ReturnType<typeof store.transaction>) => void): Promise<void> {
	const tx = store.transaction();
	fn(tx);
	await tx.commit();
	await store.finalize();
}

// Basic operations
Deno.test("FixedKVStore - basic set and get", async () => {
	await withStore(async (store) => {
		const key = createKey(1);
		const value = createValue(1);

		await commitAndFinalize(store, (tx) => tx.set(key, value));
		const got = await store.get(key);

		assertEquals(got, value);
	});
});

Deno.test("FixedKVStore - get returns undefined for missing keys", async () => {
	await withStore(async (store) => {
		const result = await store.get(createKey(999));
		assertEquals(result, undefined);
	});
});

Deno.test("FixedKVStore - set multiple and getMany", async () => {
	await withStore(async (store) => {
		const entries = [
			{ key: createKey(1), value: createValue(1) },
			{ key: createKey(2), value: createValue(2) },
			{ key: createKey(3), value: createValue(3) },
		];

		await commitAndFinalize(store, (tx) => {
			for (const { key, value } of entries) tx.set(key, value);
		});
		const results = await store.getMany(entries.map((e) => e.key));

		assertEquals(results, entries.map((e) => e.value));
	});
});

Deno.test("FixedKVStore - getMany with missing keys", async () => {
	await withStore(async (store) => {
		await commitAndFinalize(store, (tx) => tx.set(createKey(1), createValue(1)));

		const results = await store.getMany([createKey(1), createKey(999)]);

		assertEquals(results[0], createValue(1));
		assertEquals(results[1], undefined);
	});
});

// Overwrites
Deno.test("FixedKVStore - overwrites return latest value", async () => {
	await withStore(async (store) => {
		const key = createKey(1);

		await commitAndFinalize(store, (tx) => tx.set(key, createValue(1)));
		await commitAndFinalize(store, (tx) => tx.set(key, createValue(2)));
		await commitAndFinalize(store, (tx) => tx.set(key, createValue(3)));

		const got = await store.get(key);
		assertEquals(got, createValue(3));
	});
});

// Transaction reads staged ops before finalize
Deno.test("FixedKVStore - tx.get sees staged writes before finalize", async () => {
	await withStore(async (store) => {
		const key = createKey(1);
		const value = createValue(1);

		const tx = store.transaction();
		tx.set(key, value);

		// Not finalized yet — store.get returns undefined, tx.get returns value
		assertEquals(await store.get(key), undefined);
		assertEquals(await tx.get(key), value);

		await tx.commit();
		await store.finalize();

		assertEquals(await store.get(key), value);
	});
});

// Rollback
Deno.test("FixedKVStore - rollback discards staged ops", async () => {
	await withStore(async (store) => {
		const key = createKey(1);

		const tx = store.transaction();
		tx.set(key, createValue(1));
		tx.rollback();

		assertEquals(await store.get(key), undefined);
	});
});

// Only one tx at a time
Deno.test("FixedKVStore - second transaction throws while one is open", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		assertThrows(() => store.transaction());
		tx.rollback();
	});
});

// Persistence
Deno.test("FixedKVStore - persists data across reopen", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;

	const value1 = createValue(1);
	const value2 = createValue(2);
	const key1 = createKey(1);
	const key2 = createKey(2);

	try {
		// Phase 1: Write data
		const store1 = new FixedKVStore(dataPath, DEFAULT_CODECS);
		await store1.prepare();
		await commitAndFinalize(store1, (tx) => { tx.set(key1, value1); tx.set(key2, value2); });
		await store1.close();

		// Phase 2: Read data after reopen
		const store2 = new FixedKVStore(dataPath, DEFAULT_CODECS);
		await store2.prepare();

		assertEquals(await store2.get(key1), value1);
		assertEquals(await store2.get(key2), value2);

		await store2.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

// Crash recovery: WAL applied on next finalize()
Deno.test("FixedKVStore - crash recovery replays WAL on next open", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;
	const key = createKey(42);
	const value = createValue(42);

	try {
		// Phase 1: commit (WAL written) but do NOT finalize (simulate crash)
		const store1 = new FixedKVStore(dataPath, DEFAULT_CODECS);
		await store1.prepare();
		const tx = store1.transaction();
		tx.set(key, value);
		await tx.commit();
		await store1.close(); // crash — WAL still on disk

		// Phase 2: reopen, finalize() should replay WAL
		const store2 = new FixedKVStore(dataPath, DEFAULT_CODECS);
		await store2.finalize(); // crash recovery

		assertEquals(await store2.get(key), value);
		await store2.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

// Edge cases
Deno.test("FixedKVStore - handles zero value", async () => {
	await withStore(async (store) => {
		const zeroValue = new Uint8Array(VALUE_SIZE);
		const key = createKey(1);

		await commitAndFinalize(store, (tx) => tx.set(key, zeroValue));
		assertEquals(await store.get(key), zeroValue);
	});
});

Deno.test("FixedKVStore - handles many keys", async () => {
	await withStore(async (store) => {
		const count = 1000;
		await commitAndFinalize(store, (tx) => {
			for (let i = 0; i < count; i++) tx.set(createKey(i), createValue(i));
		});

		for (let i = 0; i < count; i++) {
			assertEquals(await store.get(createKey(i)), createValue(i), `Mismatch at key ${i}`);
		}
	});
});

Deno.test("FixedKVStore - handles empty transaction", async () => {
	await withStore(async (store) => {
		await commitAndFinalize(store, (_tx) => {});
		assertEquals(await store.get(createKey(1)), undefined);
	});
});

Deno.test("FixedKVStore - handles empty getMany", async () => {
	await withStore(async (store) => {
		await commitAndFinalize(store, (tx) => tx.set(createKey(1), createValue(1)));
		assertEquals(await store.getMany([]), []);
	});
});

// tx.getMany staged visibility
Deno.test("FixedKVStore - tx.getMany sees staged writes", async () => {
	await withStore(async (store) => {
		await commitAndFinalize(store, (tx) => tx.set(createKey(1), createValue(1)));

		const tx = store.transaction();
		tx.set(createKey(2), createValue(2));

		const results = await tx.getMany([createKey(1), createKey(2), createKey(999)]);
		assertEquals(results[0], createValue(1));  // already in store
		assertEquals(results[1], createValue(2));  // staged in tx
		assertEquals(results[2], undefined);        // never existed

		tx.rollback();
	});
});

// commit() twice throws
Deno.test("FixedKVStore - commit twice throws", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.set(createKey(1), createValue(1));
		await tx.commit();
		await assertRejects(() => tx.commit());
		await store.finalize();
	});
});

// finalize() is idempotent
Deno.test("FixedKVStore - finalize is idempotent", async () => {
	await withStore(async (store) => {
		await commitAndFinalize(store, (tx) => tx.set(createKey(1), createValue(1)));
		await store.finalize(); // second call — no WAL, no tx, should be no-op
		assertEquals(await store.get(createKey(1)), createValue(1));
	});
});

// overwrite within same tx
Deno.test("FixedKVStore - last write wins within same tx", async () => {
	await withStore(async (store) => {
		const key = createKey(1);
		await commitAndFinalize(store, (tx) => {
			tx.set(key, createValue(1));
			tx.set(key, createValue(2));
			tx.set(key, createValue(3));
		});
		assertEquals(await store.get(key), createValue(3));
	});
});

// Double prepare/close
Deno.test("FixedKVStore - handles double prepare", async () => {
	await withStore(async (store) => {
		await store.prepare();
		await commitAndFinalize(store, (tx) => tx.set(createKey(1), createValue(1)));
		assertEquals(await store.get(createKey(1)), createValue(1));
	});
});

Deno.test("FixedKVStore - handles double close", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;
	const store = new FixedKVStore(dataPath, DEFAULT_CODECS);

	try {
		await store.prepare();
		await commitAndFinalize(store, (tx) => tx.set(createKey(1), createValue(1)));
		await store.close();
		await store.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});
