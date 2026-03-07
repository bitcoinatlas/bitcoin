import { assertEquals } from "@std/assert";
import { FixedKVStore, FixedKVStoreOptions } from "~/lib/storage/FixedKVStore.ts";

// Simple fixed-size codec for testing
class FixedBytesCodec {
	readonly stride: number;

	constructor(size: number) {
		this.stride = size;
	}

	encode(value: Uint8Array): Uint8Array {
		if (value.length !== this.stride) {
			throw new Error(`Expected ${this.stride} bytes, got ${value.length}`);
		}
		return value;
	}

	decode(data: Uint8Array): [Uint8Array, number] {
		if (data.length < this.stride) {
			throw new Error(`Expected at least ${this.stride} bytes, got ${data.length}`);
		}
		return [data.slice(0, this.stride), this.stride];
	}
}

const KEY_SIZE = 16;
const VALUE_SIZE = 64;
const KEY_CODEC = new FixedBytesCodec(KEY_SIZE);
const VALUE_CODEC = new FixedBytesCodec(VALUE_SIZE);

const DEFAULT_OPTIONS: FixedKVStoreOptions<Uint8Array, Uint8Array> = {
	codecs: [KEY_CODEC, VALUE_CODEC],
};

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
	const store = new FixedKVStore(dataPath, DEFAULT_OPTIONS);

	try {
		await store.prepare();
		return await testFn(store);
	} finally {
		await store.close().catch(() => {});
		await Deno.remove(testDir, { recursive: true }).catch(() => {});
	}
}

// Basic operations
Deno.test("FixedKVStore - basic set and get", async () => {
	await withStore(async (store) => {
		const key = createKey(1);
		const value = createValue(1);

		await store.set(key, value);
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

Deno.test("FixedKVStore - setMany and getMany", async () => {
	await withStore(async (store) => {
		const entries = [
			{ key: createKey(1), value: createValue(1) },
			{ key: createKey(2), value: createValue(2) },
			{ key: createKey(3), value: createValue(3) },
		];

		await store.setMany(entries);
		const results = await store.getMany(entries.map((e) => e.key));

		assertEquals(results, entries.map((e) => e.value));
	});
});

Deno.test("FixedKVStore - getMany with missing keys", async () => {
	await withStore(async (store) => {
		await store.set(createKey(1), createValue(1));

		const results = await store.getMany([createKey(1), createKey(999)]);

		assertEquals(results[0], createValue(1));
		assertEquals(results[1], undefined);
	});
});

// Overwrites
Deno.test("FixedKVStore - overwrites return latest value", async () => {
	await withStore(async (store) => {
		const key = createKey(1);

		await store.set(key, createValue(1));
		await store.set(key, createValue(2));
		await store.set(key, createValue(3));

		const got = await store.get(key);
		assertEquals(got, createValue(3));
	});
});

Deno.test("FixedKVStore - batch overwrites", async () => {
	await withStore(async (store) => {
		const key = createKey(1);

		await store.setMany([{ key, value: createValue(1) }]);
		await store.setMany([{ key, value: createValue(2) }]);

		const got = await store.get(key);
		assertEquals(got, createValue(2));
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
		const store1 = new FixedKVStore(dataPath, DEFAULT_OPTIONS);
		await store1.prepare();
		await store1.set(key1, value1);
		await store1.set(key2, value2);
		await store1.close();

		// Phase 2: Read data after reopen
		const store2 = new FixedKVStore(dataPath, DEFAULT_OPTIONS);
		await store2.prepare();

		const got1 = await store2.get(key1);
		const got2 = await store2.get(key2);

		assertEquals(got1, value1);
		assertEquals(got2, value2);

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

		await store.set(key, zeroValue);
		const got = await store.get(key);

		assertEquals(got, zeroValue);
	});
});

Deno.test("FixedKVStore - handles many keys", async () => {
	await withStore(async (store) => {
		const count = 1000;
		const entries = [];

		for (let i = 0; i < count; i++) {
			entries.push({ key: createKey(i), value: createValue(i) });
		}

		await store.setMany(entries);

		// Verify all can be retrieved
		for (let i = 0; i < count; i++) {
			const got = await store.get(createKey(i));
			assertEquals(got, createValue(i), `Mismatch at key ${i}`);
		}
	});
});

// Custom sizes
Deno.test("FixedKVStore - supports custom key/value sizes", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;

	const customOptions: FixedKVStoreOptions<Uint8Array, Uint8Array> = {
		codecs: [new FixedBytesCodec(32), new FixedBytesCodec(256)],
	};

	const store = new FixedKVStore(dataPath, customOptions);
	await store.prepare();

	try {
		const key = createKey(1, 32);
		const value = createValue(1, 256);

		await store.set(key, value);
		const got = await store.get(key);

		assertEquals(got, value);
	} finally {
		await store.close();
		await Deno.remove(testDir, { recursive: true });
	}
});

// Error handling
Deno.test("FixedKVStore - throws on wrong key size", async () => {
	await withStore(async (store) => {
		const wrongKey = new Uint8Array(KEY_SIZE + 1);

		let error: Error | undefined;
		try {
			await store.set(wrongKey, createValue(1));
		} catch (e) {
			error = e as Error;
		}

		assertEquals(error?.message.includes("Expected 16 bytes"), true);
	});
});

Deno.test("FixedKVStore - throws on wrong value size", async () => {
	await withStore(async (store) => {
		const wrongValue = new Uint8Array(VALUE_SIZE + 1);

		let error: Error | undefined;
		try {
			await store.set(createKey(1), wrongValue);
		} catch (e) {
			error = e as Error;
		}

		assertEquals(error?.message.includes("Expected 64 bytes"), true);
	});
});

// Empty batch operations
Deno.test("FixedKVStore - handles empty setMany", async () => {
	await withStore(async (store) => {
		await store.setMany([]);
		const got = await store.get(createKey(1));
		assertEquals(got, undefined);
	});
});

Deno.test("FixedKVStore - handles empty getMany", async () => {
	await withStore(async (store) => {
		await store.set(createKey(1), createValue(1));
		const results = await store.getMany([]);
		assertEquals(results, []);
	});
});

// Concurrent operations
Deno.test("FixedKVStore - handles concurrent sets", async () => {
	await withStore(async (store) => {
		const promises = [];
		for (let i = 0; i < 100; i++) {
			promises.push(store.set(createKey(i), createValue(i)));
		}
		await Promise.all(promises);

		for (let i = 0; i < 100; i++) {
			const got = await store.get(createKey(i));
			assertEquals(got, createValue(i), `Mismatch at key ${i}`);
		}
	});
});

Deno.test("FixedKVStore - handles concurrent gets", async () => {
	await withStore(async (store) => {
		await store.setMany(Array.from({ length: 100 }, (_, i) => ({
			key: createKey(i),
			value: createValue(i),
		})));

		const promises = [];
		for (let i = 0; i < 100; i++) {
			promises.push(store.get(createKey(i)));
		}
		const results = await Promise.all(promises);

		for (let i = 0; i < 100; i++) {
			assertEquals(results[i], createValue(i), `Mismatch at key ${i}`);
		}
	});
});

// Double prepare/close calls
Deno.test("FixedKVStore - handles double prepare", async () => {
	await withStore(async (store) => {
		await store.prepare();
		await store.set(createKey(1), createValue(1));
		const got = await store.get(createKey(1));
		assertEquals(got, createValue(1));
	});
});

Deno.test("FixedKVStore - handles double close", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;
	const store = new FixedKVStore(dataPath, DEFAULT_OPTIONS);

	try {
		await store.prepare();
		await store.set(createKey(1), createValue(1));
		await store.close();
		await store.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});
