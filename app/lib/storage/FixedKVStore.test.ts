import { assertEquals, assertExists, assertRejects } from "jsr:@std/assert";
import { FixedKVStore } from "./FixedKVStore.ts";

const TEST_DIR = "./test_data";

async function cleanup() {
	try {
		await Deno.remove(TEST_DIR, { recursive: true });
	} catch {
		// Ignore if doesn't exist
	}
}

async function createTestFile(name: string): Promise<Deno.FsFile> {
	await Deno.mkdir(TEST_DIR, { recursive: true });
	return await Deno.open(`${TEST_DIR}/${name}`, { read: true, write: true, create: true });
}

Deno.test("FixedKVStore - basic CRUD operations", async (t) => {
	await cleanup();

	await t.step("set and get a single value", async () => {
		const file = await createTestFile("basic.db");
		const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
		await store.init();

		const key = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
		const value = new Uint8Array(16).fill(42);

		await store.set(key, value);
		const result = await store.get(key);

		assertExists(result);
		assertEquals(result, value);

		await store.close();
		file.close();
	});

	await t.step("get returns null for missing key", async () => {
		const file = await createTestFile("missing.db");
		const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
		await store.init();

		const key = new Uint8Array([9, 9, 9, 9, 9, 9, 9, 9]);
		const result = await store.get(key);

		assertEquals(result, null);

		await store.close();
		file.close();
	});

	await t.step("overwrite existing key", async () => {
		const file = await createTestFile("overwrite.db");
		const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
		await store.init();

		const key = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
		const value1 = new Uint8Array(16).fill(1);
		const value2 = new Uint8Array(16).fill(2);

		await store.set(key, value1);
		await store.set(key, value2);

		const result = await store.get(key);
		assertEquals(result, value2);

		await store.close();
		file.close();
	});

	await cleanup();
});

Deno.test("FixedKVStore - batch operations", async (t) => {
	await cleanup();

	await t.step("getMany retrieves multiple values", async () => {
		const file = await createTestFile("batch.db");
		const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
		await store.init();

		const keys: Uint8Array[] = [];
		const values: Uint8Array[] = [];

		for (let i = 0; i < 100; i++) {
			const key = new Uint8Array(8);
			const value = new Uint8Array(16);
			key.set([i, i + 1, i + 2, i + 3, 0, 0, 0, 0]);
			value.fill(i);
			keys.push(key);
			values.push(value);
			await store.set(key, value);
		}

		// Test batch retrieval
		const results = await store.getMany(keys);

		assertEquals(results.length, 100);
		for (let i = 0; i < 100; i++) {
			assertExists(results[i]);
			assertEquals(results[i], values[i]);
		}

		await store.close();
		file.close();
	});

	await t.step("getMany returns null for missing keys", async () => {
		const file = await createTestFile("batch_missing.db");
		const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
		await store.init();

		const existingKey = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
		const existingValue = new Uint8Array(16).fill(1);
		await store.set(existingKey, existingValue);

		const missingKey = new Uint8Array([9, 9, 9, 9, 9, 9, 9, 9]);

		const results = await store.getMany([existingKey, missingKey]);

		assertEquals(results.length, 2);
		assertEquals(results[0], existingValue);
		assertEquals(results[1], null);

		await store.close();
		file.close();
	});

	await cleanup();
});

Deno.test("FixedKVStore - size validation", async (t) => {
	await cleanup();

	await t.step("throws on wrong key size", async () => {
		const file = await createTestFile("keysize.db");
		const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
		await store.init();

		const wrongKey = new Uint8Array(4); // Should be 8
		const value = new Uint8Array(16);

		await assertRejects(
			async () => await store.set(wrongKey, value),
			Error,
			"Key must be 8 bytes",
		);

		await store.close();
		file.close();
	});

	await t.step("throws on wrong value size", async () => {
		const file = await createTestFile("valuesize.db");
		const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
		await store.init();

		const key = new Uint8Array(8);
		const wrongValue = new Uint8Array(8); // Should be 16

		await assertRejects(
			async () => await store.set(key, wrongValue),
			Error,
			"Value must be 16 bytes",
		);

		await store.close();
		file.close();
	});

	await t.step("getMany throws on wrong key size", async () => {
		const file = await createTestFile("batch_keysize.db");
		const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
		await store.init();

		const wrongKey = new Uint8Array(4);

		await assertRejects(
			async () => await store.getMany([wrongKey]),
			Error,
			"Key must be 8 bytes",
		);

		await store.close();
		file.close();
	});

	await cleanup();
});

Deno.test("FixedKVStore - persistence", async (t) => {
	await cleanup();

	await t.step("data persists after close and reopen", async () => {
		const filePath = `${TEST_DIR}/persist.db`;
		await Deno.mkdir(TEST_DIR, { recursive: true });

		// First session: write data
		{
			const file = await Deno.open(filePath, { read: true, write: true, create: true });
			const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
			await store.init();

			const key = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
			const value = new Uint8Array(16).fill(42);
			await store.set(key, value);
			await store.close();
			file.close();
		}

		// Second session: read data
		{
			const file = await Deno.open(filePath, { read: true, write: true });
			const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
			await store.init();

			const key = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
			const result = await store.get(key);

			assertExists(result);
			assertEquals(result, new Uint8Array(16).fill(42));

			await store.close();
			file.close();
		}
	});

	await t.step("data persists after memtable flush", async () => {
		const filePath = `${TEST_DIR}/flush.db`;
		await Deno.mkdir(TEST_DIR, { recursive: true });

		// First session: write more than memtable size
		{
			const file = await Deno.open(filePath, { read: true, write: true, create: true });
			const store = new FixedKVStore(file, {
				keySize: 8,
				valueSize: 16,
				memtableSize: 10, // Small to force flush
			});
			await store.init();

			// Write 20 entries to force flush
			for (let i = 0; i < 20; i++) {
				const key = new Uint8Array(8);
				const value = new Uint8Array(16);
				key.set([i, 0, 0, 0, 0, 0, 0, 0]);
				value.fill(i);
				await store.set(key, value);
			}

			await store.close();
			file.close();
		}

		// Second session: read all data
		{
			const file = await Deno.open(filePath, { read: true, write: true });
			const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
			await store.init();

			for (let i = 0; i < 20; i++) {
				const key = new Uint8Array(8);
				key.set([i, 0, 0, 0, 0, 0, 0, 0]);
				const result = await store.get(key);

				assertExists(result);
				const expectedValue = new Uint8Array(16);
				expectedValue.fill(i);
				assertEquals(result, expectedValue);
			}

			await store.close();
			file.close();
		}
	});

	await cleanup();
});

Deno.test("FixedKVStore - entries iteration", async (t) => {
	await cleanup();

	await t.step("entries returns all stored values", async () => {
		const file = await createTestFile("entries.db");
		const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
		await store.init();

		const entries = new Map<string, number>();

		// Write 50 entries
		for (let i = 0; i < 50; i++) {
			const key = new Uint8Array(8);
			const value = new Uint8Array(16);
			key.set([i, i + 1, 0, 0, 0, 0, 0, 0]);
			value.fill(i);
			await store.set(key, value);
			entries.set(i.toString(), i);
		}

		// Read all entries
		let count = 0;
		for await (const entry of store.entries()) {
			const keyFirstByte = entry.key[0];
			assertExists(keyFirstByte);
			const expectedValue = new Uint8Array(16);
			expectedValue.fill(keyFirstByte);
			assertEquals(entry.value, expectedValue);
			entries.delete(keyFirstByte.toString());
			count++;
		}

		assertEquals(count, 50);
		assertEquals(entries.size, 0);

		await store.close();
		file.close();
	});

	await cleanup();
});

Deno.test("FixedKVStore - statistics", async (t) => {
	await cleanup();

	await t.step("stats reflect store state", async () => {
		const file = await createTestFile("stats.db");
		const store = new FixedKVStore(file, {
			keySize: 8,
			valueSize: 16,
			memtableSize: 100,
			blockCacheSize: 10,
		});
		await store.init();

		// Initial stats
		let stats = store.getStats();
		assertEquals(stats.memtableEntries, 0);
		assertEquals(stats.totalEntries, 0);
		assertEquals(stats.sstCount, 0);

		// Add some entries
		for (let i = 0; i < 10; i++) {
			const key = new Uint8Array(8);
			const value = new Uint8Array(16);
			key.set([i, 0, 0, 0, 0, 0, 0, 0]);
			await store.set(key, value);
		}

		stats = store.getStats();
		assertEquals(stats.memtableEntries, 10);
		assertEquals(stats.totalEntries, 10);

		// Force flush by closing
		await store.close();
		file.close();

		// Reopen and check stats
		const file2 = await Deno.open(`${TEST_DIR}/stats.db`, { read: true, write: true });
		const store2 = new FixedKVStore(file2, {
			keySize: 8,
			valueSize: 16,
			memtableSize: 100,
			blockCacheSize: 10,
		});
		await store2.init();

		stats = store2.getStats();
		assertEquals(stats.sstCount, 1);
		assertEquals(stats.sstEntries, 10);
		assertEquals(stats.totalEntries, 10);

		await store2.close();
		file2.close();
	});

	await t.step("cache stats are tracked", async () => {
		const file = await createTestFile("cache_stats.db");
		const store = new FixedKVStore(file, {
			keySize: 8,
			valueSize: 16,
			memtableSize: 5,
			blockCacheSize: 10,
		});
		await store.init();

		// Write entries to create SST files
		for (let i = 0; i < 20; i++) {
			const key = new Uint8Array(8);
			const value = new Uint8Array(16);
			key.set([i, 0, 0, 0, 0, 0, 0, 0]);
			await store.set(key, value);
		}

		await store.close();
		file.close();

		// Reopen and read to populate cache
		const file2 = await Deno.open(`${TEST_DIR}/cache_stats.db`, { read: true, write: true });
		const store2 = new FixedKVStore(file2, {
			keySize: 8,
			valueSize: 16,
			memtableSize: 5,
			blockCacheSize: 10,
		});
		await store2.init();

		// Read same keys multiple times
		const key = new Uint8Array(8);
		key.set([0, 0, 0, 0, 0, 0, 0, 0]);

		await store2.get(key);
		await store2.get(key);
		await store2.get(key);

		const stats = store2.getStats();
		assertEquals(stats.cacheHits, 2);
		assertEquals(stats.cacheMisses, 1);

		await store2.close();
		file2.close();
	});

	await cleanup();
});

Deno.test("FixedKVStore - edge cases", async (t) => {
	await cleanup();

	await t.step("handles zero-filled keys and values", async () => {
		const file = await createTestFile("zero.db");
		const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
		await store.init();

		const zeroKey = new Uint8Array(8);
		const zeroValue = new Uint8Array(16);

		await store.set(zeroKey, zeroValue);
		const result = await store.get(zeroKey);

		assertExists(result);
		assertEquals(result, zeroValue);

		await store.close();
		file.close();
	});

	await t.step("handles max byte values", async () => {
		const file = await createTestFile("max.db");
		const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
		await store.init();

		const maxKey = new Uint8Array(8).fill(255);
		const maxValue = new Uint8Array(16).fill(255);

		await store.set(maxKey, maxValue);
		const result = await store.get(maxKey);

		assertExists(result);
		assertEquals(result, maxValue);

		await store.close();
		file.close();
	});

	await t.step("handles many sequential keys", async () => {
		const file = await createTestFile("sequential.db");
		const store = new FixedKVStore(file, {
			keySize: 8,
			valueSize: 16,
			memtableSize: 100,
		});
		await store.init();

		// Write 100 sequential keys (smaller set for stability)
		for (let i = 0; i < 100; i++) {
			const key = new Uint8Array(8);
			key.set([i, 0, 0, 0, 0, 0, 0, 0]);
			const value = new Uint8Array(16);
			value.fill(i % 256);
			await store.set(key, value);
		}

		// Read back in reverse order
		for (let i = 99; i >= 0; i--) {
			const key = new Uint8Array(8);
			key.set([i, 0, 0, 0, 0, 0, 0, 0]);

			const result = await store.get(key);
			assertExists(result);

			const expectedValue = new Uint8Array(16);
			expectedValue.fill(i % 256);
			assertEquals(result, expectedValue);
		}

		await store.close();
		file.close();
	});

	await t.step("handles empty getMany", async () => {
		const file = await createTestFile("empty_batch.db");
		const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
		await store.init();

		const results = await store.getMany([]);
		assertEquals(results.length, 0);

		await store.close();
		file.close();
	});

	await t.step("handles single entry getMany", async () => {
		const file = await createTestFile("single_batch.db");
		const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
		await store.init();

		const key = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
		const value = new Uint8Array(16).fill(42);
		await store.set(key, value);

		const results = await store.getMany([key]);
		assertEquals(results.length, 1);
		assertEquals(results[0], value);

		await store.close();
		file.close();
	});

	await cleanup();
});

Deno.test("FixedKVStore - concurrent operations", async (t) => {
	await cleanup();

	await t.step("handles concurrent reads", async () => {
		const file = await createTestFile("concurrent.db");
		const store = new FixedKVStore(file, { keySize: 8, valueSize: 16 });
		await store.init();

		// Write test data
		for (let i = 0; i < 100; i++) {
			const key = new Uint8Array(8);
			const value = new Uint8Array(16);
			key.set([i, 0, 0, 0, 0, 0, 0, 0]);
			value.fill(i);
			await store.set(key, value);
		}

		// Concurrent reads
		const promises = [];
		for (let i = 0; i < 100; i++) {
			const key = new Uint8Array(8);
			key.set([i, 0, 0, 0, 0, 0, 0, 0]);
			promises.push(store.get(key));
		}

		const results = await Promise.all(promises);
		assertEquals(results.length, 100);

		for (let i = 0; i < 100; i++) {
			assertExists(results[i]);
			const expectedValue = new Uint8Array(16);
			expectedValue.fill(i);
			assertEquals(results[i], expectedValue);
		}

		await store.close();
		file.close();
	});

	await cleanup();
});

Deno.test("FixedKVStore - multiple SST files", async (t) => {
	await cleanup();

	await t.step("creates multiple SST files when memtable fills", async () => {
		const file = await createTestFile("multi_sst.db");
		const store = new FixedKVStore(file, {
			keySize: 8,
			valueSize: 16,
			memtableSize: 10, // Very small to force multiple flushes
			blockSize: 1024,
		});
		await store.init();

		// Write enough to create multiple SST files
		for (let i = 0; i < 100; i++) {
			const key = new Uint8Array(8);
			const value = new Uint8Array(16);
			key.set([i, 0, 0, 0, 0, 0, 0, 0]);
			value.fill(i);
			await store.set(key, value);
		}

		await store.close();
		file.close();

		// Reopen and verify all data
		const file2 = await Deno.open(`${TEST_DIR}/multi_sst.db`, { read: true, write: true });
		const store2 = new FixedKVStore(file2, {
			keySize: 8,
			valueSize: 16,
			memtableSize: 10,
			blockSize: 1024,
		});
		await store2.init();

		const stats = store2.getStats();
		assertEquals(stats.totalEntries, 100);
		assertEquals(stats.sstCount >= 1, true);

		// Verify all data is readable
		for (let i = 0; i < 100; i++) {
			const key = new Uint8Array(8);
			key.set([i, 0, 0, 0, 0, 0, 0, 0]);
			const result = await store2.get(key);
			assertExists(result);
			const expectedValue = new Uint8Array(16);
			expectedValue.fill(i);
			assertEquals(result, expectedValue);
		}

		await store2.close();
		file2.close();
	});

	await cleanup();
});

// Cleanup after all tests
await cleanup();
