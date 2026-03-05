import { assertEquals } from "@std/assert";
import { FixedKVStore, FixedKVStoreOptions } from "./FixedKVStore.ts";

// Fixed-size key and value codecs
class FixedBytesCodec {
	readonly stride: number;
	private size: number;

	constructor(size: number) {
		this.stride = size;
		this.size = size;
	}

	encode(value: Uint8Array): Uint8Array {
		if (value.length !== this.size) {
			throw new Error(`Expected ${this.size} bytes, got ${value.length}`);
		}
		return value;
	}

	decode(data: Uint8Array): [Uint8Array, number] {
		if (data.length < this.size) {
			throw new Error(`Expected at least ${this.size} bytes, got ${data.length}`);
		}
		return [data.slice(0, this.size), this.size];
	}
}

// Simple Map wrapper that mimics FixedKVStore behavior
// but uses JavaScript Map for reference correctness
class MapKVStore {
	private map = new Map<string, Uint8Array>();

	constructor(
		private keySize: number,
		private valueSize: number,
	) {}

	private keyToString(key: Uint8Array): string {
		if (key.length !== this.keySize) {
			throw new Error(`Key must be ${this.keySize} bytes`);
		}
		// Use hex string for consistent comparison
		return Array.from(key, (b) => b.toString(16).padStart(2, "0")).join("");
	}

	get(keys: Uint8Array[]): (Uint8Array | undefined)[] {
		return keys.map((key) => {
			const k = this.keyToString(key);
			const value = this.map.get(k);
			return value !== undefined ? new Uint8Array(value) : undefined;
		});
	}

	set(key: Uint8Array, value: Uint8Array): void {
		if (value.length !== this.valueSize) {
			throw new Error(`Value must be ${this.valueSize} bytes`);
		}
		const k = this.keyToString(key);
		// Store a copy to prevent external mutation
		this.map.set(k, new Uint8Array(value));
	}

	close(): Promise<void> {
		return Promise.resolve();
	}

	get size(): number {
		return this.map.size;
	}

	entries(): [Uint8Array, Uint8Array][] {
		const result: [Uint8Array, Uint8Array][] = [];
		for (const [k, v] of this.map) {
			// Convert hex string back to Uint8Array
			const keyBytes = new Uint8Array(this.keySize);
			for (let i = 0; i < this.keySize; i++) {
				keyBytes[i] = parseInt(k.slice(i * 2, i * 2 + 2), 16);
			}
			result.push([keyBytes, new Uint8Array(v)]);
		}
		return result;
	}
}

// Helper to create a test key
function createKey(n: number, size: number): Uint8Array {
	const key = new Uint8Array(size);
	// Fill key with bytes from n (little endian)
	for (let i = 0; i < size; i++) {
		key[i] = (n >> (i * 8)) & 0xFF;
	}
	return key;
}

// Helper to create a test value
function createValue(n: number, size: number): Uint8Array {
	const value = new Uint8Array(size);
	// Fill value with bytes from n
	for (let i = 0; i < size; i++) {
		value[i] = (n * 31 + i) % 256;
	}
	return value;
}

// Helper to compare Uint8Arrays
function arraysEqual(a: Uint8Array, b: Uint8Array | undefined): boolean {
	if (b === undefined) return false;
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) return false;
	}
	return true;
}

const KEY_SIZE = 16;
	const VALUE_SIZE = 64;
const KEY_CODEC = new FixedBytesCodec(KEY_SIZE);
const VALUE_CODEC = new FixedBytesCodec(VALUE_SIZE);

const DEFAULT_OPTIONS: FixedKVStoreOptions<Uint8Array, Uint8Array> = {
	keyCodec: KEY_CODEC as any,
	valueCodec: VALUE_CODEC as any,
	memtableSize: 100,
	blockSize: 4096,
	blockCacheSize: 100,
};

Deno.test("FixedKVStore - basic set and get (single)", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;

	try {
		const file = await Deno.open(dataPath, {
			create: true,
			read: true,
			write: true,
		});
		const store = new FixedKVStore(file, DEFAULT_OPTIONS);
		await store.prepare();

		const refStore = new MapKVStore(16, 64);

		// Test: set some values and verify they can be retrieved
		for (let i = 0; i < 50; i++) {
			const key = createKey(i, 16);
			const value = createValue(i, 64);

			await store.set(key, value);
			refStore.set(key, value);

			// Verify immediate retrieval (single key)
			const got = await store.get(key);
			const [refGot] = refStore.get([key]);

			assertEquals(
				arraysEqual(value, got!),
				true,
				`Value mismatch for key ${i} after set`,
			);
			assertEquals(got, refGot, `Store and reference differ for key ${i}`);
		}

		await store.close();
		await file.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

Deno.test("FixedKVStore - setMany and getMany", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;

	try {
		const file = await Deno.open(dataPath, {
			create: true,
			read: true,
			write: true,
		});
		const store = new FixedKVStore(file, DEFAULT_OPTIONS);
		await store.prepare();

		const refStore = new MapKVStore(16, 64);

		// Test: batch set using setMany
		const entries: Array<{ key: Uint8Array; value: Uint8Array }> = [];
		for (let i = 0; i < 50; i++) {
			const key = createKey(i, 16);
			const value = createValue(i, 64);
			entries.push({ key, value });
			refStore.set(key, value);
		}

		await store.setMany(entries);

		// Verify using getMany
		const keys = entries.map(e => e.key);
		const results = await store.getMany(keys);
		const refResults = refStore.get(keys);

		assertEquals(results.length, keys.length, "Result count mismatch");
		for (let i = 0; i < results.length; i++) {
			assertEquals(
				arraysEqual(results[i]!, refResults[i]!),
				true,
				`Value mismatch for key ${i}`,
			);
		}

		// Also test individual gets
		for (let i = 0; i < 10; i++) {
			const key = createKey(i, 16);
			const got = await store.get(key);
			const [refGot] = refStore.get([key]);
			assertEquals(got, refGot, `Individual get mismatch for key ${i}`);
		}

		await store.close();
		await file.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

Deno.test("FixedKVStore - persistence across reopen", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;

	const refStore = new MapKVStore(16, 64);
	const testKeys: number[] = [];

	try {
		// Phase 1: Create store and add data
		{
			const file = await Deno.open(dataPath, {
				create: true,
				read: true,
				write: true,
			});
			const store = new FixedKVStore(file, DEFAULT_OPTIONS);
			await store.prepare();

			for (let i = 0; i < 200; i++) {
				const key = createKey(i, 16);
				const value = createValue(i, 64);
				testKeys.push(i);

				await store.set(key, value);
				refStore.set(key, value);
			}

			await store.close();
			await file.close();
		}

		// Phase 2: Reopen and verify data persisted
		{
			const file = await Deno.open(dataPath, {
				read: true,
				write: true,
			});
			const store = new FixedKVStore(file, DEFAULT_OPTIONS);
			await store.prepare();

			// Verify all values are still there
			for (const i of testKeys) {
				const key = createKey(i, 16);
				const expected = createValue(i, 64);

				const [got] = await store.get(key);
				const [refGot] = refStore.get([key]);

				assertEquals(
					arraysEqual(expected, got),
					true,
					`Value not persisted for key ${i}`,
				);
				assertEquals(got, refGot, `Store and reference differ after reopen for key ${i}`);
			}

			await store.close();
			await file.close();
		}
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

Deno.test("FixedKVStore - overwrites return latest value", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;

	const refStore = new MapKVStore(16, 64);

	try {
		const file = await Deno.open(dataPath, {
			create: true,
			read: true,
			write: true,
		});
		const store = new FixedKVStore(file, DEFAULT_OPTIONS);
		await store.prepare();

		// Set initial values
		for (let i = 0; i < 50; i++) {
			const key = createKey(i % 10, 16); // Only 10 unique keys
			const value = createValue(i, 64);

			await store.set(key, value);
			refStore.set(key, value);
		}

		// Verify latest values
		for (let i = 0; i < 10; i++) {
			const key = createKey(i, 16);
			const expected = createValue(40 + i, 64); // Last write for each key

			const [got] = await store.get(key);
			const [refGot] = refStore.get([key]);

			assertEquals(
				arraysEqual(expected, got),
				true,
				`Latest value not returned for key ${i}`,
			);
			assertEquals(got, refGot, `Store and reference differ on overwrite for key ${i}`);
		}

		await store.close();
		await file.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

Deno.test("FixedKVStore - batch retrieval", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;

	const refStore = new MapKVStore(16, 64);
	const keys: Uint8Array[] = [];

	try {
		const file = await Deno.open(dataPath, {
			create: true,
			read: true,
			write: true,
		});
		const store = new FixedKVStore(file, DEFAULT_OPTIONS);
		await store.prepare();

		// Set values
		for (let i = 0; i < 100; i++) {
			const key = createKey(i, 16);
			const value = createValue(i, 64);
			keys.push(key);

			await store.set(key, value);
			refStore.set(key, value);
		}

		// Flush to ensure some data is on disk
		await store.close();
		await file.close();

		// Reopen and test batch get
		const file2 = await Deno.open(dataPath, {
			read: true,
			write: true,
		});
		const store2 = new FixedKVStore(file2, DEFAULT_OPTIONS);
		await store2.prepare();

		// Test getMany with all keys
		const results = await store2.get(keys);
		const refResults = refStore.get(keys);

		assertEquals(results.length, keys.length, "Result count mismatch");
		assertEquals(results, refResults, "Batch results differ from reference");

		// Test getMany with subset (including some that don't exist)
		const subsetKeys = [
			createKey(0, 16),
			createKey(50, 16),
			createKey(99, 16),
			createKey(999, 16), // Doesn't exist
		];
		const subsetResults = await store2.get(subsetKeys);
		const refSubsetResults = refStore.get(subsetKeys);

		assertEquals(subsetResults, refSubsetResults, "Subset results differ from reference");
		assertEquals(subsetResults[3], undefined, "Non-existent key should return null");

		await store2.close();
		await file2.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

Deno.test("FixedKVStore - missing keys return null", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;

	try {
		const file = await Deno.open(dataPath, {
			create: true,
			read: true,
			write: true,
		});
		const store = new FixedKVStore(file, DEFAULT_OPTIONS);
		await store.prepare();

		// Try to get non-existent keys
		for (let i = 1000; i < 1010; i++) {
			const key = createKey(i, 16);
			const [result] = await store.get(key);
			assertEquals(result, undefined, `Non-existent key ${i} should return null`);
		}

		await store.close();
		await file.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

Deno.test("FixedKVStore - stats are accurate", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;

	try {
		const file = await Deno.open(dataPath, {
			create: true,
			read: true,
			write: true,
		});
		const store = new FixedKVStore(file, DEFAULT_OPTIONS);
		await store.prepare();

		// Initial stats
		const initialStats = store.getStats();
		assertEquals(initialStats.memtableEntries, 0, "Initial memtable should be empty");
		assertEquals(initialStats.sstCount, 0, "Initial SST count should be 0");

		// Add some entries
		for (let i = 0; i < 50; i++) {
			const key = createKey(i, 16);
			const value = createValue(i, 64);
			await store.set(key, value);
		}

		const midStats = store.getStats();
		assertEquals(midStats.memtableEntries, 50, "Memtable should have 50 entries");
		assertEquals(midStats.sstCount, 0, "No SSTs yet");
		assertEquals(midStats.totalEntries, 50, "Total entries should be 50");

		// Close (flushes to disk)
		await store.close();
		await file.close();

		// Reopen and check stats
		const file2 = await Deno.open(dataPath, {
			read: true,
			write: true,
		});
		const store2 = new FixedKVStore(file2, DEFAULT_OPTIONS);
		await store2.prepare();

		const finalStats = store2.getStats();
		assertEquals(finalStats.memtableEntries, 0, "Memtable should be empty after reopen");
		assertEquals(finalStats.sstEntries, 50, "SST should have 50 entries");
		assertEquals(finalStats.totalEntries, 50, "Total entries should still be 50");
		assertEquals(finalStats.sstCount, 1, "Should have 1 SST");

		await store2.close();
		await file2.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

Deno.test("FixedKVStore - cache behavior", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;

	try {
		const file = await Deno.open(dataPath, {
			create: true,
			read: true,
			write: true,
		});
		const store = new FixedKVStore(file, DEFAULT_OPTIONS);
		await store.prepare();

		// Add data and flush
		for (let i = 0; i < 50; i++) {
			const key = createKey(i, 16);
			const value = createValue(i, 64);
			await store.set(key, value);
		}
		await store.close();
		await file.close();

		// Reopen and access some keys
		const file2 = await Deno.open(dataPath, {
			read: true,
			write: true,
		});
		const store2 = new FixedKVStore(file2, DEFAULT_OPTIONS);
		await store2.prepare();

		// Initial cache stats
		const stats1 = store2.getStats();
		assertEquals(stats1.cacheHits, 0, "Initial cache hits should be 0");
		assertEquals(stats1.cacheMisses, 0, "Initial cache misses should be 0");

		// First access - cache miss
		await store2.get([createKey(0, 16)]);
		const stats2 = store2.getStats();
		assertEquals(stats2.cacheMisses, 1, "Should have 1 cache miss");
		assertEquals(stats2.cacheHits, 0, "Should have 0 cache hits");

		// Second access - cache hit
		await store2.get([createKey(0, 16)]);
		const stats3 = store2.getStats();
		assertEquals(stats3.cacheMisses, 1, "Should still have 1 cache miss");
		assertEquals(stats3.cacheHits, 1, "Should have 1 cache hit");

		await store2.close();
		await file2.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

Deno.test("FixedKVStore - stress test with random operations", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;

	const refStore = new MapKVStore(16, 64);
	const keyPool: number[] = [];

	try {
		const file = await Deno.open(dataPath, {
			create: true,
			read: true,
			write: true,
		});
		const store = new FixedKVStore(file, {
			...DEFAULT_OPTIONS,
			memtableSize: 500, // Smaller memtable to trigger more flushes
		});
		await store.prepare();

		// Random operations
		const random = (seed: number) => {
			let x = seed;
			return () => {
				x = (x * 1103515245 + 12345) & 0x7fffffff;
				return x;
			};
		};
		const rand = random(42);

		// Perform many random sets
		for (let i = 0; i < 1000; i++) {
			const keyNum = rand() % 100;
			keyPool.push(keyNum);

			const key = createKey(keyNum, 16);
			const value = createValue(i, 64);

			await store.set(key, value);
			refStore.set(key, value);
		}

		// Close and reopen
		await store.close();
		await file.close();

		// Verify after reopen
		const file2 = await Deno.open(dataPath, {
			read: true,
			write: true,
		});
		const store2 = new FixedKVStore(file2, DEFAULT_OPTIONS);
		await store2.prepare();

		// Check all unique keys
		const uniqueKeys = [...new Set(keyPool)];
		for (const keyNum of uniqueKeys) {
			const key = createKey(keyNum, 16);
			const [got] = await store2.get([key]);
			const [refGot] = refStore.get([key]);

			assertEquals(got, refGot, `Mismatch for key ${keyNum} after stress test`);
		}

		await store2.close();
		await file2.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

Deno.test("FixedKVStore - empty value handling", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;

	const refStore = new MapKVStore(16, 64);

	try {
		const file = await Deno.open(dataPath, {
			create: true,
			read: true,
			write: true,
		});
		const store = new FixedKVStore(file, DEFAULT_OPTIONS);
		await store.prepare();

		// Value of all zeros
		const zeroValue = new Uint8Array(64);
		const key = createKey(0, 16);

		await store.set(key, zeroValue);
		refStore.set(key, zeroValue);

		const [got] = await store.get(key);
		const [refGot] = refStore.get([key]);

		assertEquals(got, refGot, "Zero value should be handled correctly");
		assertEquals(got, zeroValue, "Should retrieve zero value correctly");

		await store.close();
		await file.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

Deno.test("FixedKVStore - large key/value sizes", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;

	const keySize = 32;
	const valueSize = 256;

	const largeKeyCodec = new FixedBytesCodec(keySize);
	const largeValueCodec = new FixedBytesCodec(valueSize);

	const largeOptions: FixedKVStoreOptions<Uint8Array, Uint8Array> = {
		keyCodec: largeKeyCodec as any,
		valueCodec: largeValueCodec as any,
		memtableSize: 50,
		blockSize: 8192,
		blockCacheSize: 50,
	};

	const refStore = new MapKVStore(keySize, valueSize);

	try {
		const file = await Deno.open(dataPath, {
			create: true,
			read: true,
			write: true,
		});
		const store = new FixedKVStore(file, largeOptions);
		await store.prepare();

		// Test with larger sizes
		for (let i = 0; i < 100; i++) {
			const key = createKey(i, keySize);
			const value = createValue(i, valueSize);

			await store.set(key, value);
			refStore.set(key, value);
		}

		// Verify
		for (let i = 0; i < 100; i++) {
			const key = createKey(i, keySize);
			const expected = createValue(i, valueSize);

			const [got] = await store.get(key);
			const [refGot] = refStore.get([key]);

			assertEquals(
				arraysEqual(expected, got),
				true,
				`Mismatch for key ${i} with large sizes`,
			);
			assertEquals(got, refGot, `Reference mismatch for key ${i}`);
		}

		await store.close();
		await file.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

Deno.test("FixedKVStore - massive scale test with 2 million entries", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "fixedkvstore-test" });
	const dataPath = `${testDir}/data.bin`;

	const keySize = 16;
	const valueSize = 64;
	const TOTAL_ENTRIES = 2_000_000;

	const massiveKeyCodec = new FixedBytesCodec(keySize);
	const massiveValueCodec = new FixedBytesCodec(valueSize);

	const massiveOptions: FixedKVStoreOptions<Uint8Array, Uint8Array> = {
		keyCodec: massiveKeyCodec as any,
		valueCodec: massiveValueCodec as any,
		memtableSize: 50000,
		blockSize: 65536,
		blockCacheSize: 1000,
	};

	// Use MapKVStore as reference
	const refStore = new MapKVStore(keySize, valueSize);

	let seed = 12345;
	const random = () => {
		seed = (seed * 1103515245 + 12345) & 0x7fffffff;
		return seed;
	};

	try {
		console.log(`Starting massive test: ${TOTAL_ENTRIES.toLocaleString()} entries...`);
		const startTime = Date.now();

		const file = await Deno.open(dataPath, {
			create: true,
			read: true,
			write: true,
		});

		const store = new FixedKVStore(file, massiveOptions);
		await store.prepare();

		console.log("Phase 1: Inserting 2 million entries...");
		const insertStart = Date.now();

		for (let i = 0; i < TOTAL_ENTRIES; i++) {
			const keyNum = random() % TOTAL_ENTRIES;
			const key = createKey(keyNum, keySize);
			const value = createValue(i, valueSize);

			await store.set(key, value);
			refStore.set(key, value);

			if (i % 100000 === 0 && i > 0) {
				const elapsed = (Date.now() - insertStart) / 1000;
				const rate = i / elapsed;
				console.log(`  Inserted ${i.toLocaleString()} entries (${rate.toFixed(0)}/sec)`);
			}
		}

		const insertTime = (Date.now() - insertStart) / 1000;
		console.log(
			`Insert complete: ${TOTAL_ENTRIES.toLocaleString()} entries in ${insertTime.toFixed(1)}s ($
			{(TOTAL_ENTRIES / insertTime).toFixed(0)
			}/sec)`,
		);

		const stats = store.getStats();
		console.log("Stats after insert:", {
			memtableEntries: stats.memtableEntries,
			sstCount: stats.sstCount,
			totalEntries: stats.totalEntries,
			sstSize: (stats.sstSize / 1024 / 1024).toFixed(1) + " MB",
			fileSize: (stats.fileSize / 1024 / 1024).toFixed(1) + " MB",
		});

		console.log("Phase 2: Closing and reopening...");
		await store.close();
		await file.close();

		console.log("Phase 3: Reopening and verifying against reference...");

		const file2 = await Deno.open(dataPath, {
			read: true,
			write: true,
		});
		const store2 = new FixedKVStore(file2, massiveOptions);
		await store2.prepare();

		const reopenStats = store2.getStats();
		console.log("Stats after reopen:", {
			memtableEntries: reopenStats.memtableEntries,
			sstCount: reopenStats.sstCount,
			totalEntries: reopenStats.totalEntries,
		});

		// Compare total entries count
		assertEquals(reopenStats.totalEntries, refStore.size, "Entry count mismatch with reference");

		console.log(`Phase 4: Verifying all ${refStore.size} unique keys against reference...`);
		let verifiedCount = 0;
		let errors = 0;

		// Get all entries from reference store and verify
		const refEntries = refStore.entries();

		// Process in batches for efficiency
		const BATCH_SIZE = 1000;
		for (let i = 0; i < refEntries.length; i += BATCH_SIZE) {
			const batch = refEntries.slice(i, i + BATCH_SIZE);
			const keys = batch.map(([k, _v]) => k);
			const values = await store2.get(keys);

			for (let j = 0; j < batch.length; j++) {
				const entry = batch[j]!;
				const expected = entry[1];
				const got = values[j];

				if (!arraysEqual(expected, got)) {
					errors++;
					if (errors <= 5) {
						console.error(`Mismatch at entry ${i + j}`);
					}
				}
			}

			verifiedCount += batch.length;
			if (verifiedCount % 10000 === 0) {
				console.log(`  Verified ${verifiedCount}/${refEntries.length} entries`);
			}
		}

		assertEquals(errors, 0, `${errors} entries had mismatches`);

		const finalStats = store2.getStats();
		console.log("Final stats:", {
			cacheHitRate: (finalStats.cacheHitRate * 100).toFixed(1) + "%",
			cacheEntries: finalStats.cacheEntries,
			cacheSize: (finalStats.cacheSize / 1024 / 1024).toFixed(1) + " MB",
		});

		await store2.close();
		await file2.close();

		const totalTime = (Date.now() - startTime) / 1000;
		console.log(
			`\n✓ Massive test complete: ${TOTAL_ENTRIES.toLocaleString()} ops, ${refStore.size} unique entries in ${totalTime.toFixed(1)}s`,
		);
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});
