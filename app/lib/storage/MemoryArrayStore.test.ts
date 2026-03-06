import { assertEquals } from "@std/assert";
import { MemoryArrayStore } from "~/lib/storage/MemoryArrayStore.ts";

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

const ITEM_SIZE = 32;
const CODEC = new FixedBytesCodec(ITEM_SIZE);

// Test helpers
function createItem(n: number, size = ITEM_SIZE): Uint8Array {
	const item = new Uint8Array(size);
	for (let i = 0; i < size; i++) {
		item[i] = (n * 17 + i) % 256;
	}
	return item;
}

async function withStore<T>(
	testFn: (store: MemoryArrayStore<Uint8Array>) => Promise<T>,
): Promise<T> {
	const testDir = await Deno.makeTempDir({ prefix: "memoryarraystore-test" });
	const filePath = `${testDir}/data.bin`;
	const file = await Deno.open(filePath, { create: true, read: true, write: true });
	const store = new MemoryArrayStore<Uint8Array>(file, CODEC);

	try {
		await store.prepare();
		return await testFn(store);
	} finally {
		await store.close().catch(() => {});
		await Deno.remove(testDir, { recursive: true }).catch(() => {});
	}
}

// Basic operations
Deno.test("MemoryArrayStore - basic push and get", async () => {
	await withStore(async (store) => {
		const item = createItem(1);

		const index = await store.push(item);
		const got = await store.get(index);

		assertEquals(index, 0);
		assertEquals(got, item);
	});
});

Deno.test("MemoryArrayStore - get returns undefined for out of bounds", async () => {
	await withStore(async (store) => {
		const result = await store.get(999);
		assertEquals(result, undefined);
	});
});

Deno.test("MemoryArrayStore - pushMany and getRange", async () => {
	await withStore(async (store) => {
		const items = [
			createItem(1),
			createItem(2),
			createItem(3),
		];

		const indices = await store.pushMany(items);
		const results = await store.getRange(0, 3);

		assertEquals(indices, [0, 1, 2]);
		assertEquals(results, items);
	});
});

Deno.test("MemoryArrayStore - getRange with partial results", async () => {
	await withStore(async (store) => {
		await store.pushMany([createItem(1), createItem(2), createItem(3)]);

		const results = await store.getRange(1, 2);

		assertEquals(results, [createItem(2), createItem(3)]);
	});
});

Deno.test("MemoryArrayStore - length returns correct count", async () => {
	await withStore(async (store) => {
		assertEquals(await store.length(), 0);

		await store.push(createItem(1));
		assertEquals(await store.length(), 1);

		await store.pushMany([createItem(2), createItem(3)]);
		assertEquals(await store.length(), 3);
	});
});

// Persistence
Deno.test("MemoryArrayStore - persists data across reopen", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "memoryarraystore-test" });
	const filePath = `${testDir}/data.bin`;

	const items = [createItem(1), createItem(2), createItem(3)];

	try {
		// Phase 1: Write data
		const file1 = await Deno.open(filePath, { create: true, read: true, write: true });
		const store1 = new MemoryArrayStore(file1, CODEC);
		await store1.prepare();
		await store1.pushMany(items);
		await store1.close();

		// Phase 2: Read data after reopen
		const file2 = await Deno.open(filePath, { read: true, write: true });
		const store2 = new MemoryArrayStore(file2, CODEC);
		await store2.prepare();

		assertEquals(await store2.length(), 3);
		assertEquals(await store2.get(0), items[0]);
		assertEquals(await store2.get(1), items[1]);
		assertEquals(await store2.get(2), items[2]);

		await store2.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

// Truncate
Deno.test("MemoryArrayStore - truncate reduces length", async () => {
	await withStore(async (store) => {
		await store.pushMany([createItem(1), createItem(2), createItem(3)]);
		await store.truncate(2);

		assertEquals(await store.length(), 2);
		assertEquals(await store.get(0), createItem(1));
		assertEquals(await store.get(1), createItem(2));
		assertEquals(await store.get(2), undefined);
	});
});

Deno.test("MemoryArrayStore - truncate to zero clears all", async () => {
	await withStore(async (store) => {
		await store.pushMany([createItem(1), createItem(2)]);
		await store.truncate(0);

		assertEquals(await store.length(), 0);
		assertEquals(await store.get(0), undefined);
	});
});

Deno.test("MemoryArrayStore - truncate throws on invalid length", async () => {
	await withStore(async (store) => {
		await store.push(createItem(1));

		let error: Error | undefined;
		try {
			await store.truncate(5);
		} catch (e) {
			error = e as Error;
		}

		assertEquals(error?.message.includes("out of bounds"), true);
	});
});

Deno.test("MemoryArrayStore - truncate persists after close", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "memoryarraystore-test" });
	const filePath = `${testDir}/data.bin`;

	try {
		// Phase 1: Write and truncate
		const file1 = await Deno.open(filePath, { create: true, read: true, write: true });
		const store1 = new MemoryArrayStore(file1, CODEC);
		await store1.prepare();
		await store1.pushMany([createItem(1), createItem(2), createItem(3)]);
		await store1.truncate(1);
		await store1.close();

		// Phase 2: Verify truncation persisted
		const file2 = await Deno.open(filePath, { read: true, write: true });
		const store2 = new MemoryArrayStore(file2, CODEC);
		await store2.prepare();

		assertEquals(await store2.length(), 1);
		assertEquals(await store2.get(0), createItem(1));

		await store2.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

// Empty operations
Deno.test("MemoryArrayStore - handles empty pushMany", async () => {
	await withStore(async (store) => {
		const indices = await store.pushMany([]);
		assertEquals(indices, []);
		assertEquals(await store.length(), 0);
	});
});

Deno.test("MemoryArrayStore - handles empty getRange", async () => {
	await withStore(async (store) => {
		await store.push(createItem(1));
		const results = await store.getRange(0, 0);
		assertEquals(results, []);
	});
});

// Concurrent operations
Deno.test("MemoryArrayStore - handles concurrent pushes", async () => {
	await withStore(async (store) => {
		const promises = [];
		for (let i = 0; i < 100; i++) {
			promises.push(store.push(createItem(i)));
		}
		const indices = await Promise.all(promises);

		// All indices should be unique
		const uniqueIndices = new Set(indices);
		assertEquals(uniqueIndices.size, 100);

		// Verify all items
		for (let i = 0; i < 100; i++) {
			const index = indices[i]!;
			const got = await store.get(index);
			assertEquals(got, createItem(i), `Mismatch at index ${index}`);
		}
	});
});

Deno.test("MemoryArrayStore - handles concurrent gets", async () => {
	await withStore(async (store) => {
		await store.pushMany(Array.from({ length: 100 }, (_, i) => createItem(i)));

		const promises = [];
		for (let i = 0; i < 100; i++) {
			promises.push(store.get(i));
		}
		const results = await Promise.all(promises);

		for (let i = 0; i < 100; i++) {
			assertEquals(results[i], createItem(i), `Mismatch at index ${i}`);
		}
	});
});

// Double prepare/close
Deno.test("MemoryArrayStore - handles double prepare", async () => {
	await withStore(async (store) => {
		await store.prepare();
		await store.push(createItem(1));
		const got = await store.get(0);
		assertEquals(got, createItem(1));
	});
});

Deno.test("MemoryArrayStore - handles double close", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "memoryarraystore-test" });
	const filePath = `${testDir}/data.bin`;
	const file = await Deno.open(filePath, { create: true, read: true, write: true });
	const store = new MemoryArrayStore(file, CODEC);

	try {
		await store.prepare();
		await store.push(createItem(1));
		await store.close();
		await store.close();
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

// Many items
Deno.test("MemoryArrayStore - handles many items", async () => {
	await withStore(async (store) => {
		const count = 1000;
		const items = [];

		for (let i = 0; i < count; i++) {
			items.push(createItem(i));
		}

		await store.pushMany(items);

		// Verify all can be retrieved
		for (let i = 0; i < count; i++) {
			const got = await store.get(i);
			assertEquals(got, createItem(i), `Mismatch at index ${i}`);
		}
	});
});
