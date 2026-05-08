import { U32LE } from "@nomadshiba/codec";
import { assertEquals, assertRejects, assertThrows } from "@std/assert";
import { ArrayStore } from "~/lib/storage/ArrayStore.ts";

const CODEC = U32LE;

async function withStore<T>(
	testFn: (store: ArrayStore<typeof CODEC>) => Promise<T>,
): Promise<T> {
	const testDir = await Deno.makeTempDir({ prefix: "arraystore-test" });
	const dataPath = `${testDir}/data.bin`;
	const store = new ArrayStore(dataPath, CODEC);

	try {
		return await testFn(store);
	} finally {
		await Deno.remove(testDir, { recursive: true }).catch(() => {});
	}
}

async function commitAndFinalize(
	store: ArrayStore<typeof CODEC>,
	fn: (tx: ReturnType<typeof store.transaction>) => void,
): Promise<void> {
	const tx = store.transaction();
	fn(tx);
	await tx.commit();
	await store.finalize();
}

// Basic operations
Deno.test("ArrayStore - tx.push and get", async () => {
	await withStore(async (store) => {
		await commitAndFinalize(store, (tx) => tx.push(42));
		assertEquals(await store.get(0), 42);
	});
});

Deno.test("ArrayStore - tx.concat and range", async () => {
	await withStore(async (store) => {
		await commitAndFinalize(store, (tx) => tx.concat([1, 2, 3]));
		assertEquals(await store.range(0, 3), [1, 2, 3]);
	});
});

Deno.test("ArrayStore - length reflects committed entries", async () => {
	await withStore(async (store) => {
		await commitAndFinalize(store, (tx) => tx.concat([10, 20, 30]));
		assertEquals(await store.length(), 3);
	});
});

Deno.test("ArrayStore - get returns undefined out of bounds", async () => {
	await withStore(async (store) => {
		assertEquals(await store.get(0), undefined);
		await commitAndFinalize(store, (tx) => tx.push(1));
		assertEquals(await store.get(1), undefined);
	});
});

Deno.test("ArrayStore - rollback discards staged ops", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.push(99);
		tx.rollback();
		assertEquals(await store.length(), 0);
	});
});

Deno.test("ArrayStore - second transaction throws while one is open", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		assertThrows(() => store.transaction());
		tx.rollback();
	});
});

// Persistence
Deno.test("ArrayStore - persists data across reopen", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "arraystore-test" });
	const dataPath = `${testDir}/data.bin`;

	try {
		const store1 = new ArrayStore(dataPath, CODEC);
		await commitAndFinalize(store1, (tx) => tx.concat([1, 2, 3]));

		const store2 = new ArrayStore(dataPath, CODEC);
		assertEquals(await store2.range(0, 3), [1, 2, 3]);
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

// Crash recovery
Deno.test("ArrayStore - crash recovery replays WAL on next finalize", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "arraystore-test" });
	const dataPath = `${testDir}/data.bin`;

	try {
		// commit but do NOT finalize (simulate crash)
		const store1 = new ArrayStore(dataPath, CODEC);
		const tx = store1.transaction();
		tx.concat([7, 8, 9]);
		await tx.commit();
		// crash — WAL on disk, no finalize

		// reopen and finalize
		const store2 = new ArrayStore(dataPath, CODEC);
		await store2.finalize(); // replay WAL
		assertEquals(await store2.range(0, 3), [7, 8, 9]);
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

// commit() twice throws
Deno.test("ArrayStore - commit twice throws", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.push(1);
		await tx.commit();
		await assertRejects(() => tx.commit());
		await store.finalize();
	});
});

// finalize is idempotent
Deno.test("ArrayStore - finalize is idempotent", async () => {
	await withStore(async (store) => {
		await commitAndFinalize(store, (tx) => tx.push(5));
		await store.finalize(); // second call — no WAL
		assertEquals(await store.get(0), 5);
	});
});

// empty transaction
Deno.test("ArrayStore - handles empty transaction", async () => {
	await withStore(async (store) => {
		await commitAndFinalize(store, (_tx) => {});
		assertEquals(await store.length(), 0);
	});
});

// multiple transactions in sequence
Deno.test("ArrayStore - multiple sequential transactions accumulate", async () => {
	await withStore(async (store) => {
		await commitAndFinalize(store, (tx) => tx.concat([1, 2]));
		await commitAndFinalize(store, (tx) => tx.concat([3, 4]));
		assertEquals(await store.range(0, 4), [1, 2, 3, 4]);
	});
});
