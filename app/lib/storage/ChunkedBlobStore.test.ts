import { assertEquals, assertRejects } from "@std/assert";
import { ChunkedBlobStore } from "~/lib/storage/ChunkedBlobStore.ts";

function makeData(byte: number, length: number): Uint8Array {
	return new Uint8Array(length).fill(byte);
}

async function withStore<T>(
	testFn: (store: ChunkedBlobStore) => Promise<T>,
	options: { chunkByteSize?: number } = {},
): Promise<T> {
	const testDir = await Deno.makeTempDir({ prefix: "chunkedblobstore-test" });
	const store = new ChunkedBlobStore(testDir, options);

	try {
		return await testFn(store);
	} finally {
		await Deno.remove(testDir, { recursive: true }).catch(() => {});
	}
}

// Basic operations
Deno.test("ChunkedBlobStore - tx.append and get", async () => {
	await withStore(async (store) => {
		const data = makeData(0xAB, 16);
		const tx = await store.transaction();
		const pointer = tx.append(data);
		assertEquals(pointer, 0);
		await tx.commit();
		await store.finalize();

		const got = await store.get(0, 16);
		assertEquals(got, data);
	});
});

Deno.test("ChunkedBlobStore - tx.append returns sequential pointers", async () => {
	await withStore(async (store) => {
		const tx = await store.transaction();
		const p0 = tx.append(makeData(1, 10));
		const p1 = tx.append(makeData(2, 20));
		const p2 = tx.append(makeData(3, 5));
		assertEquals(p0, 0);
		assertEquals(p1, 10);
		assertEquals(p2, 30);
		await tx.commit();
		await store.finalize();

		assertEquals(await store.get(0, 10), makeData(1, 10));
		assertEquals(await store.get(10, 20), makeData(2, 20));
		assertEquals(await store.get(30, 5), makeData(3, 5));
	});
});

Deno.test("ChunkedBlobStore - rollback discards staged ops", async () => {
	await withStore(async (store) => {
		const tx = await store.transaction();
		tx.append(makeData(0xFF, 8));
		tx.rollback();

		// nothing written
		const got = await store.get(0, 8);
		assertEquals(got, new Uint8Array(8)); // all zeros
	});
});

// Crash recovery
Deno.test("ChunkedBlobStore - crash recovery replays WAL on next finalize", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "chunkedblobstore-test" });
	const data = makeData(0x42, 32);

	try {
		// commit but do NOT finalize (simulate crash)
		const store1 = new ChunkedBlobStore(testDir);
		const tx = await store1.transaction();
		tx.append(data);
		await tx.commit();
		// crash — WAL on disk

		// reopen and finalize
		const store2 = new ChunkedBlobStore(testDir);
		await store2.finalize(); // replay WAL
		assertEquals(await store2.get(0, 32), data);
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

// Persistence
Deno.test("ChunkedBlobStore - persists data across reopen", async () => {
	const testDir = await Deno.makeTempDir({ prefix: "chunkedblobstore-test" });
	const data = makeData(0x77, 64);

	try {
		const store1 = new ChunkedBlobStore(testDir);
		const tx = await store1.transaction();
		tx.append(data);
		await tx.commit();
		await store1.finalize();

		const store2 = new ChunkedBlobStore(testDir);
		assertEquals(await store2.get(0, 64), data);
	} finally {
		await Deno.remove(testDir, { recursive: true });
	}
});

// commit() twice throws
Deno.test("ChunkedBlobStore - commit twice throws", async () => {
	await withStore(async (store) => {
		const tx = await store.transaction();
		tx.append(makeData(1, 4));
		await tx.commit();
		await assertRejects(() => tx.commit());
		await store.finalize();
	});
});

// finalize is idempotent
Deno.test("ChunkedBlobStore - finalize is idempotent", async () => {
	await withStore(async (store) => {
		const tx = await store.transaction();
		const data = makeData(0x11, 8);
		tx.append(data);
		await tx.commit();
		await store.finalize();
		await store.finalize(); // second call — no WAL

		assertEquals(await store.get(0, 8), data);
	});
});

// Chunk boundary
Deno.test("ChunkedBlobStore - pointers continue correctly across chunks", async () => {
	const chunkByteSize = 16;
	await withStore(async (store) => {
		// Fill first chunk (16 bytes)
		const tx1 = await store.transaction();
		const p0 = tx1.append(makeData(0xAA, 16));
		assertEquals(p0, 0);
		await tx1.commit();
		await store.finalize();

		// Next append goes to chunk 1; pointer = 16
		const tx2 = await store.transaction();
		const p1 = tx2.append(makeData(0xBB, 8));
		assertEquals(p1, 16);
		await tx2.commit();
		await store.finalize();

		assertEquals(await store.get(0, 16), makeData(0xAA, 16));
		assertEquals(await store.get(16, 8), makeData(0xBB, 8));
	}, { chunkByteSize });
});

// Multiple sequential transactions
Deno.test("ChunkedBlobStore - multiple sequential transactions accumulate", async () => {
	await withStore(async (store) => {
		const blobs = [makeData(1, 4), makeData(2, 8), makeData(3, 16)];
		let nextExpected = 0;

		for (const blob of blobs) {
			const tx = await store.transaction();
			const p = tx.append(blob);
			assertEquals(p, nextExpected);
			nextExpected += blob.length;
			await tx.commit();
			await store.finalize();
		}

		assertEquals(await store.get(0, 4), blobs[0]);
		assertEquals(await store.get(4, 8), blobs[1]);
		assertEquals(await store.get(12, 16), blobs[2]);
	});
});
