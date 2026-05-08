import { assertEquals, assertRejects, assertThrows } from "@std/assert";
import { createBlobStore, type BlobStore } from "~/lib/storage/BlobStore.ts";

function makeData(byte: number, length: number): Uint8Array {
	return new Uint8Array(length).fill(byte);
}

async function withStore<T>(
	testFn: (store: BlobStore) => Promise<T>,
	options: { chunkByteSize?: number } = {},
): Promise<T> {
	const dir = await Deno.makeTempDir({ prefix: "blobstore-test-" });
	const store = await createBlobStore({ name: "test", path: dir, ...options });
	try {
		return await testFn(store);
	} finally {
		await Deno.remove(dir, { recursive: true }).catch(() => {});
	}
}

// Basic operations

Deno.test("BlobStore - append and get", async () => {
	await withStore(async (store) => {
		const data = makeData(0xAB, 16);
		const tx = store.transaction();
		const pointer = tx.append(data);
		assertEquals(pointer, 0);
		tx.apply();

		const wal = await store.WAL();
		await wal.save();
		await wal.apply();

		assertEquals(await store.get(0, 16), data);
	});
});

Deno.test("BlobStore - append returns sequential pointers", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		const p0 = tx.append(makeData(1, 10));
		const p1 = tx.append(makeData(2, 20));
		const p2 = tx.append(makeData(3, 5));
		assertEquals(p0, 0);
		assertEquals(p1, 10);
		assertEquals(p2, 30);
		tx.apply();

		const wal = await store.WAL();
		await wal.save();
		await wal.apply();

		assertEquals(await store.get(0, 10), makeData(1, 10));
		assertEquals(await store.get(10, 20), makeData(2, 20));
		assertEquals(await store.get(30, 5), makeData(3, 5));
	});
});

Deno.test("BlobStore - tx.get sees staged writes before apply", async () => {
	await withStore(async (store) => {
		const data = makeData(0x77, 8);
		const tx = store.transaction();
		const pointer = tx.append(data);
		assertEquals(await tx.get(pointer, 8), data);
		tx.discard();
	});
});

Deno.test("BlobStore - length reflects staged length", async () => {
	await withStore(async (store) => {
		assertEquals(store.length(), 0);
		const tx = store.transaction();
		tx.append(makeData(1, 10));
		assertEquals(tx.length(), 10);
		tx.apply();
		assertEquals(store.length(), 10);
	});
});

Deno.test("BlobStore - discard throws away staged changes", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.append(makeData(0xFF, 8));
		tx.discard();
		assertEquals(store.length(), 0);
	});
});

Deno.test("BlobStore - second transaction throws while one is open", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		assertThrows(() => store.transaction());
		tx.discard();
	});
});

Deno.test("BlobStore - multiple sequential transactions accumulate", async () => {
	await withStore(async (store) => {
		const blobs = [makeData(1, 4), makeData(2, 8), makeData(3, 16)];
		let nextExpected = 0;
		for (const blob of blobs) {
			const tx = store.transaction();
			const p = tx.append(blob);
			assertEquals(p, nextExpected);
			nextExpected += blob.length;
			tx.apply();
		}

		const wal = await store.WAL();
		await wal.save();
		await wal.apply();

		assertEquals(await store.get(0, 4), blobs[0]);
		assertEquals(await store.get(4, 8), blobs[1]);
		assertEquals(await store.get(12, 16), blobs[2]);
	});
});

// WAL + persistence

Deno.test("BlobStore - WAL lookup by id returns null if not found", async () => {
	await withStore(async (store) => {
		const wal = await store.WAL({ id: "nonexistent-id" });
		assertEquals(wal, null);
	});
});

Deno.test("BlobStore - WAL discard removes the file", async () => {
	const dir = await Deno.makeTempDir({ prefix: "blobstore-test-" });
	try {
		const store = await createBlobStore({ name: "test", path: dir });
		const tx = store.transaction();
		tx.append(makeData(1, 4));
		tx.apply();

		const wal = await store.WAL();
		await wal.save();

		const walExists = (await Array.fromAsync(Deno.readDir(dir))).some((e) => e.name.endsWith(".wal"));
		assertEquals(walExists, true);

		await wal.discard();
		const walExistsAfter = (await Array.fromAsync(Deno.readDir(dir))).some((e) => e.name.endsWith(".wal"));
		assertEquals(walExistsAfter, false);
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("BlobStore - crash recovery: WAL apply replays changes", async () => {
	const dir = await Deno.makeTempDir({ prefix: "blobstore-test-" });
	const data = makeData(0x42, 32);
	try {
		const store1 = await createBlobStore({ name: "test", path: dir });
		const tx = store1.transaction();
		tx.append(data);
		tx.apply();

		const wal = await store1.WAL();
		await wal.save();
		// crash before apply

		const store2 = await createBlobStore({ name: "test", path: dir });
		const recovered = await store2.WAL({ id: wal.id });
		assertEquals(recovered !== null, true);
		await recovered!.apply();

		assertEquals(await store2.get(0, 32), data);
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("BlobStore - persists data across reopen", async () => {
	const dir = await Deno.makeTempDir({ prefix: "blobstore-test-" });
	const data = makeData(0x77, 64);
	try {
		const store1 = await createBlobStore({ name: "test", path: dir });
		const tx = store1.transaction();
		tx.append(data);
		tx.apply();
		const wal = await store1.WAL();
		await wal.save();
		await wal.apply();
		await wal.discard();

		const store2 = await createBlobStore({ name: "test", path: dir });
		assertEquals(await store2.get(0, 64), data);
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("BlobStore - pointers continue correctly across chunk boundary", async () => {
	await withStore(async (store) => {
		// chunkByteSize = 16, first blob fills it exactly
		const tx1 = store.transaction();
		const p0 = tx1.append(makeData(0xAA, 16));
		assertEquals(p0, 0);
		tx1.apply();

		const wal1 = await store.WAL();
		await wal1.save();
		await wal1.apply();
		await wal1.discard();

		// second blob goes to next chunk
		const tx2 = store.transaction();
		const p1 = tx2.append(makeData(0xBB, 8));
		assertEquals(p1, 16);
		tx2.apply();

		const wal2 = await store.WAL();
		await wal2.save();
		await wal2.apply();
		await wal2.discard();

		assertEquals(await store.get(0, 16), makeData(0xAA, 16));
		assertEquals(await store.get(16, 8), makeData(0xBB, 8));
	}, { chunkByteSize: 16 });
});

Deno.test("BlobStore - empty transaction WAL saves and applies cleanly", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.apply();
		const wal = await store.WAL();
		await wal.save();
		await wal.apply();
		assertEquals(store.length(), 0);
	});
});

Deno.test("BlobStore - blob exceeding chunk size throws", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		await assertRejects(async () => {
			tx.append(makeData(1, 32)); // 32 > chunkByteSize of 16
		});
		tx.discard();
	}, { chunkByteSize: 16 });
});
