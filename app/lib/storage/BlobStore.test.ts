import { assertEquals, assertRejects, assertThrows } from "@std/assert";
import { Codec } from "@nomadshiba/codec";
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

		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();

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

		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();

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

		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();

		assertEquals(await store.get(0, 4), blobs[0]);
		assertEquals(await store.get(4, 8), blobs[1]);
		assertEquals(await store.get(12, 16), blobs[2]);
	});
});

// WAL + persistence

Deno.test("BlobStore - WAL is null when no WAL on disk", async () => {
	await withStore(async (store) => {
		assertEquals(store.wal, null);
	});
});

Deno.test("BlobStore - WAL discard removes the file", async () => {
	const dir = await Deno.makeTempDir({ prefix: "blobstore-test-" });
	try {
		const store = await createBlobStore({ name: "test", path: dir });
		const tx = store.transaction();
		tx.append(makeData(1, 4));
		tx.apply();

		const wal = await store.createWAL();

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

		const wal = await store1.createWAL();
		// crash before apply

		const store2 = await createBlobStore({ name: "test", path: dir });
		const recovered = store2.wal;
		assertEquals(recovered !== null, true);
		await recovered!.apply();
		await recovered!.discard();

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
		const wal = await store1.createWAL();
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

		const wal1 = await store.createWAL();
		await wal1.apply();
		await wal1.discard();

		// second blob goes to next chunk
		const tx2 = store.transaction();
		const p1 = tx2.append(makeData(0xBB, 8));
		assertEquals(p1, 16);
		tx2.apply();

		const wal2 = await store.createWAL();
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
		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();
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

Deno.test("BlobStore - staged blob readable after apply but before WAL save", async () => {
	await withStore(async (store) => {
		const data = makeData(0xCC, 8);
		const tx = store.transaction();
		const pointer = tx.append(data);
		tx.apply();

		// Not yet on disk, but readable from staged
		assertEquals(await store.get(pointer, 8), data);
	});
});

Deno.test("BlobStore - blob straddling chunk boundary rolls to next chunk with gap", async () => {
	// chunkByteSize = 16
	// First blob: 10 bytes → pointer 0, occupies bytes 0-9 of chunk_0
	// Second blob: 10 bytes → won't fit in remaining 6 bytes → rolls to chunk_1
	// tx.append now uses the same roll logic as appendBlobToDisk, so pointer is canonical
	const dir = await Deno.makeTempDir({ prefix: "blobstore-test-" });
	try {
		const store1 = await createBlobStore({ name: "test", path: dir, chunkByteSize: 16 });
		const tx1 = store1.transaction();
		const p0 = tx1.append(makeData(0xAA, 10));
		assertEquals(p0, 0);
		tx1.apply();
		const wal1 = await store1.createWAL();
		await wal1.apply();
		await wal1.discard();

		// Reopen so stagedLength reflects actual totalLength from disk
		const store2 = await createBlobStore({ name: "test", path: dir, chunkByteSize: 16 });
		const tx2 = store2.transaction();
		const p1 = tx2.append(makeData(0xBB, 10));
		assertEquals(p1, 16); // correctly rolled to chunk_1 start
		tx2.apply();
		const wal2 = await store2.createWAL();
		await wal2.apply();
		await wal2.discard();

		const store3 = await createBlobStore({ name: "test", path: dir, chunkByteSize: 16 });
		assertEquals(await store3.get(0, 10), makeData(0xAA, 10));
		assertEquals(await store3.get(16, 10), makeData(0xBB, 10));
		assertEquals(store3.length(), 26); // 16 + 10
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("BlobStore - multiple chunks reopen recovers correct total length", async () => {
	const dir = await Deno.makeTempDir({ prefix: "blobstore-test-" });
	try {
		// chunkByteSize = 16, write 3 blobs of 8 bytes each → spans 2 chunks
		const store1 = await createBlobStore({ name: "test", path: dir, chunkByteSize: 16 });
		const tx = store1.transaction();
		tx.append(makeData(1, 8));  // chunk_0 bytes 0-7
		tx.append(makeData(2, 8));  // chunk_0 bytes 8-15 (fills it)
		tx.append(makeData(3, 8));  // chunk_1 bytes 0-7
		tx.apply();
		const wal = await store1.createWAL();
		await wal.apply();
		await wal.discard();

		// Reopen — should recover length = 2*16 + 8 = 40? No.
		// chunk_0 = 16 bytes, chunk_1 = 8 bytes → totalLength = 1*16 + 8 = 24
		const store2 = await createBlobStore({ name: "test", path: dir, chunkByteSize: 16 });
		assertEquals(store2.length(), 24);
		assertEquals(await store2.get(0, 8), makeData(1, 8));
		assertEquals(await store2.get(8, 8), makeData(2, 8));
		assertEquals(await store2.get(16, 8), makeData(3, 8));
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("BlobStore - get on pointer beyond committed length throws", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.append(makeData(1, 8));
		tx.apply();
		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();

		// pointer 999 doesn't exist
		await assertRejects(() => store.get(999, 8));
	});
});

Deno.test("BlobStore - WAL with transaction open throws", async () => {
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

Deno.test("BlobStore - length after discard stays at pre-tx value", async () => {
	await withStore(async (store) => {
		const tx1 = store.transaction();
		tx1.append(makeData(1, 8));
		tx1.apply();
		assertEquals(store.length(), 8);

		const tx2 = store.transaction();
		tx2.append(makeData(2, 8));
		assertEquals(tx2.length(), 16);
		tx2.discard();

		assertEquals(store.length(), 8);
	});
});

Deno.test("BlobStore - zero-length blob append and read", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		const p = tx.append(new Uint8Array(0));
		assertEquals(p, 0);
		tx.apply();
		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();

		const result = await store.get(0, 0);
		assertEquals(result.length, 0);
	});
});

Deno.test("BlobStore - tx.get on staged value after outer apply", async () => {
	await withStore(async (store) => {
		// Stage blob in outer store
		const tx1 = store.transaction();
		const p = tx1.append(makeData(0xDE, 4));
		tx1.apply();

		// New tx can read it via store.get fallback
		const tx2 = store.transaction();
		assertEquals(await tx2.get(p, 4), makeData(0xDE, 4));
		tx2.discard();
	});
});

// Codec-based get

/** Simple variable-length codec: [u8 length][bytes] */
class LengthPrefixedCodec extends Codec<Uint8Array> {
	readonly stride = -1;
	encode(value: Uint8Array): Uint8Array<ArrayBuffer> {
		const out = new Uint8Array(1 + value.length);
		out[0] = value.length;
		out.set(value, 1);
		return out as Uint8Array<ArrayBuffer>;
	}
	decode(data: Uint8Array): [Uint8Array, number] {
		const len = data[0]!;
		return [data.slice(1, 1 + len), 1 + len];
	}
}

const lpCodec = new LengthPrefixedCodec();

Deno.test("BlobStore - get with variable codec reads staged blob", async () => {
	await withStore(async (store) => {
		const payload = makeData(0xAB, 8);
		const encoded = lpCodec.encode(payload);
		const tx = store.transaction();
		const pointer = tx.append(encoded);
		tx.apply();

		const result = await store.get(pointer, lpCodec);
		assertEquals(result, payload);
	});
});

Deno.test("BlobStore - get with variable codec reads from disk", async () => {
	await withStore(async (store) => {
		const payload = makeData(0xCD, 12);
		const encoded = lpCodec.encode(payload);
		const tx = store.transaction();
		const pointer = tx.append(encoded);
		tx.apply();
		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();

		const result = await store.get(pointer, lpCodec);
		assertEquals(result, payload);
	});
});

Deno.test("BlobStore - get with codec respects custom readAheadSize", async () => {
	await withStore(async (store) => {
		const payload = makeData(0xEF, 4);
		const encoded = lpCodec.encode(payload);
		const tx = store.transaction();
		const pointer = tx.append(encoded);
		tx.apply();
		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();

		const result = await store.get(pointer, lpCodec, { readAheadSize: 16 });
		assertEquals(result, payload);
	});
});

Deno.test("BlobStore - tx.get with codec sees tx-staged blob", async () => {
	await withStore(async (store) => {
		const payload = makeData(0x77, 6);
		const encoded = lpCodec.encode(payload);
		const tx = store.transaction();
		const pointer = tx.append(encoded);

		const result = await tx.get(pointer, lpCodec);
		assertEquals(result, payload);
		tx.discard();
	});
});
