import { U32LE } from "@nomadshiba/codec";
import { assertEquals, assertRejects, assertThrows } from "@std/assert";
import { createArrayStore, type ArrayStore } from "~/lib/storage/ArrayStore.ts";

const CODEC = U32LE;

async function withStore<T>(
	testFn: (store: ArrayStore<number>) => Promise<T>,
): Promise<T> {
	const dir = await Deno.makeTempDir({ prefix: "arraystore-test-" });
	const store = await createArrayStore({ name: "test", path: dir, codec: CODEC });
	try {
		return await testFn(store);
	} finally {
		store.close();
		await Deno.remove(dir, { recursive: true }).catch(() => {});
	}
}

// Basic operations

Deno.test("ArrayStore - append and get", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		const i = tx.append(42);
		assertEquals(i, 0);
		tx.apply();
		assertEquals(await store.get(0), 42);
	});
});

Deno.test("ArrayStore - set and get", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.append(1);
		tx.append(2);
		tx.set(0, 99);
		tx.apply();
		assertEquals(await store.get(0), 99);
		assertEquals(await store.get(1), 2);
	});
});

Deno.test("ArrayStore - length reflects applied changes", async () => {
	await withStore(async (store) => {
		assertEquals(store.length(), 0);
		const tx = store.transaction();
		tx.append(10);
		tx.append(20);
		tx.apply();
		assertEquals(store.length(), 2);
	});
});

Deno.test("ArrayStore - tx.length reflects staged appends", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		assertEquals(tx.length(), 0);
		tx.append(1);
		tx.append(2);
		assertEquals(tx.length(), 2);
		tx.discard();
	});
});

Deno.test("ArrayStore - get out of bounds throws", async () => {
	await withStore(async (store) => {
		await assertRejects(() => store.get(0));
		const tx = store.transaction();
		tx.append(1);
		tx.apply();
		await assertRejects(() => store.get(1));
	});
});

Deno.test("ArrayStore - discard throws away staged changes", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.append(99);
		tx.discard();
		assertEquals(store.length(), 0);
	});
});

Deno.test("ArrayStore - second transaction throws while one is open", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		assertThrows(() => store.transaction());
		tx.discard();
	});
});

Deno.test("ArrayStore - tx.get sees staged writes", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.append(55);
		assertEquals(await tx.get(0), 55);
		tx.discard();
	});
});

Deno.test("ArrayStore - multiple sequential transactions accumulate", async () => {
	await withStore(async (store) => {
		const tx1 = store.transaction();
		tx1.append(1);
		tx1.append(2);
		tx1.apply();

		const tx2 = store.transaction();
		tx2.append(3);
		tx2.append(4);
		tx2.apply();

		assertEquals(await store.get(0), 1);
		assertEquals(await store.get(1), 2);
		assertEquals(await store.get(2), 3);
		assertEquals(await store.get(3), 4);
	});
});

Deno.test("ArrayStore - last write wins within same tx", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.append(1);
		tx.set(0, 2);
		tx.set(0, 3);
		tx.apply();
		assertEquals(await store.get(0), 3);
	});
});

// WAL + persistence

Deno.test("ArrayStore - WAL save and apply persists to disk", async () => {
	const dir = await Deno.makeTempDir({ prefix: "arraystore-test-" });
	try {
		const store = await createArrayStore({ name: "test", path: dir, codec: CODEC });
		const tx = store.transaction();
		tx.append(7);
		tx.append(8);
		tx.apply();

		const wal = await store.WAL();
		await wal.save();
		await wal.apply();
		store.close();

		// Reopen — data should be on disk
		const store2 = await createArrayStore({ name: "test", path: dir, codec: CODEC });
		assertEquals(await store2.get(0), 7);
		assertEquals(await store2.get(1), 8);
		store2.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("ArrayStore - WAL lookup by id returns null if not found", async () => {
	await withStore(async (store) => {
		const wal = await store.WAL({ id: "nonexistent-id" });
		assertEquals(wal, null);
	});
});

Deno.test("ArrayStore - WAL discard removes the file", async () => {
	const dir = await Deno.makeTempDir({ prefix: "arraystore-test-" });
	try {
		const store = await createArrayStore({ name: "test", path: dir, codec: CODEC });
		const tx = store.transaction();
		tx.append(1);
		tx.apply();

		const wal = await store.WAL();
		await wal.save();

		// WAL file should exist
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

Deno.test("ArrayStore - crash recovery: WAL apply replays changes", async () => {
	const dir = await Deno.makeTempDir({ prefix: "arraystore-test-" });
	try {
		const store1 = await createArrayStore({ name: "test", path: dir, codec: CODEC });
		const tx = store1.transaction();
		tx.append(10);
		tx.append(20);
		tx.apply();

		const wal = await store1.WAL();
		await wal.save();
		// crash before apply — WAL is on disk, data is not
		store1.close();

		// reopen, find WAL by id, apply
		const store2 = await createArrayStore({ name: "test", path: dir, codec: CODEC });
		const recovered = await store2.WAL({ id: wal.id });
		assertEquals(recovered !== null, true);
		await recovered!.apply();

		assertEquals(await store2.get(0), 10);
		assertEquals(await store2.get(1), 20);
		store2.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("ArrayStore - empty transaction WAL saves and applies cleanly", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.apply();
		const wal = await store.WAL();
		await wal.save();
		await wal.apply();
		assertEquals(store.length(), 0);
	});
});

Deno.test("ArrayStore - tx.set overwrites within-tx append before apply", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		const i = tx.append(10);
		tx.set(i, 99);
		tx.apply();
		assertEquals(await store.get(0), 99);
	});
});

Deno.test("ArrayStore - tx.set out of bounds throws", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.append(1);
		let threw = false;
		try {
			tx.set(5, 99);
		} catch {
			threw = true;
		}
		assertEquals(threw, true);
		tx.discard();
	});
});

Deno.test("ArrayStore - tx.get negative index throws", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.append(1);
		let threw = false;
		try {
			await tx.get(-1);
		} catch {
			threw = true;
		}
		assertEquals(threw, true);
		tx.discard();
	});
});

Deno.test("ArrayStore - get negative index throws", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.append(1);
		tx.apply();
		let threw = false;
		try {
			await store.get(-1);
		} catch {
			threw = true;
		}
		assertEquals(threw, true);
	});
});

Deno.test("ArrayStore - getMany with duplicate indices returns correct values", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.append(42);
		tx.append(7);
		tx.apply();

		const results = await store.getMany([0, 1, 0, 1]);
		assertEquals(results[0], 42);
		assertEquals(results[1], 7);
		assertEquals(results[2], 42);
		assertEquals(results[3], 7);
	});
});

Deno.test("ArrayStore - getMany out of bounds throws", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		tx.append(1);
		tx.apply();

		let threw = false;
		try {
			await store.getMany([0, 5]);
		} catch {
			threw = true;
		}
		assertEquals(threw, true);
	});
});

Deno.test("ArrayStore - getMany mixes staged sets, staged appends, and disk", async () => {
	await withStore(async (store) => {
		// Flush two items to disk
		const tx1 = store.transaction();
		tx1.append(10);
		tx1.append(20);
		tx1.apply();
		const wal = await store.WAL();
		await wal.save();
		await wal.apply();
		await wal.discard();

		// Stage a set on index 0 and a new append
		const tx2 = store.transaction();
		tx2.set(0, 99);
		tx2.append(30);
		tx2.apply();

		const results = await store.getMany([0, 1, 2]);
		assertEquals(results[0], 99);  // staged set
		assertEquals(results[1], 20);  // disk
		assertEquals(results[2], 30);  // staged append
	});
});

Deno.test("ArrayStore - WAL with both appends and sets persists correctly", async () => {
	const dir = await Deno.makeTempDir({ prefix: "arraystore-test-" });
	try {
		// Flush base items to disk
		const store1 = await createArrayStore({ name: "test", path: dir, codec: CODEC });
		const tx1 = store1.transaction();
		tx1.append(1);
		tx1.append(2);
		tx1.append(3);
		tx1.apply();
		const wal1 = await store1.WAL();
		await wal1.save();
		await wal1.apply();
		await wal1.discard();
		store1.close();

		// Reopen: append new item AND update existing
		const store2 = await createArrayStore({ name: "test", path: dir, codec: CODEC });
		const tx2 = store2.transaction();
		tx2.append(4);
		tx2.set(1, 99);
		tx2.apply();
		const wal2 = await store2.WAL();
		await wal2.save();
		await wal2.apply();
		await wal2.discard();
		store2.close();

		// Verify all changes on reopen
		const store3 = await createArrayStore({ name: "test", path: dir, codec: CODEC });
		assertEquals(await store3.get(0), 1);
		assertEquals(await store3.get(1), 99); // updated
		assertEquals(await store3.get(2), 3);
		assertEquals(await store3.get(3), 4);  // appended
		assertEquals(store3.length(), 4);
		store3.close();
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("ArrayStore - WAL with transaction open throws", async () => {
	await withStore(async (store) => {
		const tx = store.transaction();
		let threw = false;
		try {
			await store.WAL();
		} catch {
			threw = true;
		}
		assertEquals(threw, true);
		tx.discard();
	});
});

Deno.test("ArrayStore - codec stride validation throws on create", async () => {
	const dir = await Deno.makeTempDir({ prefix: "arraystore-test-" });
	try {
		const badCodec = { stride: 0, encode: () => new Uint8Array(0), decode: () => [0, 0] as [number, number] };
		let threw = false;
		try {
			await createArrayStore({ name: "test", path: dir, codec: badCodec as never });
		} catch {
			threw = true;
		}
		assertEquals(threw, true);
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("ArrayStore - set on staged (outer) append after tx apply", async () => {
	await withStore(async (store) => {
		// Append in tx1, apply → staged
		const tx1 = store.transaction();
		tx1.append(10);
		tx1.apply();

		// tx2 can set index 0 (it's in stagedAppends)
		const tx2 = store.transaction();
		tx2.set(0, 55);
		tx2.apply();

		assertEquals(await store.get(0), 55);
	});
});
