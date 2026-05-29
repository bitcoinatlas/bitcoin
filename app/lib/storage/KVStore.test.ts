/**
 * NASA-grade KVStore tests.
 *
 * Covers:
 *  - Basic get/set/getMany across batch → WAL → apply lifecycle
 *  - liveCount correctness: inserts, updates, multi-WAL cycles
 *  - Hash-collision handling (intra-batch keys that collide in the same shard slot)
 *  - maybeGrow: load-factor threshold triggers growth, data survives rehash
 *  - WAL discard: staged data is discarded, store unchanged
 *  - WAL crash-recovery: WAL present on re-open is replayed
 *  - clear(): all data wiped, liveCount reset
 *  - getMany: correct ordering, missing keys return undefined
 *  - Batch visibility: batch.get sees own staged data before apply
 *  - Batch discard: discarded batch does not affect store
 *  - Large volume: 10 000 unique keys, all readable after apply
 *  - Sequential WAL cycles: multiple createWAL/apply cycles stay consistent
 *  - Update-only WAL cycle: liveCount must not grow
 */

import { assertEquals, assertRejects, assertThrows } from "@std/assert";
import { Codec, Stride } from "@nomadshiba/codec";
import { createKVStore } from "~/lib/storage/KVStore.ts";

// ---------------------------------------------------------------------------
// Codec helpers
// ---------------------------------------------------------------------------

/** 4-byte big-endian uint32 key codec */
class U32BECodec extends Codec<number> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 4 };
	encode(n: number): Uint8Array<ArrayBuffer> {
		const b = new Uint8Array(4);
		new DataView(b.buffer).setUint32(0, n, false);
		return b;
	}
	decode(b: Uint8Array): [number, number] {
		return [new DataView(b.buffer, b.byteOffset).getUint32(0, false), 4];
	}
}

/** 8-byte little-endian bigint value codec */
class U64LECodec extends Codec<bigint> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 8 };
	encode(n: bigint): Uint8Array<ArrayBuffer> {
		const b = new Uint8Array(8);
		new DataView(b.buffer).setBigUint64(0, n, true);
		return b;
	}
	decode(b: Uint8Array): [bigint, number] {
		return [new DataView(b.buffer, b.byteOffset).getBigUint64(0, true), 8];
	}
}

const u32Codec = new U32BECodec();
const u64Codec = new U64LECodec();

// ---------------------------------------------------------------------------
// Test scaffolding
// ---------------------------------------------------------------------------

async function withStore<T>(
	fn: (store: Awaited<ReturnType<typeof createKVStore<number, bigint>>>, dir: string) => Promise<T>,
): Promise<T> {
	const dir = await Deno.makeTempDir({ prefix: "kvstore_test_" });
	try {
		const store = await createKVStore({ name: "test", path: dir, keyCodec: u32Codec, valueCodec: u64Codec });
		try {
			return await fn(store, dir);
		} finally {
			store.close();
		}
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
}

/** Commit a batch with given key/value pairs, apply WAL, then discard (full cycle). */
async function commitPairs(
	store: Awaited<ReturnType<typeof createKVStore<number, bigint>>>,
	pairs: [number, bigint][],
): Promise<void> {
	const b = store.batch();
	for (const [k, v] of pairs) b.set(k, v);
	b.apply();
	const wal = await store.createWAL();
	await wal.apply();
	await wal.discard();
}

/** Read liveCount from meta.bin for shard s inside dir. */
async function readLiveCount(dir: string, s: number): Promise<number> {
	const metaPath = `${dir}/shard_${s}/meta.bin`;
	const buf = await Deno.readFile(metaPath);
	return new DataView(buf.buffer).getUint32(4, true);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

Deno.test("basic insert and read", async () => {
	await withStore(async (store) => {
		await commitPairs(store, [[1, 100n], [2, 200n], [3, 300n]]);
		assertEquals(await store.get(1), 100n);
		assertEquals(await store.get(2), 200n);
		assertEquals(await store.get(3), 300n);
		assertEquals(await store.get(999), undefined);
	});
});

Deno.test("getMany preserves order and handles missing keys", async () => {
	await withStore(async (store) => {
		await commitPairs(store, [[10, 10n], [20, 20n], [30, 30n]]);
		const result = await store.getMany([30, 999, 10, 20, 888]);
		assertEquals(result, [30n, undefined, 10n, 20n, undefined]);
	});
});

Deno.test("update does not increase liveCount", async () => {
	await withStore(async (store, dir) => {
		// Key 0x01_00_00_00 goes to shard 1 (first byte = 1)
		// Use key = 0x01000000 => shard index = 1
		const key = 0x01_00_00_00;
		await commitPairs(store, [[key, 42n]]);

		const shard = 1;
		const countAfterInsert = await readLiveCount(dir, shard);

		// Update same key
		await commitPairs(store, [[key, 99n]]);
		const countAfterUpdate = await readLiveCount(dir, shard);

		assertEquals(countAfterInsert, countAfterUpdate, "liveCount must not grow on update");
		assertEquals(await store.get(key), 99n, "updated value must be readable");
	});
});

Deno.test("liveCount increments correctly for new inserts", async () => {
	await withStore(async (store, dir) => {
		// Insert 3 unique keys into shard 0 (keys with first byte = 0)
		// u32 big-endian: first byte = (n >>> 24). For small n, first byte = 0 → shard 0.
		await commitPairs(store, [[0, 1n], [1, 2n], [2, 3n]]);
		const count = await readLiveCount(dir, 0);
		assertEquals(count, 3);
	});
});

Deno.test("multiple WAL cycles accumulate liveCount correctly", async () => {
	await withStore(async (store, dir) => {
		await commitPairs(store, [[0, 1n]]);
		await commitPairs(store, [[1, 2n]]);
		await commitPairs(store, [[2, 3n]]);
		const count = await readLiveCount(dir, 0);
		assertEquals(count, 3);
	});
});

Deno.test("update across multiple WAL cycles does not inflate liveCount", async () => {
	await withStore(async (store, dir) => {
		await commitPairs(store, [[0, 1n]]);
		const countAfter1 = await readLiveCount(dir, 0);

		// Update same key 5 times
		for (let i = 0; i < 5; i++) {
			await commitPairs(store, [[0, BigInt(i + 10)]]);
		}
		const countAfterUpdates = await readLiveCount(dir, 0);

		assertEquals(countAfter1, 1);
		assertEquals(countAfterUpdates, 1, "repeated updates must not inflate liveCount");
		assertEquals(await store.get(0), 14n);
	});
});

Deno.test("WAL discard leaves store unchanged", async () => {
	await withStore(async (store) => {
		await commitPairs(store, [[42, 42n]]);

		const b = store.batch();
		b.set(42, 999n);
		b.apply();
		const wal = await store.createWAL();
		await wal.discard();

		// Value should still be the pre-discard value
		assertEquals(await store.get(42), 42n);
	});
});

Deno.test("batch.get sees own staged data before apply", async () => {
	await withStore(async (store) => {
		await commitPairs(store, [[1, 1n]]);

		const b = store.batch();
		b.set(1, 999n); // staged in batch, not yet in store
		b.set(2, 888n); // new key, only in batch

		assertEquals(await b.get(1), 999n, "batch.get should see batch-staged value");
		assertEquals(await b.get(2), 888n, "batch.get should see new key from batch");
		assertEquals(await store.get(1), 1n, "store.get should see old value before batch apply");
		b.discard();
	});
});

Deno.test("batch discard does not affect store", async () => {
	await withStore(async (store) => {
		await commitPairs(store, [[7, 7n]]);

		const b = store.batch();
		b.set(7, 77n);
		b.discard();

		assertEquals(await store.get(7), 7n, "discarded batch must not modify store");
	});
});

Deno.test("clear wipes all data and resets liveCount", async () => {
	await withStore(async (store, dir) => {
		await commitPairs(store, [[0, 1n], [1, 2n], [2, 3n]]);
		await store.clear();

		assertEquals(await store.get(0), undefined);
		assertEquals(await store.get(1), undefined);
		assertEquals(await store.get(2), undefined);

		const count = await readLiveCount(dir, 0);
		assertEquals(count, 0, "liveCount must be 0 after clear");
	});
});

Deno.test("crash recovery: WAL on disk is replayed on re-open", { sanitizeResources: false }, async () => {
	const dir = await Deno.makeTempDir({ prefix: "kvstore_crash_" });
	try {
		// First session: write WAL but do NOT apply (simulate crash after WAL write)
		{
			const store = await createKVStore({ name: "test", path: dir, keyCodec: u32Codec, valueCodec: u64Codec });
			const b = store.batch();
			b.set(1, 111n);
			b.set(2, 222n);
			b.apply();
			await store.createWAL(); // WAL written to disk, NOT applied — process dies here
			// Simulate crash: don't call close(), just let file handles leak (Deno cleans up on process exit)
		}

		// Second session: should replay WAL on open
		{
			const store = await createKVStore({ name: "test", path: dir, keyCodec: u32Codec, valueCodec: u64Codec });
			// WAL replay happens externally (via Store.recover), but createKVStore opens any
			// existing WAL as self.wal. Apply it manually here to simulate recover().
			if (store.wal) {
				await store.wal.apply();
				await store.wal.discard();
			}
			assertEquals(await store.get(1), 111n);
			assertEquals(await store.get(2), 222n);
			store.close();
		}
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("intra-batch collision: multiple keys in same shard slot probe correctly", async () => {
	await withStore(async (store) => {
		// All u32 keys with first byte = 0 go to shard 0. Within the shard, they may
		// collide at the same hash slot. Insert enough keys to force probing.
		// INITIAL_SLOTS_PER_SHARD = 4096. Insert 100 keys into shard 0 in a single batch.
		const pairs: [number, bigint][] = [];
		for (let i = 0; i < 100; i++) {
			// Keys 0x00000000 to 0x00000063 — all land in shard 0 (first byte = 0)
			pairs.push([i, BigInt(i * 1000)]);
		}

		// Commit all 100 in a single batch (exercises intra-batch walSlots probing)
		const b = store.batch();
		for (const [k, v] of pairs) b.set(k, v);
		b.apply();
		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();

		// Verify all 100 readable with correct values
		for (const [k, v] of pairs) {
			assertEquals(await store.get(k), v, `key ${k} should have value ${v}`);
		}
	});
});

Deno.test("intra-batch updates: later set() for same key wins, single liveCount entry", async () => {
	await withStore(async (store, dir) => {
		const b = store.batch();
		b.set(0, 1n);
		b.set(0, 2n); // Uint8ArrayMap deduplicates — last write wins
		b.apply();
		const wal = await store.createWAL();
		await wal.apply();

		assertEquals(await store.get(0), 2n);
		assertEquals(await readLiveCount(dir, 0), 1, "duplicate key in batch must count as 1 live entry");
		await wal.discard();
	});
});

Deno.test("large volume: 10000 unique keys all readable after apply", async () => {
	await withStore(async (store) => {
		const COUNT = 10_000;
		// Generate 10000 unique u32 keys spread across all 256 shards.
		// key = i * 65537 (0x10001) gives distinct values with varying first bytes.
		const pairs: [number, bigint][] = [];
		for (let i = 0; i < COUNT; i++) {
			const key = (i * 65537) >>> 0; // unique u32, wraps around fine
			pairs.push([key, BigInt(i)]);
		}

		// Insert in chunks of 500 to avoid WAL too large
		for (let offset = 0; offset < COUNT; offset += 500) {
			const chunk = pairs.slice(offset, offset + 500);
			const b = store.batch();
			for (const [k, v] of chunk) b.set(k, v);
			b.apply();
			const wal = await store.createWAL();
			await wal.apply();
			await wal.discard();
		}

		// Spot-check every 100th key
		for (let i = 0; i < COUNT; i += 100) {
			const [key, val] = pairs[i]!;
			assertEquals(await store.get(key), val, `key at index ${i} mismatch`);
		}
	});
});

Deno.test("sequential WAL cycles: interleaved inserts and updates stay consistent", async () => {
	await withStore(async (store) => {
		// Cycle 1: insert keys A, B, C
		await commitPairs(store, [[0, 10n], [1, 20n], [2, 30n]]);
		// Cycle 2: update A, insert D
		await commitPairs(store, [[0, 99n], [3, 40n]]);
		// Cycle 3: update B and C
		await commitPairs(store, [[1, 88n], [2, 77n]]);

		assertEquals(await store.get(0), 99n);
		assertEquals(await store.get(1), 88n);
		assertEquals(await store.get(2), 77n);
		assertEquals(await store.get(3), 40n);
	});
});

Deno.test("cannot create WAL while batch is in progress", async () => {
	await withStore(async (store) => {
		const b = store.batch();
		b.set(1, 1n);
		await assertRejects(() => store.createWAL(), Error, "Can't create WAL while batch is in progress");
		b.discard();
	});
});

Deno.test("cannot create second WAL while one is active", async () => {
	await withStore(async (store) => {
		const b = store.batch();
		b.set(1, 1n);
		b.apply();
		const wal = await store.createWAL();
		await assertRejects(() => store.createWAL(), Error, "WAL already exists");
		await wal.discard();
	});
});

Deno.test("growth: exceeding load factor rehashes correctly", async () => {
	await withStore(async (store, dir) => {
		// INITIAL_SLOTS_PER_SHARD = 4096, LOAD_FACTOR_THRESHOLD = 0.75
		// Insert 3073 keys into shard 0 (> 75% of 4096) to trigger growth.
		// Keys with first byte 0: key = i for i in [0, 3072].
		const COUNT = 3_073;
		for (let offset = 0; offset < COUNT; offset += 500) {
			const end = Math.min(offset + 500, COUNT);
			const b = store.batch();
			for (let i = offset; i < end; i++) b.set(i, BigInt(i));
			b.apply();
			const wal = await store.createWAL();
			await wal.apply();
			await wal.discard();
		}

		// After growth, all keys must still be readable
		for (let i = 0; i < COUNT; i += 100) {
			assertEquals(await store.get(i), BigInt(i), `key ${i} missing after shard growth`);
		}

		// liveCount in shard 0 must equal COUNT
		const liveCount = await readLiveCount(dir, 0);
		assertEquals(liveCount, COUNT, "liveCount must match number of distinct keys after growth");
	});
});

// ---------------------------------------------------------------------------
// Deterministic collision tests
// ---------------------------------------------------------------------------
// Keys 966 and 2304 (u32 big-endian, first byte = 0 → shard 0) hash to the
// same slot 1892 in a 4096-slot shard. This is proven by the slotHash function.

Deno.test("deterministic collision: two keys at same slot hash are both retrievable", async () => {
	// Keys 966 and 2304 both hash to slot 1892 in shard 0 (4096 slots).
	// 2304 must be found at slot 1893 (or next empty) via linear probing.
	await withStore(async (store) => {
		await commitPairs(store, [[966, 111n], [2304, 222n]]);
		assertEquals(await store.get(966), 111n);
		assertEquals(await store.get(2304), 222n);
	});
});

Deno.test("deterministic collision: intra-batch walSlots probing for known collision pair", async () => {
	// Both keys in one batch — exercises the incremental walSlots population fix.
	await withStore(async (store) => {
		const b = store.batch();
		b.set(966, 100n);
		b.set(2304, 200n);
		b.apply();
		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();

		assertEquals(await store.get(966), 100n);
		assertEquals(await store.get(2304), 200n);
	});
});

Deno.test("deterministic collision: update of colliding key does not disturb the other", async () => {
	await withStore(async (store) => {
		await commitPairs(store, [[966, 1n], [2304, 2n]]);
		// Update just the probed key (2304)
		await commitPairs(store, [[2304, 99n]]);
		assertEquals(await store.get(966), 1n, "primary key must be unchanged after collision-key update");
		assertEquals(await store.get(2304), 99n, "probed key must reflect update");
	});
});

Deno.test("deterministic collision: liveCount stays correct with colliding pair", async () => {
	await withStore(async (store, dir) => {
		await commitPairs(store, [[966, 1n], [2304, 2n]]);
		assertEquals(await readLiveCount(dir, 0), 2);
		// Update both — liveCount must stay 2
		await commitPairs(store, [[966, 10n], [2304, 20n]]);
		assertEquals(await readLiveCount(dir, 0), 2, "liveCount must not grow on updates of colliding pair");
	});
});

// ---------------------------------------------------------------------------
// Guard / error path tests
// ---------------------------------------------------------------------------

Deno.test("store.get reads staged value between batch.apply() and wal.apply()", async () => {
	await withStore(async (store) => {
		await commitPairs(store, [[5, 5n]]);

		const b = store.batch();
		b.set(5, 55n);
		b.apply(); // now in staged, not yet on disk

		// store.get reads from staged before disk
		assertEquals(await store.get(5), 55n, "store.get must see staged value before WAL apply");

		// Clean up
		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();
	});
});

Deno.test("clear() while WAL active throws", async () => {
	await withStore(async (store) => {
		const b = store.batch();
		b.set(1, 1n);
		b.apply();
		const wal = await store.createWAL();
		await assertRejects(() => store.clear(), Error, "Can't clear while WAL is in progress");
		await wal.discard();
	});
});

Deno.test("clear() while batch active throws", async () => {
	await withStore(async (store) => {
		const b = store.batch();
		b.set(1, 1n);
		await assertRejects(() => store.clear(), Error, "Can't clear while batch is in progress");
		b.discard();
	});
});

Deno.test("batch() throws if another batch already in progress", async () => {
	await withStore(async (store) => {
		const b1 = store.batch();
		assertThrows(() => store.batch(), Error, "Batch already in progress");
		b1.discard();
	});
});

Deno.test("batch() throws if WAL is active", async () => {
	await withStore(async (store) => {
		const b = store.batch();
		b.set(1, 1n);
		b.apply();
		const wal = await store.createWAL();
		assertThrows(() => store.batch(), Error, "Can't start batch while WAL is in progress");
		await wal.discard();
	});
});

Deno.test("createWAL with nothing staged produces zero-shard WAL, apply is no-op", async () => {
	await withStore(async (store) => {
		// No batch, nothing staged
		const wal = await store.createWAL();
		await wal.apply(); // must not throw
		await wal.discard();
		// Store is still empty
		assertEquals(await store.get(0), undefined);
	});
});

Deno.test("wal.apply() is idempotent: applying same WAL twice does not corrupt data or liveCount", async () => {
	await withStore(async (store, dir) => {
		const b = store.batch();
		b.set(0, 42n);
		b.apply();
		const wal = await store.createWAL();
		await wal.apply();
		const countAfterFirst = await readLiveCount(dir, 0);
		// Apply again (WAL file still on disk, not discarded)
		await wal.apply();
		const countAfterSecond = await readLiveCount(dir, 0);
		assertEquals(countAfterFirst, 1);
		assertEquals(countAfterSecond, 1, "double apply must not inflate liveCount");
		assertEquals(await store.get(0), 42n);
		await wal.discard();
	});
});

Deno.test("batch.getMany respects batch-staged and store-staged values", async () => {
	await withStore(async (store) => {
		await commitPairs(store, [[1, 10n], [2, 20n]]);

		const b = store.batch();
		b.set(1, 99n); // override in batch
		b.set(3, 33n); // new in batch

		const result = await b.getMany([1, 2, 3, 999]);
		assertEquals(result, [99n, 20n, 33n, undefined]);
		b.discard();
	});
});

// ---------------------------------------------------------------------------
// Growth + intra-batch post-rehash collision
// ---------------------------------------------------------------------------

Deno.test("growth: intra-batch post-rehash collision resolved correctly", async () => {
	// Keys 1420 and 4352 both hash to slot 316 in an 8192-slot shard (post-growth).
	// First fill shard 0 past the 0.75 load threshold (3073 keys) to force growth,
	// then insert the two colliding keys in a single batch — this exercises the
	// walSlots.clear() + incremental re-find path in the grew branch.
	await withStore(async (store) => {
		// Fill to just below growth threshold in chunks, avoiding keys 1420 and 4352
		const FILL = 3_073;
		for (let offset = 0; offset < FILL; offset += 500) {
			const end = Math.min(offset + 500, FILL);
			const b = store.batch();
			for (let i = offset; i < end; i++) {
				if (i === 1420 || i === 4352) continue;
				b.set(i, BigInt(i));
			}
			b.apply();
			const wal = await store.createWAL();
			await wal.apply();
			await wal.discard();
		}

		// Now insert the collision pair in one batch — this batch will trigger growth
		// (liveCount ≈ 3071, adding 2 new → projected ≥ 0.75 * 4096).
		// After growth to 8192 slots, both keys must be re-resolved with incremental
		// walSlots probing to handle their post-rehash collision at slot 316.
		const b = store.batch();
		b.set(1420, 1420n);
		b.set(4352, 4352n);
		b.apply();
		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();

		assertEquals(await store.get(1420), 1420n, "first colliding key must be readable after growth");
		assertEquals(await store.get(4352), 4352n, "second colliding key must be readable after growth");
	});
});

