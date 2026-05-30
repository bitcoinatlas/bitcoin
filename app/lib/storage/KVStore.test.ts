/**
 * KVStore tests.
 *
 * Covers:
 *  - Basic get/set/getMany across batch → WAL → apply lifecycle
 *  - liveCount correctness: inserts, updates, multi-WAL cycles
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

import { Database } from "@db/sqlite";

/** Read total liveCount by summing COUNT(*) across all shard DBs in dir. */
function readLiveCount(dir: string, shards = 16): number {
	let total = 0;
	for (let i = 0; i < shards; i++) {
		const db = new Database(`${dir}/shard-${i}.db`);
		const row = db.prepare("SELECT COUNT(*) as n FROM kv").get<{ n: number }>();
		db.close();
		total += row!.n;
	}
	return total;
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
		const key = 0x01_00_00_00;
		await commitPairs(store, [[key, 42n]]);
		const countAfterInsert = readLiveCount(dir);

		await commitPairs(store, [[key, 99n]]);
		const countAfterUpdate = readLiveCount(dir);

		assertEquals(countAfterInsert, countAfterUpdate, "liveCount must not grow on update");
		assertEquals(await store.get(key), 99n, "updated value must be readable");
	});
});

Deno.test("liveCount increments correctly for new inserts", async () => {
	await withStore(async (store, dir) => {
		await commitPairs(store, [[0, 1n], [1, 2n], [2, 3n]]);
		const count = readLiveCount(dir);
		assertEquals(count, 3);
	});
});

Deno.test("multiple WAL cycles accumulate liveCount correctly", async () => {
	await withStore(async (store, dir) => {
		await commitPairs(store, [[0, 1n]]);
		await commitPairs(store, [[1, 2n]]);
		await commitPairs(store, [[2, 3n]]);
		const count = readLiveCount(dir);
		assertEquals(count, 3);
	});
});

Deno.test("update across multiple WAL cycles does not inflate liveCount", async () => {
	await withStore(async (store, dir) => {
		await commitPairs(store, [[0, 1n]]);
		const countAfter1 = readLiveCount(dir);

		// Update same key 5 times
		for (let i = 0; i < 5; i++) {
			await commitPairs(store, [[0, BigInt(i + 10)]]);
		}
		const countAfterUpdates = readLiveCount(dir);

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

		const count = readLiveCount(dir);
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
				const wal = store.wal;
				await wal.apply();
				await wal.discard();
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
		const pairs: [number, bigint][] = [];
		for (let i = 0; i < 100; i++) {
			pairs.push([i, BigInt(i * 1000)]);
		}

		// Commit all 100 in a single batch
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
		assertEquals(readLiveCount(dir), 1, "duplicate key in batch must count as 1 live entry");
		await wal.discard();
	});
});

Deno.test("large volume: 10000 unique keys all readable after apply", async () => {
	await withStore(async (store) => {
		const COUNT = 10_000;
		// Generate 10000 unique u32 keys spread across all shards.
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
		// SQLite handles growth automatically; verify all keys survive.
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

		for (let i = 0; i < COUNT; i += 100) {
			assertEquals(await store.get(i), BigInt(i), `key ${i} missing`);
		}

		const liveCount = readLiveCount(dir);
		assertEquals(liveCount, COUNT, "liveCount must match number of distinct keys");
	});
});

// ---------------------------------------------------------------------------
// Collision tests (SQLite handles these transparently via BLOB PRIMARY KEY)
// ---------------------------------------------------------------------------

Deno.test("deterministic collision: two keys at same slot hash are both retrievable", async () => {
	await withStore(async (store) => {
		await commitPairs(store, [[966, 111n], [2304, 222n]]);
		assertEquals(await store.get(966), 111n);
		assertEquals(await store.get(2304), 222n);
	});
});

Deno.test("deterministic collision: intra-batch for known pair", async () => {
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
		assertEquals(readLiveCount(dir), 2);
		// Update both — liveCount must stay 2
		await commitPairs(store, [[966, 10n], [2304, 20n]]);
		assertEquals(readLiveCount(dir), 2, "liveCount must not grow on updates of colliding pair");
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
		const countAfterFirst = readLiveCount(dir);
		// Apply again (WAL file still on disk, not discarded)
		await wal.apply();
		const countAfterSecond = readLiveCount(dir);
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
// Growth with staged-only entries (never flushed to disk before grow)
// ---------------------------------------------------------------------------

Deno.test("growth: staged-only entries survive when growth is triggered by a subsequent WAL", async () => {
	// Scenario: batch A is applied (entries land in staged, not disk), then batch B
	// triggers a WAL flush. After wal.apply() clears staged, batch A's keys must
	// still be readable from SQLite.
	await withStore(async (store) => {
		const STAGE_COUNT = 3060;
		const b = store.batch();
		for (let i = 0; i < STAGE_COUNT; i++) b.set(i, BigInt(i));
		b.apply(); // staged in memory only — NOT on disk yet

		const b2 = store.batch();
		for (let i = STAGE_COUNT; i < STAGE_COUNT + 20; i++) b2.set(i, BigInt(i));
		b2.apply();
		const wal = await store.createWAL();
		await wal.apply(); // staged cleared here
		await wal.discard();

		// All keys from batch A must still be readable after staged was cleared.
		for (let i = 0; i < STAGE_COUNT; i++) {
			const v = await store.get(i);
			assertEquals(v, BigInt(i), `staged-only key ${i} lost after growth`);
		}
		// Keys from batch B must also be readable.
		for (let i = STAGE_COUNT; i < STAGE_COUNT + 20; i++) {
			const v = await store.get(i);
			assertEquals(v, BigInt(i), `batch B key ${i} lost after growth`);
		}
	});
});

Deno.test("growth: intra-batch post-rehash collision resolved correctly", async () => {
	// Fill past 3073 keys then insert two more in a single batch — SQLite handles all of this.
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

		// Now insert the two keys in one batch.
		const b = store.batch();
		b.set(1420, 1420n);
		b.set(4352, 4352n);
		b.apply();
		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();

		assertEquals(await store.get(1420), 1420n, "first key must be readable");
		assertEquals(await store.get(4352), 4352n, "second key must be readable");
	});
});

// ---------------------------------------------------------------------------
// Large-volume stress test with 32-byte keys (simulates txId → pointer store)
// ---------------------------------------------------------------------------

/** 32-byte fixed key codec (simulates txId) */
class Bytes32Codec extends Codec<Uint8Array> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 32 };
	encode(b: Uint8Array): Uint8Array<ArrayBuffer> {
		const out = new Uint8Array(32);
		out.set(b.subarray(0, 32));
		return out;
	}
	decode(b: Uint8Array): [Uint8Array, number] {
		return [new Uint8Array(b.subarray(0, 32)), 32];
	}
}

/** Deterministic pseudo-random 32-byte key from index — collision-free up to 2^32 */
function makeKey(i: number): Uint8Array {
	const b = new Uint8Array(32);
	// Write i as 4 bytes LE in first 4 bytes, then fill rest with FNV-1a derived bytes
	const view = new DataView(b.buffer);
	view.setUint32(0, i, true);
	let h = 2166136261 ^ i;
	for (let j = 4; j < 32; j++) {
		h ^= (h >>> 8) ^ (i ^ j);
		h = (Math.imul(h, 16777619)) >>> 0;
		b[j] = h & 0xff;
	}
	return b;
}

Deno.test("large-volume debug: single Bytes32 key roundtrip", async () => {
	const dir = await Deno.makeTempDir({ prefix: "kvstore_single_" });
	try {
		const store = await createKVStore({
			name: "single",
			path: dir,
			keyCodec: new Bytes32Codec(),
			valueCodec: new U64LECodec(),
		});
		try {
			const key0 = makeKey(0);
			const b = store.batch();
			b.set(key0, 42n);
			b.apply();
			const vStaged = await store.get(key0);
			const wal = await store.createWAL();
			await wal.apply();
			const vDisk = await store.get(key0);
			await wal.discard();
			console.log(`staged=${vStaged} disk=${vDisk}`);
			assertEquals(vDisk, 42n);
		} finally {
			store.close();
		}
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("large-volume debug: 2-key shard-0 batch survives WAL", async () => {
	// keys 0 and 256 both land in shard 0 (b[0]=0)
	const dir = await Deno.makeTempDir({ prefix: "kvstore_2key_" });
	try {
		const store = await createKVStore({
			name: "2key",
			path: dir,
			keyCodec: new Bytes32Codec(),
			valueCodec: new U64LECodec(),
		});
		try {
			const key0 = makeKey(0);
			const key256 = makeKey(256);
			const b = store.batch();
			b.set(key0, 0n);
			b.set(key256, 256n);
			b.apply();
			const wal = await store.createWAL();
			await wal.apply();
			await wal.discard();
			const v0 = await store.get(key0);
			const v256 = await store.get(key256);
			console.log(`key0=${v0} key256=${v256}`);
			assertEquals(v0, 0n, "key0 must survive");
			assertEquals(v256, 256n, "key256 must survive");
		} finally {
			store.close();
		}
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("large-volume debug: find which cycle kills key 0", async () => {
	const dir = await Deno.makeTempDir({ prefix: "kvstore_debug_" });
	try {
		const store = await createKVStore({
			name: "debug",
			path: dir,
			keyCodec: new Bytes32Codec(),
			valueCodec: new U64LECodec(),
		});
		try {
			const TOTAL = 100_000;
			const BATCH_SIZE = 400;
			const key0 = makeKey(0);

			for (let offset = 0; offset < TOTAL; offset += BATCH_SIZE) {
				const end = Math.min(offset + BATCH_SIZE, TOTAL);
				const b = store.batch();
				for (let i = offset; i < end; i++) b.set(makeKey(i), BigInt(i));
				b.apply();
				const vAfterBatchApply = await store.get(key0);
				const wal = await store.createWAL();
				const vAfterCreateWAL = await store.get(key0);
				await wal.apply();
				const vAfterApply = await store.get(key0);
				await wal.discard();
				const vAfterDiscard = await store.get(key0);

				const v = vAfterDiscard;
				if (v !== 0n) {
					console.error(`key0 lost after cycle offset=${offset}: batchApply=${vAfterBatchApply} createWAL=${vAfterCreateWAL} afterApply=${vAfterApply} afterDiscard=${vAfterDiscard}`);
					break;
				}
			}
		} finally {
			store.close();
		}
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

Deno.test("large-volume: 100k txId-like 32-byte keys survive multiple WAL cycles", async () => {
	const dir = await Deno.makeTempDir({ prefix: "kvstore_stress_" });
	try {
		const store = await createKVStore({
			name: "stress",
			path: dir,
			keyCodec: new Bytes32Codec(),
			valueCodec: new U64LECodec(),
		});
		try {
			const TOTAL = 100_000;
			const BATCH_SIZE = 400; // simulate ~400 blocks per WAL cycle like the real app

			// Insert all keys in batches of BATCH_SIZE, flushing WAL every BATCH_SIZE inserts
			for (let offset = 0; offset < TOTAL; offset += BATCH_SIZE) {
				const end = Math.min(offset + BATCH_SIZE, TOTAL);
				const b = store.batch();
				for (let i = offset; i < end; i++) b.set(makeKey(i), BigInt(i));
				b.apply();
				const wal = await store.createWAL();
				await wal.apply();
				await wal.discard();
			}

			// Verify all keys are readable
			let missing = 0;
			for (let i = 0; i < TOTAL; i++) {
				const v = await store.get(makeKey(i));
				if (v !== BigInt(i)) {
					console.error(`key ${i} expected ${i} got ${v}`);
					missing++;
					if (missing >= 10) { console.error("...stopping after 10 failures"); break; }
				}
			}
			assertEquals(missing, 0, `${missing} keys lost after large-volume write`);
		} finally {
			store.close();
		}
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
});

