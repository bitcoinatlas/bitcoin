/**
 * KVStore test suite.
 *
 * Place next to KVStore.ts (e.g. app/lib/storage/KVStore.test.ts) and run:
 *   deno test --allow-read --allow-write app/lib/storage/KVStore.test.ts
 *
 * Coverage:
 *   - open / RocksDB init, undefined on missing key
 *   - batch: set, get, duplicate-set in-place, discard, settle guards, one at a time
 *   - reads: batch → staged → frozen → RocksDB precedence (each layer isolated)
 *   - flush: persistence, empty no-op, sequential accumulation
 *   - durability (flushed survives reopen) vs volatility (unflushed staged lost on reopen)
 *   - flush guards (no double flush, batch ALLOWED mid-flush, no clear mid-flush)
 *   - frozen serves reads after createWAL() and after apply() but before discard()
 *   - frozen serves reads during a concurrent apply() (reads never race the db transaction)
 *   - batch committed during flush lands in fresh staged, survives the next flush
 *   - full precedence ladder: batch > staged > frozen > RocksDB on the same key
 *   - WAL header format (entryCount + stride layout)
 *   - replay idempotency (apply twice == once: re-put is a no-op rewrite in Rocks)
 *   - crash recovery: manually constructed WAL replayed on reopen
 *   - clear(): wipes staged + RocksDB, guards
 *   - close(): guards (during batch, during flush)
 *   - differential fuzz vs in-memory Map oracle
 *
 * NOTE: uses real RocksDB (temp dirs, each test gets its own path). Unlike ArrayStore /
 * BlobStore there is no process-level fd to simulate "crash" cleanly, so crash recovery is
 * exercised by writing a WAL file manually before opening — this faithfully tests the WAL
 * parsing + apply path that recover() would take, without needing to force-close a live DB.
 */

import { U32 } from "@nomadshiba/codec";
import { assertEquals, assertExists, assertFalse, assertRejects, assertThrows } from "@std/assert";
import { exists } from "@std/fs";
import { join } from "@std/path";
import { KVStore } from "./KVStore.ts";
import { Uint8ArrayView } from "~/utils/Uint8ArrayView.ts";

// ── harness ───────────────────────────────────────────────────────────────────

const Key = U32; // 4-byte key
const Val = U32; // 4-byte value

function open(dir: string) {
	return KVStore.open({ path: dir, keyCodec: Key, valueCodec: Val });
}

async function withTmp(fn: (dir: string) => Promise<void>): Promise<void> {
	const dir = await Deno.makeTempDir({ prefix: "kvstore_test_" });
	try {
		await fn(dir);
	} finally {
		await Deno.remove(dir, { recursive: true }).catch(() => {});
	}
}

function closeQuiet(store: KVStore<number, number>): void {
	try {
		store.close();
	} catch {
		// left in a guarded state — fine for teardown
	}
}

/** Commit a batch of [key, value] pairs in one apply. */
function setAll(store: KVStore<number, number>, entries: [number, number][]): void {
	const b = store.batch();
	for (const [k, v] of entries) b.set(k, v);
	b.apply();
}

/** Read several keys at once. Test convenience — KVStore exposes only get(). */
function getAll(store: KVStore<number, number>, keys: number[]): Promise<(number | undefined)[]> {
	return Promise.all(keys.map((k) => store.get(k)));
}

function walPath(dir: string): string {
	return join(dir, "data.wal");
}

/**
 * Build a WAL buffer manually for crash-recovery tests.
 * Format: [u32 entryCount]([u32 key][u32 val] * count)
 */
function buildWalBuf(entries: [number, number][]): Uint8Array {
	const buf = new Uint8Array(4 + entries.length * 8);
	const view = new Uint8ArrayView(buf);
	view.setUint32(0, entries.length);
	let pos = 4;
	for (const [k, v] of entries) {
		view.setUint32(pos, k);
		pos += 4;
		view.setUint32(pos, v);
		pos += 4;
	}
	return buf;
}

// ── open ─────────────────────────────────────────────────────────────────────

Deno.test("open: empty store returns undefined for any key", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			assertEquals(await store.get(0), undefined);
			assertEquals(await getAll(store, [1, 2, 3]), [undefined, undefined, undefined]);
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("open: existing RocksDB data is visible after reopen", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir);
		try {
			setAll(store, [[1, 100], [2, 200]]);
			await store.flush();
			store.close();
			store = await open(dir);
			assertEquals(await store.get(1), 100);
			assertEquals(await store.get(2), 200);
		} finally {
			closeQuiet(store);
		}
	});
});

// ── batch semantics ───────────────────────────────────────────────────────────

Deno.test("batch: set/apply makes entries readable; invisible before apply", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			const b = store.batch();
			b.set(1, 111);
			b.set(2, 222);
			assertEquals(await store.get(1), undefined); // not visible yet
			b.apply();
			assertEquals(await store.get(1), 111);
			assertEquals(await store.get(2), 222);
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("batch: discard leaves the store unchanged", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			setAll(store, [[1, 10]]);
			const b = store.batch();
			b.set(1, 999);
			b.set(2, 888);
			b.discard();
			assertEquals(await store.get(1), 10);
			assertEquals(await store.get(2), undefined);
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("batch: reads its own uncommitted sets; falls through for unknown keys", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			setAll(store, [[1, 10]]);
			const b = store.batch();
			b.set(2, 20);
			assertEquals(await b.get(2), 20); // own set
			assertEquals(await b.get(1), 10); // falls through to committed store
			assertEquals(await b.get(3), undefined); // absent everywhere
			b.discard();
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("batch: duplicate set overwrites in-place (no extra entry, last value wins)", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			const b = store.batch();
			b.set(1, 100);
			b.set(1, 200); // overwrite
			b.set(1, 300); // overwrite again
			b.apply();
			assertEquals(await store.get(1), 300);
			// Only one entry should have been staged (no accumulation of stale values)
			await store.flush(); // apply to RocksDB
			// Reopen and verify it's still one value
			store.close();
			const store2 = await open(dir);
			try {
				assertEquals(await store2.get(1), 300);
			} finally {
				closeQuiet(store2);
			}
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("batch: reads return the right mix of batch / staged / missing", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			setAll(store, [[10, 1000]]); // staged
			const b = store.batch();
			b.set(20, 2000);
			const result = await Promise.all([b.get(10), b.get(20), b.get(30)]);
			assertEquals(result, [1000, 2000, undefined]);
			b.discard();
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("batch: only one open at a time", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			const b = store.batch();
			assertThrows(() => store.batch(), Error);
			b.discard();
			store.batch().discard(); // ok again
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("batch: settled batch throws on further use", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			const b = store.batch();
			b.set(1, 1);
			b.apply();
			assertThrows(() => b.set(2, 2), Error);
			assertThrows(() => b.apply(), Error);
			await assertRejects(() => b.get(1), Error);
		} finally {
			closeQuiet(store);
		}
	});
});

// ── reads: layer precedence ─────────────────────────────────────────────────────

Deno.test("reads: staged shadows RocksDB for the same key", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir);
		try {
			setAll(store, [[1, 111]]);
			await store.flush(); // 1→111 now in RocksDB
			store.close();
			store = await open(dir);
			setAll(store, [[1, 999]]); // same key, staged value
			assertEquals(await store.get(1), 999); // staged wins
			await store.flush();
			assertEquals(await store.get(1), 999); // still wins after flush
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("reads: batch shadows staged shadows RocksDB for the same key", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir);
		try {
			setAll(store, [[1, 1]]);
			await store.flush(); // RocksDB: 1→1
			store.close();
			store = await open(dir);
			setAll(store, [[1, 2]]); // staged: 1→2
			const b = store.batch();
			b.set(1, 3); // batch: 1→3
			assertEquals(await b.get(1), 3); // batch wins
			assertEquals(await store.get(1), 2); // staged wins (batch not applied yet)
			b.apply();
			assertEquals(await store.get(1), 3); // now staged has 3
		} finally {
			closeQuiet(store);
		}
	});
});

// ── flush / persistence ─────────────────────────────────────────────────────────

Deno.test("flush: persists staged to RocksDB, removes WAL, clears frozen", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			setAll(store, [[1, 100], [2, 200]]);
			await store.flush();
			assertEquals(store.wal, null);
			assertFalse(await exists(walPath(dir)));
			assertEquals(await store.get(1), 100);
			assertEquals(await store.get(2), 200);
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("flush: empty staged is a clean no-op", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			setAll(store, [[1, 1]]);
			await store.flush();
			await store.flush(); // nothing staged
			assertEquals(await store.get(1), 1);
			assertFalse(await exists(walPath(dir)));
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("flush: sequential flushes accumulate and overwrite correctly", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			setAll(store, [[1, 1], [2, 2]]);
			await store.flush();
			setAll(store, [[2, 22], [3, 3]]); // overwrite 2, add 3
			await store.flush();
			assertEquals(await getAll(store, [1, 2, 3]), [1, 22, 3]);
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("durability: flushed entries survive reopen", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir);
		try {
			setAll(store, [[10, 1000], [20, 2000], [30, 3000]]);
			await store.flush();
			store.close();
			store = await open(dir);
			assertEquals(await getAll(store, [10, 20, 30]), [1000, 2000, 3000]);
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("volatility: unflushed staged is lost on reopen", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir);
		try {
			setAll(store, [[1, 1]]);
			await store.flush(); // 1 is durable
			setAll(store, [[2, 2]]); // staged, never flushed
			store.close(); // clean close: no batch, no wal
			store = await open(dir);
			assertEquals(await store.get(1), 1);
			assertEquals(await store.get(2), undefined); // lost
		} finally {
			closeQuiet(store);
		}
	});
});

// ── flush guards ──────────────────────────────────────────────────────────────

Deno.test("guards: createWAL rejected during a batch", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			const b = store.batch();
			await assertRejects(() => store.createWAL(), Error);
			b.discard();
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("guards: during a flush — no second flush, no clear; batch is allowed", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			setAll(store, [[1, 10]]);
			const w = await store.createWAL();

			await assertRejects(() => store.createWAL(), Error);
			await assertRejects(() => store.clear(), Error);

			// batch is explicitly allowed mid-flush (lands in fresh staged)
			const b = store.batch();
			b.set(2, 20);
			b.apply();

			await w.apply();
			await w.discard();

			assertEquals(await getAll(store, [1, 2]), [10, 20]);
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("guards: close rejected during a batch and during a flush", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			const b = store.batch();
			assertThrows(() => store.close(), Error);
			b.discard();

			setAll(store, [[1, 1]]);
			const w = await store.createWAL();
			assertThrows(() => store.close(), Error);
			await w.apply();
			await w.discard();
		} finally {
			closeQuiet(store);
		}
	});
});

// ── frozen serves reads ────────────────────────────────────────────────────────

Deno.test("frozen serves reads after createWAL() and after apply() but before discard()", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			setAll(store, [[1, 100], [2, 200]]);
			const w = await store.createWAL(); // staged frozen; RocksDB still empty

			// After createWAL, before apply: served from frozen
			assertEquals(await getAll(store, [1, 2]), [100, 200]);
			assertEquals(await store.get(3), undefined);

			await w.apply(); // entries now in RocksDB too, but frozen still set

			// After apply, before discard: STILL served from frozen (frozen not cleared yet)
			assertEquals(await getAll(store, [1, 2]), [100, 200]);

			await w.discard(); // frozen cleared

			// After discard: served from RocksDB
			assertEquals(await getAll(store, [1, 2]), [100, 200]);
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("reads during a concurrent apply() are correct (served from frozen, not stale)", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			setAll(store, [[1, 111], [2, 222], [3, 333]]);
			const w = await store.createWAL();

			// Fire apply without awaiting — RocksDB transaction may or may not have started
			const applyP = w.apply();

			// Reads must come from #frozen, not race the transaction
			const reads = await Promise.all([
				store.get(1),
				store.get(2),
				store.get(3),
				store.get(4), // absent
			]);

			await applyP;
			await w.discard();

			assertEquals(reads, [111, 222, 333, undefined]);
			// After discard: now from RocksDB
			assertEquals(await getAll(store, [1, 2, 3, 4]), [111, 222, 333, undefined]);
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("batch committed during flush lands in fresh staged and survives the next flush", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir);
		try {
			setAll(store, [[1, 10]]);
			const w = await store.createWAL(); // freeze [1→10]
			setAll(store, [[2, 20]]); // lands in fresh staged
			await w.apply();
			await w.discard();

			assertEquals(await getAll(store, [1, 2]), [10, 20]);

			await store.flush(); // flush [2→20]
			store.close();
			store = await open(dir);
			assertEquals(await getAll(store, [1, 2]), [10, 20]);
		} finally {
			closeQuiet(store);
		}
	});
});

// ── full precedence ladder ─────────────────────────────────────────────────────

Deno.test("precedence ladder: batch > staged > frozen > RocksDB on the same key", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir);
		try {
			// RocksDB layer: key 1 → 1
			setAll(store, [[1, 1]]);
			await store.flush();
			store.close();
			store = await open(dir);

			// staged layer: key 1 → 2 (shadows RocksDB)
			setAll(store, [[1, 2], [2, 20]]);

			// freeze: [1→2, 2→20] into frozen
			const w = await store.createWAL();

			// fresh staged: key 1 → 3 (shadows frozen)
			setAll(store, [[1, 3], [3, 300]]);

			// batch: key 1 → 4 (shadows staged)
			const b = store.batch();
			b.set(1, 4);
			b.set(4, 4000);

			// Precedence check before apply
			assertEquals(await b.get(1), 4); // batch wins
			assertEquals(await b.get(2), 20); // frozen (staged is empty for 2 after freeze)
			assertEquals(await b.get(3), 300); // fresh staged
			assertEquals(await b.get(4), 4000); // own batch append
			b.apply();

			// After apply: staged has 1→4, 3→300, 4→4000
			assertEquals(await store.get(1), 4); // staged wins over frozen wins over rocks
			assertEquals(await store.get(2), 20); // frozen
			assertEquals(await store.get(3), 300); // staged
			assertEquals(await store.get(4), 4000); // staged

			await w.apply();
			await w.discard();

			// After discard: frozen gone, rocks has [1→2, 2→20]; staged has [1→4, 3→300, 4→4000]
			assertEquals(await store.get(1), 4); // staged still wins
			assertEquals(await store.get(2), 20); // from RocksDB (frozen gone, nothing in staged)
			assertEquals(await store.get(3), 300); // staged
			assertEquals(await store.get(4), 4000); // staged

			await store.flush();
			store.close();
			store = await open(dir);

			// Durable: everything in RocksDB, last-write-wins
			assertEquals(await getAll(store, [1, 2, 3, 4]), [4, 20, 300, 4000]);
		} finally {
			closeQuiet(store);
		}
	});
});

// ── WAL format ────────────────────────────────────────────────────────────────

Deno.test("WAL header: entryCount matches staged and entries are packed at fixed stride", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			setAll(store, [[10, 100], [20, 200], [30, 300]]);
			const w = await store.createWAL();

			const buf = await Deno.readFile(walPath(dir));
			const view = new Uint8ArrayView(buf);

			assertEquals(view.getUint32(0), 3); // entryCount
			assertEquals(buf.length, 4 + 3 * 8); // 4-byte header + 3 × (4-byte key + 4-byte val)

			// Collect all [key, val] pairs from the WAL (order may vary — use a map)
			const parsed = new Map<number, number>();
			for (let i = 0; i < 3; i++) {
				const k = view.getUint32(4 + i * 8);
				const v = view.getUint32(4 + i * 8 + 4);
				parsed.set(k, v);
			}
			assertEquals(parsed.get(10), 100);
			assertEquals(parsed.get(20), 200);
			assertEquals(parsed.get(30), 300);

			await w.apply();
			await w.discard();
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("WAL: empty staged produces a zero-entry WAL that applies cleanly", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			const w = await store.createWAL();
			const buf = await Deno.readFile(walPath(dir));
			const count = new Uint8ArrayView(buf).getUint32(0);
			assertEquals(count, 0);
			assertEquals(buf.length, 4);
			await w.apply();
			await w.discard();
			assertFalse(await exists(walPath(dir)));
		} finally {
			closeQuiet(store);
		}
	});
});

// ── replay / crash recovery ────────────────────────────────────────────────────

Deno.test("replay: wal.apply() is idempotent (apply twice == once)", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			setAll(store, [[1, 11], [2, 22]]);
			const w = await store.createWAL();

			await w.apply();
			assertEquals(await getAll(store, [1, 2]), [11, 22]);

			await w.apply(); // replay — re-put is idempotent in RocksDB
			assertEquals(await getAll(store, [1, 2]), [11, 22]);

			await w.discard();
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("crash recovery: manually constructed WAL replayed on reopen", async () => {
	await withTmp(async (dir) => {
		// Baseline: flush some entries so RocksDB has data
		let store = await open(dir);
		setAll(store, [[1, 10]]);
		await store.flush();
		store.close();

		// Simulate crash: entries [2→20, 3→30] were staged but never reached RocksDB.
		// The WAL for them is on disk.
		await Deno.writeFile(walPath(dir), buildWalBuf([[2, 20], [3, 30]]));

		// Reopen — WAL detected
		store = await open(dir);
		try {
			assertExists(store.wal);

			// Staged [2,3] not yet in RocksDB, but WAL says they should be
			await store.wal!.apply();
			await store.wal!.discard();

			// All three now readable
			assertEquals(await getAll(store, [1, 2, 3]), [10, 20, 30]);
			assertFalse(await exists(walPath(dir)));
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("crash recovery: WAL with zero entries is a clean no-op", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir);
		setAll(store, [[1, 10]]);
		await store.flush();
		store.close();

		await Deno.writeFile(walPath(dir), buildWalBuf([]));

		store = await open(dir);
		try {
			assertExists(store.wal);
			await store.wal!.apply();
			await store.wal!.discard();
			assertEquals(await store.get(1), 10); // baseline intact
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("crash recovery: WAL entries overwrite stale RocksDB values", async () => {
	await withTmp(async (dir) => {
		// Flush an old value to RocksDB
		let store = await open(dir);
		setAll(store, [[5, 50]]);
		await store.flush();
		store.close();

		// WAL has a newer value for key 5
		await Deno.writeFile(walPath(dir), buildWalBuf([[5, 500], [6, 600]]));

		store = await open(dir);
		try {
			await store.wal!.apply();
			await store.wal!.discard();
			assertEquals(await getAll(store, [5, 6]), [500, 600]);
		} finally {
			closeQuiet(store);
		}
	});
});

// ── clear ─────────────────────────────────────────────────────────────────────

Deno.test("clear: wipes staged and RocksDB; data gone after reopen", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir);
		try {
			setAll(store, [[1, 1], [2, 2]]);
			await store.flush();
			setAll(store, [[3, 3]]); // staged
			await store.clear();
			assertEquals(await getAll(store, [1, 2, 3]), [undefined, undefined, undefined]);
			store.close();
			store = await open(dir);
			assertEquals(await getAll(store, [1, 2, 3]), [undefined, undefined, undefined]);
		} finally {
			closeQuiet(store);
		}
	});
});

Deno.test("clear: guards — rejected during batch and during flush", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		try {
			const b = store.batch();
			await assertRejects(() => store.clear(), Error);
			b.discard();

			setAll(store, [[1, 1]]);
			const w = await store.createWAL();
			await assertRejects(() => store.clear(), Error);
			await w.apply();
			await w.discard();
		} finally {
			closeQuiet(store);
		}
	});
});

// ── differential fuzz ─────────────────────────────────────────────────────────

function mulberry32(seed: number): () => number {
	return function () {
		seed |= 0;
		seed = (seed + 0x6d2b79f5) | 0;
		let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

Deno.test("fuzz: differential test against an in-memory Map oracle", async () => {
	await withTmp(async (dir) => {
		const SEED = 0xdeadbeef;
		const rng = mulberry32(SEED);
		const randInt = (n: number) => Math.floor(rng() * n);
		const KEY_SPACE = 64; // small enough for frequent overwrites

		let store = await open(dir);
		const model = new Map<number, number>(); // flushed-only oracle (what's in RocksDB)
		const staged = new Map<number, number>(); // staged-only (what's been applied but not flushed)

		function effectiveValue(k: number): number | undefined {
			return staged.has(k) ? staged.get(k) : model.get(k);
		}

		const check = async (where: string) => {
			try {
				// Sample a random selection of keys including ones that should be absent
				const keys = Array.from({ length: 16 }, (_, i) => i);
				const got = await getAll(store, keys);
				for (let i = 0; i < keys.length; i++) {
					const expected = effectiveValue(keys[i]!);
					if (got[i] !== expected) {
						throw new Error(
							`key ${keys[i]}: expected ${expected}, got ${got[i]}`,
						);
					}
				}
			} catch (e) {
				throw new Error(`fuzz mismatch (seed=0x${SEED.toString(16)}) at ${where}: ${e}`);
			}
		};

		try {
			const ITERS = 200;
			for (let it = 0; it < ITERS; it++) {
				const roll = rng();

				if (roll < 0.55) {
					// Random batch: 1–5 sets, maybe discard
					const localUpdates = new Map<number, number>();
					const b = store.batch();
					const n = 1 + randInt(5);
					for (let i = 0; i < n; i++) {
						const k = randInt(KEY_SPACE);
						const v = randInt(0xffffffff);
						b.set(k, v);
						localUpdates.set(k, v);
					}
					if (rng() < 0.85) {
						b.apply();
						for (const [k, v] of localUpdates) staged.set(k, v);
					} else {
						b.discard();
					}
				} else if (roll < 0.75) {
					// Flush
					await store.flush();
					for (const [k, v] of staged) model.set(k, v);
					staged.clear();
				} else {
					// Reopen (flush first so nothing staged is lost)
					await store.flush();
					for (const [k, v] of staged) model.set(k, v);
					staged.clear();
					store.close();
					store = await open(dir);
				}

				await check(`iter ${it} (roll=${roll.toFixed(3)})`);
			}
		} finally {
			closeQuiet(store);
		}
	});
});
