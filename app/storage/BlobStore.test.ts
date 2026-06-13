/**
 * BlobStore test suite.
 *
 * Place next to BlobStore.ts (e.g. app/lib/storage/BlobStore.test.ts) and run:
 *   deno test --allow-read --allow-write app/lib/storage/BlobStore.test.ts
 *
 * Coverage:
 *   - open / disk-length detection across chunk files
 *   - batch semantics (append, own-read, size, discard, settle, one-at-a-time)
 *   - exact-length reads (EOF throw) and codec reads (EOF-tolerant readahead)
 *   - multi-chunk single-blob writes and sub-range reads crossing chunk boundaries
 *   - flush: persistence, empty no-op, accumulation, chunk-layout invariant
 *   - durability (flushed survives reopen) vs volatility (unflushed lost)
 *   - truncate: requires empty staged (flush first), disk shrink, EXACT chunk-boundary
 *     regression, to-zero, crash-safe sentinel (truncate.target replayed on open),
 *     guards, bounds
 *   - flush guards (no double flush, no truncate mid-flush, batch ALLOWED mid-flush)
 *   - reads served from frozen during a concurrent apply() (no torn reads)
 *   - frozen/staged stitch across a chunk boundary mid-flush
 *   - WAL header format
 *   - replay idempotency, self-heal of a torn chunk, crash recovery incl. torn disk
 *   - differential fuzz vs an in-memory byte-stream oracle
 *
 * Uses a tiny chunkByteSize so multi-chunk / boundary paths are exercised cheaply.
 * Note BlobStore holds no persistent fd, so a "crash" is just abandoning the instance
 * and reopening the same dir — no fd dance required.
 */

import { assertEquals, assertExists, assertFalse, assertRejects, assertThrows } from "@std/assert";
import { exists } from "@std/fs";
import { join } from "@std/path";
import { U32 } from "@nomadshiba/codec";
import { BlobStore } from "./BlobStore.ts";
import { concat } from "@std/bytes";
import { Uint8ArrayView } from "~/utils/Uint8ArrayView.ts";

const CHUNK = 16;

// Stores opened during the current withTmp scope, closed in its finally so the
// persistent read/append fds the store now caches don't trip Deno's leak sanitizer.
let openStores: BlobStore[] = [];

function open(dir: string, chunkByteSize = CHUNK) {
	return BlobStore.open({ path: dir, chunkByteSize }).then((store) => {
		openStores.push(store);
		return store;
	});
}

async function withTmp(fn: (dir: string) => Promise<void>): Promise<void> {
	const dir = await Deno.makeTempDir({ prefix: "blobstore_test_" });
	const prev = openStores;
	openStores = [];
	try {
		await fn(dir);
	} finally {
		for (const store of openStores) store.close();
		openStores = prev;
		await Deno.remove(dir, { recursive: true }).catch(() => {});
	}
}

/** Bytes with identifiable, position-derived content. */
function seq(start: number, len: number): Uint8Array {
	const u = new Uint8Array(len);
	for (let i = 0; i < len; i++) u[i] = (start + i) & 0xff;
	return u;
}

function appendBlob(store: BlobStore, data: Uint8Array): number {
	const b = store.batch();
	const p = b.append(data);
	b.apply();
	return p;
}

async function chunkSize(dir: string, i: number): Promise<number> {
	return (await Deno.stat(join(dir, `chunk_${i}`))).size;
}

function truncateTargetPath(dir: string): string {
	return join(dir, "truncate.target");
}

async function appendGarbageToChunk(dir: string, i: number, n: number): Promise<void> {
	const f = await Deno.open(join(dir, `chunk_${i}`), { write: true, create: true });
	try {
		await f.seek(0, Deno.SeekMode.End);
		await f.write(new Uint8Array(n).fill(0xff));
	} finally {
		f.close();
	}
}

async function expectRange(store: BlobStore, model: Uint8Array, p: number, len: number): Promise<void> {
	assertEquals(await store.get(p, len), model.slice(p, p + len));
}

// ── open / detection ──────────────────────────────────────────────────────────

Deno.test("open: empty store has length 0 and no chunk files", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		assertEquals(store.length(), 0);
		assertFalse(await exists(join(dir, "chunk_0")));
	});
});

Deno.test("open: detects existing chunks and computes length", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir);
		const data = seq(0, CHUNK + 5); // fills chunk_0, partial chunk_1
		appendBlob(store, data);
		await store.flush();
		store = await open(dir);
		assertEquals(store.length(), CHUNK + 5);
		assertEquals(await store.get(0, CHUNK + 5), data);
	});
});

// ── batch semantics ───────────────────────────────────────────────────────────

Deno.test("batch: append/apply makes bytes readable; pointer & length track", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		const b = store.batch();
		const d = seq(0, 5);
		assertEquals(b.append(d), 0);
		assertEquals(b.append(seq(100, 3)), 5);
		assertEquals(b.size(), 8);
		assertEquals(store.length(), 0); // invisible until apply
		b.apply();
		assertEquals(store.length(), 8);
		assertEquals(await store.get(0, 8), concat([seq(0, 5), seq(100, 3)]));
	});
});

Deno.test("batch: discard leaves the store unchanged", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		appendBlob(store, seq(0, 4));
		const b = store.batch();
		b.append(seq(50, 10));
		b.discard();
		assertEquals(store.length(), 4);
		assertEquals(await store.get(0, 4), seq(0, 4));
	});
});

Deno.test("batch: reads its own uncommitted appends and falls through below base", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		appendBlob(store, seq(0, 4));
		const b = store.batch();
		b.append(seq(50, 6)); // pointer 4
		assertEquals(await b.get(4, 6), seq(50, 6)); // own append
		assertEquals(await b.get(0, 4), seq(0, 4)); // falls through to committed
		assertEquals(await b.get(2, 6), concat([seq(2, 2), seq(50, 4)])); // spans both
		b.discard();
	});
});

Deno.test("batch: one at a time; settled batch throws", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		const b = store.batch();
		assertThrows(() => store.batch(), Error);
		b.apply();
		assertThrows(() => b.append(seq(0, 1)), Error);
		assertThrows(() => b.apply(), Error);
		await assertRejects(() => b.get(0, 1), Error);
		store.batch().discard(); // ok again
	});
});

// ── reads: exact + codec ────────────────────────────────────────────────────────

Deno.test("get(pointer, length): exact read; EOF and negative pointer reject", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		appendBlob(store, seq(0, 10));
		await store.flush();
		assertEquals(await store.get(3, 4), seq(3, 4));
		await assertRejects(() => store.get(8, 5), Error); // runs past end
		await assertRejects(() => store.get(-1, 1), Error);
		assertEquals(await store.get(10, 0), new Uint8Array(0)); // empty at end
	});
});

Deno.test("get(pointer, codec): decodes and tolerates EOF near the tail", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		const a = U32.encode(0xdeadbeef);
		const b = U32.encode(0x01020304);
		appendBlob(store, concat([a, b])); // two u32s
		await store.flush();
		assertEquals(await store.get(0, U32), 0xdeadbeef);
		assertEquals(await store.get(4, U32), 0x01020304);
		// last value sits exactly at the tail; a large readahead must still decode it
		assertEquals(await store.get(4, U32, { readAheadSize: 1024 }), 0x01020304);
	});
});

// ── multi-chunk spanning ────────────────────────────────────────────────────────

Deno.test("multi-chunk: a single blob larger than a chunk reads back across boundaries", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir, CHUNK);
		const data = seq(0, CHUNK * 2 + 5); // spans chunk_0, chunk_1, chunk_2
		appendBlob(store, data);
		await store.flush();
		assertEquals(await store.get(0, data.length), data);
		assertEquals(await store.get(CHUNK - 3, 6), seq(CHUNK - 3, 6)); // crosses 0→1
		assertEquals(await store.get(CHUNK * 2 - 2, 4), seq(CHUNK * 2 - 2, 4)); // crosses 1→2
		assertEquals(await chunkSize(dir, 0), CHUNK);
		assertEquals(await chunkSize(dir, 1), CHUNK);
		assertEquals(await chunkSize(dir, 2), 5);
	});
});

// ── flush / persistence ─────────────────────────────────────────────────────────

Deno.test("flush: persists to chunk files, removes wal, clears frozen", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		const data = seq(0, 20);
		appendBlob(store, data);
		await store.flush();
		assertEquals(store.wal, null);
		assertFalse(await exists(join(dir, "data.wal")));
		assertEquals(await store.get(0, 20), data);
	});
});

Deno.test("flush: empty staged is a clean no-op", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		appendBlob(store, seq(0, 5));
		await store.flush();
		await store.flush();
		assertEquals(await store.get(0, 5), seq(0, 5));
		assertEquals(store.length(), 5);
	});
});

Deno.test("flush: sequential flushes accumulate across chunk boundaries", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir, CHUNK);
		const d1 = seq(0, 10), d2 = seq(100, 10), d3 = seq(200, 10);
		appendBlob(store, d1);
		await store.flush();
		appendBlob(store, d2);
		await store.flush();
		appendBlob(store, d3);
		await store.flush();
		assertEquals(await store.get(0, 30), concat([d1, d2, d3]));
	});
});

Deno.test("chunk-layout invariant: all but the last chunk are full and sizes sum to length", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir, CHUNK);
		appendBlob(store, seq(0, CHUNK * 3 + 7));
		await store.flush();
		let total = 0;
		const sizes: number[] = [];
		for await (const e of Deno.readDir(dir)) {
			if (e.isFile && e.name.startsWith("chunk_")) {
				const i = parseInt(e.name.slice(6), 10);
				sizes[i] = (await Deno.stat(join(dir, e.name))).size;
			}
		}
		for (let i = 0; i < sizes.length; i++) {
			total += sizes[i]!;
			if (i < sizes.length - 1) assertEquals(sizes[i], CHUNK, `chunk_${i} should be full`);
		}
		assertEquals(total, store.length());
	});
});

Deno.test("durability: flushed data survives reopen", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir, CHUNK);
		const data = seq(0, CHUNK + 9);
		appendBlob(store, data);
		await store.flush();
		store = await open(dir, CHUNK);
		assertEquals(store.length(), data.length);
		assertEquals(await store.get(0, data.length), data);
	});
});

Deno.test("volatility: unflushed staged is lost on reopen", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir, CHUNK);
		appendBlob(store, seq(0, 8));
		await store.flush();
		appendBlob(store, seq(100, 8)); // staged, never flushed
		store = await open(dir, CHUNK);
		assertEquals(store.length(), 8);
		assertEquals(await store.get(0, 8), seq(0, 8));
	});
});

// ── truncate ──────────────────────────────────────────────────────────────────

Deno.test("truncate: shrinks disk; reopen reflects it", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir, CHUNK);
		appendBlob(store, seq(0, 20));
		await store.flush(); // disk [0,20)
		await store.truncate(10);
		assertEquals(store.length(), 10);
		assertEquals(await store.get(0, 10), seq(0, 10));
		await store.flush();
		store = await open(dir, CHUNK);
		assertEquals(store.length(), 10);
		assertEquals(await store.get(0, 10), seq(0, 10));
	});
});

Deno.test("truncate: rejects if staged is not empty (must flush first)", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir, CHUNK);
		appendBlob(store, seq(0, 10));
		await store.flush();
		appendBlob(store, seq(100, 5)); // staged but not flushed
		await assertRejects(() => store.truncate(8), Error);
	});
});

Deno.test("truncate: exact chunk-boundary keeps the preceding chunk intact (regression)", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir, 16);
		const data = seq(0, 32); // exactly fills chunk_0 and chunk_1
		appendBlob(store, data);
		await store.flush();
		assertEquals(await chunkSize(dir, 0), 16);
		assertEquals(await chunkSize(dir, 1), 16);

		await store.truncate(16); // boundary-aligned — must NOT wipe chunk_0

		assertEquals(store.length(), 16);
		assertEquals(await store.get(0, 16), data.slice(0, 16));
		assertEquals(await chunkSize(dir, 0), 16);
		assertFalse(await exists(join(dir, "chunk_1")));
	});
});

Deno.test("truncate: to zero removes everything", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir, CHUNK);
		appendBlob(store, seq(0, CHUNK * 2));
		await store.flush();
		await store.truncate(0);
		assertEquals(store.length(), 0);
		assertFalse(await exists(join(dir, "chunk_0")));
		assertFalse(await exists(join(dir, "chunk_1")));
	});
});

Deno.test("truncate: crash-safe sentinel removed after clean truncate", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir, CHUNK);
		appendBlob(store, seq(0, 20));
		await store.flush();
		await store.truncate(10);
		assertFalse(await exists(truncateTargetPath(dir)));
		assertEquals(store.length(), 10);
	});
});

Deno.test("truncate: crash recovery — sentinel replayed on open shrinks the chunks", async () => {
	await withTmp(async (dir) => {
		// Build a store with 3 chunks' worth of data
		let store = await open(dir, CHUNK);
		appendBlob(store, seq(0, CHUNK * 2 + 5));
		await store.flush();
		store = await open(dir, CHUNK); // reopen to release any state

		// Simulate: truncate started (sentinel written) but process died before chunk removal.
		const targetOffset = CHUNK + 3;
		const sentinel = new Uint8Array(8);
		new Uint8ArrayView(sentinel).setBigUint64(0, BigInt(targetOffset));
		await Deno.writeFile(truncateTargetPath(dir), sentinel);

		// Reopen — should replay the truncate
		store = await open(dir, CHUNK);
		try {
			assertEquals(store.length(), targetOffset);
			assertEquals(await store.get(0, targetOffset), seq(0, targetOffset));
			assertFalse(await exists(truncateTargetPath(dir)));
			assertFalse(await exists(join(dir, "chunk_2")));
		} finally {
			/*  */
		}
	});
});

Deno.test("truncate: bounds and guards", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		appendBlob(store, seq(0, 5));
		await store.flush();
		await assertRejects(() => store.truncate(-1), Error);
		await assertRejects(() => store.truncate(6), Error); // beyond flushed length

		const b = store.batch();
		await assertRejects(() => store.truncate(1), Error); // during a batch
		b.discard();

		const w = await store.createWAL();
		await assertRejects(() => store.truncate(1), Error); // during a flush
		await w.apply();
		await w.discard();
	});
});

// ── flush guards ──────────────────────────────────────────────────────────────

Deno.test("guards: createWAL rejected during a batch", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir);
		const b = store.batch();
		await assertRejects(() => store.createWAL(), Error);
		b.discard();
	});
});

Deno.test("guards: during a flush — no second flush, no truncate; batch is allowed", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir, CHUNK);
		appendBlob(store, seq(0, 8));
		const w = await store.createWAL();

		await assertRejects(() => store.createWAL(), Error);
		await assertRejects(() => store.truncate(0), Error);

		const b = store.batch(); // explicitly allowed mid-flush
		b.append(seq(200, 4));
		b.apply();

		await w.apply();
		await w.discard();
		assertEquals(await store.get(0, 12), concat([seq(0, 8), seq(200, 4)]));
	});
});

// ── concurrency / stitch ────────────────────────────────────────────────────────

Deno.test("reads during a concurrent apply() are correct (served from frozen)", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir, CHUNK);
		const d1 = seq(0, CHUNK * 2); // disk
		appendBlob(store, d1);
		await store.flush();
		const d2 = seq(100, CHUNK * 2); // staged → frozen
		appendBlob(store, d2);

		const w = await store.createWAL();
		const applyP = w.apply(); // not awaited
		const model = concat([d1, d2]);
		const reads = await Promise.all([
			store.get(0, CHUNK * 2), // pure disk
			store.get(CHUNK * 2 - 2, 4), // crosses disk → frozen boundary
			store.get(CHUNK * 2, CHUNK * 2), // pure frozen
			store.get(0, CHUNK * 4), // the whole thing
		]);
		await applyP;
		await w.discard();

		assertEquals(reads[0], model.slice(0, CHUNK * 2));
		assertEquals(reads[1], model.slice(CHUNK * 2 - 2, CHUNK * 2 + 2));
		assertEquals(reads[2], model.slice(CHUNK * 2, CHUNK * 4));
		assertEquals(reads[3], model);
		assertEquals(await store.get(0, CHUNK * 4), model); // from disk after discard
	});
});

Deno.test("frozen/staged stitch across a chunk boundary mid-flush", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir, CHUNK);
		const d1 = seq(0, 10); // disk [0,10)
		appendBlob(store, d1);
		await store.flush();
		const d2 = seq(100, 10); // staged [10,20) — crosses the 16-byte boundary
		appendBlob(store, d2);

		const w = await store.createWAL(); // freeze d2; base 10
		const d3 = seq(200, 5); // fresh staged [20,25)
		appendBlob(store, d3);

		const model = concat([d1, d2, d3]);
		await expectRange(store, model, 5, 10); // disk → frozen
		await expectRange(store, model, 5, 20); // disk → frozen → staged
		assertEquals(await store.get(0, 25), model);

		await w.apply();
		await w.discard();
		assertEquals(await store.get(0, 25), model); // disk (d1+d2) + staged (d3)

		await store.flush();
		store = await open(dir, CHUNK);
		assertEquals(await store.get(0, 25), model);
	});
});

Deno.test("a batch committed during a flush survives the next flush", async () => {
	await withTmp(async (dir) => {
		let store = await open(dir, CHUNK);
		const d1 = seq(0, 8);
		appendBlob(store, d1);
		await store.flush();
		const d2 = seq(50, 8);
		appendBlob(store, d2);
		const w = await store.createWAL();
		const d3 = seq(150, 8);
		appendBlob(store, d3); // fresh layer
		await w.apply();
		await w.discard();
		const model = concat([d1, d2, d3]);
		assertEquals(await store.get(0, 24), model);
		await store.flush();
		store = await open(dir, CHUNK);
		assertEquals(await store.get(0, 24), model);
	});
});

// ── WAL format ──────────────────────────────────────────────────────────────────

Deno.test("WAL header records base_offset and byte length", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir, CHUNK);
		appendBlob(store, seq(0, 10));
		await store.flush(); // base will be 10
		const b = store.batch();
		b.append(seq(100, 3));
		b.append(seq(200, 3));
		b.apply();
		const w = await store.createWAL();
		const wal = await Deno.readFile(join(dir, "data.wal"));
		const view = new Uint8ArrayView(wal);
		assertEquals(Number(view.getBigUint64(0)), 10); // base_offset
		assertEquals(Number(view.getBigUint64(8)), 6); // 6 staged bytes (two 3-byte blobs)
		assertEquals(wal.length, 16 + 6); // header + payload
		await w.apply();
		await w.discard();
	});
});

// ── replay / self-heal / recovery ────────────────────────────────────────────────

Deno.test("replay: wal.apply() is idempotent (apply twice == once)", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir, CHUNK);
		const d1 = seq(0, 10);
		appendBlob(store, d1);
		await store.flush();
		const d2 = seq(100, 12);
		appendBlob(store, d2);
		const w = await store.createWAL();
		await w.apply();
		await w.apply(); // replay
		await w.discard();
		assertEquals(await store.get(0, 22), concat([d1, d2]));
		// disk physically ends at 22
		let total = 0;
		for await (const e of Deno.readDir(dir)) {
			if (e.isFile && e.name.startsWith("chunk_")) total += (await Deno.stat(join(dir, e.name))).size;
		}
		assertEquals(total, 22);
	});
});

Deno.test("self-heal: apply() truncates a torn chunk then rewrites", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir, CHUNK);
		const d1 = seq(0, 10);
		appendBlob(store, d1);
		await store.flush(); // chunk_0 = 10 bytes
		const d2 = seq(100, 12);
		appendBlob(store, d2);
		const w = await store.createWAL(); // base 10

		await appendGarbageToChunk(dir, 0, 4); // chunk_0 now 14 bytes (torn)

		await w.apply(); // truncate to 10, rewrite d2
		await w.discard(); // drop frozen → reads come from disk
		assertEquals(await store.get(0, 22), concat([d1, d2]));
	});
});

Deno.test("crash recovery: a WAL present on reopen replays the in-flight appends", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir, CHUNK);
		const d1 = seq(0, 10);
		appendBlob(store, d1);
		await store.flush();
		const d2 = seq(100, 12);
		appendBlob(store, d2);
		await store.createWAL(); // WAL on disk; chunk data still just d1

		// "crash": abandon the instance (no fd held) and reopen the same dir
		const store2 = await open(dir, CHUNK);
		assertExists(store2.wal);
		await store2.wal!.apply();
		await store2.wal!.discard();
		assertEquals(store2.length(), 22);
		assertEquals(await store2.get(0, 22), concat([d1, d2]));
		assertFalse(await exists(join(dir, "data.wal")));
	});
});

Deno.test("crash recovery: torn disk + WAL present recovers cleanly", async () => {
	await withTmp(async (dir) => {
		const store = await open(dir, CHUNK);
		const d1 = seq(0, 10);
		appendBlob(store, d1);
		await store.flush();
		const d2 = seq(100, 12);
		appendBlob(store, d2);
		await store.createWAL();

		await appendGarbageToChunk(dir, 0, 4); // partial-write garbage from the "crash"

		const store2 = await open(dir, CHUNK);
		assertExists(store2.wal);
		await store2.wal!.apply(); // base from wal heals the torn tail
		await store2.wal!.discard();
		assertEquals(await store2.get(0, 22), concat([d1, d2]));
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

Deno.test("fuzz: differential test against an in-memory byte-stream oracle", async () => {
	await withTmp(async (dir) => {
		const SEED = 0x5eed1234;
		const rng = mulberry32(SEED);
		const randInt = (n: number) => Math.floor(rng() * n);
		const FUZZ_CHUNK = 32;

		let store = await open(dir, FUZZ_CHUNK);
		let model = new Uint8Array(0);
		let flushedLen = 0;
		let counter = 0;
		const nextBlob = (len: number) => seq(counter += 7, len);

		const check = async (where: string) => {
			try {
				assertEquals(store.length(), model.length);
				if (model.length > 0) {
					const p = randInt(model.length);
					const len = randInt(model.length - p + 1);
					assertEquals(await store.get(p, len), model.slice(p, p + len));
					assertEquals(await store.get(0, model.length), model);
				}
			} catch (e) {
				throw new Error(`fuzz mismatch (seed=${SEED.toString(16)}) at ${where}: ${e}`);
			}
		};

		try {
			const ITERS = 250;
			for (let it = 0; it < ITERS; it++) {
				const roll = rng();

				if (roll < 0.55) {
					const parts: Uint8Array[] = [];
					const b = store.batch();
					const n = 1 + randInt(4);
					for (let i = 0; i < n; i++) {
						const blob = nextBlob(1 + randInt(FUZZ_CHUNK * 2));
						b.append(blob);
						parts.push(blob);
					}
					if (rng() < 0.85) {
						b.apply();
						model = concat([model, ...parts]);
					} else {
						b.discard();
					}
				} else if (roll < 0.7) {
					await store.flush();
					flushedLen = model.length;
				} else if (roll < 0.82 && flushedLen > 0) {
					// Truncate requires empty staged — flush first
					await store.flush();
					flushedLen = model.length;
					const newLen = randInt(flushedLen + 1);
					await store.truncate(newLen);
					model = model.slice(0, newLen);
					flushedLen = newLen;
				} else {
					await store.flush(); // persist before reopening
					flushedLen = model.length;
					store = await open(dir, FUZZ_CHUNK);
				}

				await check(`iter ${it} (roll=${roll.toFixed(3)})`);
			}
		} catch (e) {
			throw e;
		}
	});
});
