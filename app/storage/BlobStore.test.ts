import { assertEquals, assertFalse, assertRejects, assertThrows } from "@std/assert";
import { exists } from "@std/fs";
import { join } from "@std/path";
import { U32 } from "@nomadshiba/codec";
import { BlobStore } from "./BlobStore.ts";

const u32 = (n: number) => U32.encode(n);

async function withStore(
	opts: { maxDiskChunkSize?: number; maxMemoryChunkSize?: number },
	fn: (store: BlobStore, path: string) => Promise<void>,
): Promise<void> {
	const path = await Deno.makeTempDir({ prefix: "blobstore-test-" });
	const store = await BlobStore.open({
		path,
		maxDiskChunkSize: opts.maxDiskChunkSize ?? 1024,
		maxMemoryChunkSize: opts.maxMemoryChunkSize ?? 1024,
	});
	try {
		await fn(store, path);
	} finally {
		store.close();
		await Deno.remove(path, { recursive: true }).catch(() => {});
	}
}

/** Sum of all on-disk chunk file sizes — the physical byte count of the disk region. */
async function diskBytes(path: string): Promise<number> {
	let total = 0;
	for await (const entry of Deno.readDir(path)) {
		if (entry.isFile && entry.name.startsWith("chunk_")) {
			total += (await Deno.stat(join(path, entry.name))).size;
		}
	}
	return total;
}

// ---------------------------------------------------------------------------
// batch: append / get / apply / discard
// ---------------------------------------------------------------------------

Deno.test("batch.get reads staged-but-uncommitted data, get reads it after apply", async () => {
	await withStore({}, async (store) => {
		const b = store.batch();
		const p = b.append(u32(55));
		assertEquals(await b.get(p, U32), 55); // from the open batch region
		b.apply();
		assertEquals(await store.get(p, U32), 55); // now from staged
	});
});

Deno.test("batch.append returns the logical start offset of each blob", async () => {
	await withStore({}, async (store) => {
		const b = store.batch();
		assertEquals(b.append(u32(1)), 0);
		assertEquals(b.append(u32(2)), 4);
		assertEquals(b.append(u32(3)), 8);
		assertEquals(b.size(), 12);
		b.apply();
		assertEquals(await store.get(0, U32), 1);
		assertEquals(await store.get(4, U32), 2);
		assertEquals(await store.get(8, U32), 3);
	});
});

Deno.test("discard drops batch data and resets the size accounting", async () => {
	await withStore({}, async (store) => {
		const b = store.batch();
		b.append(u32(99));
		assertEquals(b.size(), 4);
		b.discard();

		const b2 = store.batch();
		assertEquals(b2.size(), 0); // discarded bytes are gone, prevsize back to 0
		b2.discard();
	});
});

// ---------------------------------------------------------------------------
// the load-bearing invariant: pointers survive a flush unchanged
// ---------------------------------------------------------------------------

Deno.test("pointers are stable across flush (staged -> disk)", async () => {
	await withStore({ maxDiskChunkSize: 6, maxMemoryChunkSize: 6 }, async (store) => {
		const values = [10, 20, 30, 40];
		const b = store.batch();
		const ptrs = values.map((v) => b.append(u32(v)));
		b.apply();

		// read from staged
		for (let i = 0; i < values.length; i++) {
			assertEquals(await store.get(ptrs[i]!, U32), values[i]);
		}

		await store.flush();

		// same pointers, now served from disk
		for (let i = 0; i < values.length; i++) {
			assertEquals(await store.get(ptrs[i]!, U32), values[i]);
		}
	});
});

Deno.test("get routes to the correct region with disk and staged both populated", async () => {
	await withStore({}, async (store) => {
		let b = store.batch();
		const pa = b.append(u32(11));
		const pb = b.append(u32(22));
		b.apply();
		await store.flush(); // 11, 22 live on disk

		b = store.batch();
		const pc = b.append(u32(33));
		const pd = b.append(u32(44));
		b.apply(); // 33, 44 live in staged

		assertEquals(await store.get(pa, U32), 11); // disk
		assertEquals(await store.get(pb, U32), 22); // disk
		assertEquals(await store.get(pc, U32), 33); // staged
		assertEquals(await store.get(pd, U32), 44); // staged
	});
});

// ---------------------------------------------------------------------------
// chunk-boundary crossing in each region
// ---------------------------------------------------------------------------

Deno.test("memory region: a blob straddling a chunk boundary reads back whole", async () => {
	// 6-byte chunks, 4-byte values -> value at offset 4 spans chunk 0 and chunk 1
	await withStore({ maxMemoryChunkSize: 6 }, async (store) => {
		const b = store.batch();
		const ptrs: number[] = [];
		for (let i = 0; i < 5; i++) ptrs.push(b.append(u32(1000 + i)));
		b.apply();
		for (let i = 0; i < 5; i++) {
			assertEquals(await store.get(ptrs[i]!, U32), 1000 + i);
		}
	});
});

Deno.test("disk region: a blob straddling a chunk boundary reads back whole", async () => {
	await withStore({ maxDiskChunkSize: 6 }, async (store) => {
		const b = store.batch();
		const ptrs: number[] = [];
		for (let i = 0; i < 5; i++) ptrs.push(b.append(u32(2000 + i)));
		b.apply();
		await store.flush();
		for (let i = 0; i < 5; i++) {
			assertEquals(await store.get(ptrs[i]!, U32), 2000 + i);
		}
	});
});

// ---------------------------------------------------------------------------
// persistence across a close/reopen cycle
// ---------------------------------------------------------------------------

Deno.test("flushed data survives close and reopen (cross-chunk)", async () => {
	const path = await Deno.makeTempDir({ prefix: "blobstore-test-" });
	const opts = { path, maxDiskChunkSize: 6, maxMemoryChunkSize: 6 } as const;
	try {
		const ptrs: number[] = [];
		{
			const store = await BlobStore.open(opts);
			const b = store.batch();
			for (let i = 0; i < 3; i++) ptrs.push(b.append(u32(300 + i)));
			b.apply();
			await store.flush();
			store.close();
		}
		{
			const store = await BlobStore.open(opts);
			try {
				for (let i = 0; i < 3; i++) {
					assertEquals(await store.get(ptrs[i]!, U32), 300 + i);
				}
			} finally {
				store.close();
			}
		}
	} finally {
		await Deno.remove(path, { recursive: true }).catch(() => {});
	}
});

// ---------------------------------------------------------------------------
// truncate
// ---------------------------------------------------------------------------

Deno.test("truncate drops the physical tail and keeps surviving blobs readable", async () => {
	await withStore({ maxDiskChunkSize: 6 }, async (store, path) => {
		const b = store.batch();
		const ptrs = [1, 2, 3, 4, 5].map((v) => b.append(u32(v)));
		b.apply();
		await store.flush();
		assertEquals(await diskBytes(path), 20);

		await store.truncate(12); // keep the first three 4-byte values
		assertEquals(await diskBytes(path), 12);

		for (let i = 0; i < 3; i++) {
			assertEquals(await store.get(ptrs[i]!, U32), i + 1);
		}
	});
});

// ---------------------------------------------------------------------------
// rollback (WAL-style: rollback.size records the pre-flush disk size)
// ---------------------------------------------------------------------------

Deno.test("rollback undoes the most recent flush", async () => {
	await withStore({}, async (store, path) => {
		assertFalse(await exists(join(path, "rollback.size"))); // none before any flush

		let b = store.batch();
		const px = b.append(u32(777));
		b.apply();
		await store.pin();
		await store.flush(); // rollback.size = 0
		const afterX = await diskBytes(path);

		assertEquals(await exists(join(path, "rollback.size")), true);

		b = store.batch();
		b.append(u32(888));
		b.apply();
		await store.pin();
		await store.flush(); // rollback.size = afterX
		assertEquals(await diskBytes(path), afterX + 4);

		await store.rollback(); // truncate back to afterX
		assertEquals(await diskBytes(path), afterX);
		assertEquals(await store.get(px, U32), 777); // X is intact
	});
});

// ---------------------------------------------------------------------------
// concurrency / state guards
// ---------------------------------------------------------------------------

Deno.test("concurrent gets use independent read buffers", async () => {
	await withStore({}, async (store) => {
		const b = store.batch();
		const first = b.append(u32(11));
		const second = b.append(u32(22));
		b.apply();

		assertEquals(await Promise.all([store.get(first, U32), store.get(second, U32)]), [11, 22]);
	});
});

Deno.test("a second concurrent batch throws", async () => {
	await withStore({}, async (store) => {
		const b = store.batch();
		assertThrows(() => store.batch(), Error, "concurrent batches");
		b.discard();
	});
});

Deno.test("flush while flushing rejects", async () => {
	await withStore({}, async (store) => {
		const b = store.batch();
		b.append(u32(1));
		b.apply();

		const first = store.flush(); // flips `flushing` before its first await
		await assertRejects(() => store.flush(), Error, "already flushing");
		await first;
	});
});

Deno.test("truncate while a batch is open rejects", async () => {
	await withStore({}, async (store) => {
		const b = store.batch();
		await assertRejects(() => store.truncate(0), Error, "batch is open");
		b.discard();
	});
});

Deno.test("truncate while staged data is present rejects", async () => {
	await withStore({}, async (store) => {
		const b = store.batch();
		b.append(u32(1));
		b.apply(); // staged now non-empty, not yet flushed
		await assertRejects(() => store.truncate(0), Error, "staged data is present");
	});
});

// ---------------------------------------------------------------------------
// degenerate cases
// ---------------------------------------------------------------------------

Deno.test("flush on an empty store is a no-op and stays consistent", async () => {
	await withStore({}, async (store, path) => {
		await store.flush();
		assertEquals(await diskBytes(path), 0);

		const b = store.batch();
		const p = b.append(u32(7));
		b.apply();
		await store.flush();
		assertEquals(await store.get(p, U32), 7);
	});
});
