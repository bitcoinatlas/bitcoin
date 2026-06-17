import { assertEquals, assertRejects, assertThrows } from "@std/assert";
import { U64 } from "@nomadshiba/codec";
import { IndexStore } from "./IndexStore.ts";

// Default chunk sizing: big enough that everything lands in one chunk unless a
// test deliberately picks a small itemsPerChunk to exercise chunk spanning.
const BIG = 64;

async function withTemp(fn: (path: string) => Promise<void>): Promise<void> {
	const path = await Deno.makeTempDir({ prefix: "indexstore_test_" });
	try {
		await fn(path);
	} finally {
		await Deno.remove(path, { recursive: true });
	}
}

function appendAll(store: IndexStore<typeof U64>, values: bigint[]): void {
	const b = store.batch();
	for (const v of values) b.push(v);
	b.apply();
}

async function readAll(store: IndexStore<typeof U64>): Promise<bigint[]> {
	const out: bigint[] = [];
	for (let i = 0; i < store.length(); i++) out.push(await store.get(i));
	return out;
}

Deno.test("append then get reads staged values before any flush", async () => {
	await withTemp(async (path) => {
		using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
		appendAll(store, [10n, 20n, 30n]);

		assertEquals(store.length(), 3);
		assertEquals(await store.get(0), 10n);
		assertEquals(await store.get(1), 20n);
		assertEquals(await store.get(2), 30n);
	});
});

Deno.test("set overwrites a staged slot", async () => {
	await withTemp(async (path) => {
		using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
		appendAll(store, [10n, 20n, 30n]);

		const b = store.batch();
		b.set(1, 99n);
		b.apply();

		assertEquals(store.length(), 3);
		assertEquals(await store.get(1), 99n);
		assertEquals(await store.get(0), 10n);
	});
});

Deno.test("batch.set out of bounds throws", async () => {
	await withTemp(async (path) => {
		using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
		const b = store.batch();
		assertThrows(() => b.set(0, 1n), RangeError); // length is 0
		b.discard();
	});
});

Deno.test("discard drops staged changes", async () => {
	await withTemp(async (path) => {
		using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
		appendAll(store, [10n, 20n, 30n]);

		const b = store.batch();
		b.push(40n);
		b.set(0, 99n);
		b.discard();

		assertEquals(store.length(), 3);
		assertEquals(await store.get(0), 10n);
	});
});

Deno.test("batch sees its own pending writes", async () => {
	await withTemp(async (path) => {
		using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
		const b = store.batch();
		const idx = b.push(10n);
		assertEquals(await b.get(idx), 10n);
		b.set(idx, 5n);
		assertEquals(await b.get(idx), 5n);
		assertEquals(b.length(), 1);
		b.discard();
	});
});

Deno.test("concurrent batch throws", async () => {
	await withTemp(async (path) => {
		using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
		const b = store.batch();
		assertThrows(() => store.batch());
		b.discard();
	});
});

Deno.test("get out of bounds rejects", async () => {
	await withTemp(async (path) => {
		using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
		appendAll(store, [10n]);
		await assertRejects(() => store.get(5), RangeError);
	});
});

Deno.test("flush persists across reopen", async () => {
	await withTemp(async (path) => {
		{
			using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
		appendAll(store, [10n, 20n, 30n]);
		await store.pin();
		await store.flush();
	}
	{
		using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
		assertEquals(store.length(), 3);
		assertEquals(await readAll(store), [10n, 20n, 30n]);
		}
	});
});

Deno.test("set on a disk-resident slot persists across reopen", async () => {
	await withTemp(async (path) => {
		{
			using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
		appendAll(store, [10n, 20n, 30n]);
		await store.pin();
		await store.flush();

		const b = store.batch();
		b.set(1, 99n);
		b.apply();
		await store.pin();
		await store.flush();
		}
		{
			using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
			assertEquals(await readAll(store), [10n, 99n, 30n]);
		}
	});
});

Deno.test("appends spanning multiple chunks persist", async () => {
	await withTemp(async (path) => {
		const values = [1n, 2n, 3n, 4n, 5n];
		{
			// itemsPerChunk=2 with stride 8 => 16-byte chunks; 5 items => 3 chunks.
			using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: 2 });
		appendAll(store, values);
		await store.pin();
		await store.flush();
		}
		{
			using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: 2 });
			assertEquals(store.length(), 5);
			assertEquals(await readAll(store), values);
		}
	});
});

Deno.test("rollback restores overwritten values and truncates appends", async () => {
	await withTemp(async (path) => {
		using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });

		appendAll(store, [10n, 20n, 30n]);
		await store.pin();
		await store.flush(); // disk: [10, 20, 30]

		const b = store.batch();
		b.set(0, 111n);
		b.set(2, 333n);
		b.push(40n);
		b.apply();
		await store.pin();
		await store.flush(); // disk: [111, 20, 333, 40], WAL: oldLength=3, {0:10, 2:30}

		assertEquals(store.length(), 4);
		assertEquals(await store.get(0), 111n);

		await store.rollback(); // undo the most recent flush

		assertEquals(store.length(), 3);
		assertEquals(await readAll(store), [10n, 20n, 30n]);
	});
});

Deno.test("rollback with no prior flush is a no-op", async () => {
	await withTemp(async (path) => {
		using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
		await store.rollback(); // WAL file does not exist
		assertEquals(store.length(), 0);
	});
});

Deno.test("truncate shrinks length and drops items", async () => {
	await withTemp(async (path) => {
		using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
		appendAll(store, [1n, 2n, 3n, 4n, 5n]);
		await store.pin();
		await store.flush();

		await store.truncate(2);

		assertEquals(store.length(), 2);
		assertEquals(await store.get(1), 2n);
		await assertRejects(() => store.get(2), RangeError);
	});
});

Deno.test("truncate is rejected while a batch is open", async () => {
	await withTemp(async (path) => {
		using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
		appendAll(store, [1n, 2n, 3n]);
		await store.pin();
		await store.flush();

		const b = store.batch();
		await assertRejects(() => store.truncate(1));
		b.discard();
	});
});

Deno.test("truncate is rejected while staged data is present", async () => {
	await withTemp(async (path) => {
		using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
		appendAll(store, [1n, 2n, 3n]); // staged, not flushed
		await assertRejects(() => store.truncate(1));
	});
});

Deno.test("a batch applied during an in-flight flush survives", async () => {
	await withTemp(async (path) => {
		{
			using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });

			appendAll(store, [10n, 20n, 30n]);
			await store.pin();
			await store.flush(); // disk: [10, 20, 30]

			// Stage an overwrite of slot 1, then pin and start flushing it.
			const a = store.batch();
			a.set(1, 222n);
			a.apply();

			// pin() freezes staged and opens a fresh staged layer. flush() then
			// applies the frozen snapshot to disk; while it is in-flight, the batch
			// below lands in the fresh staged layer and is not touched by the flush.
			await store.pin();
			const p = store.flush();

			const b = store.batch();
			b.set(2, 333n);
			b.apply();

			await p;

			// slot 1 was flushed to disk; slot 2 is shadowed by the live staged layer.
			assertEquals(await store.get(1), 222n);
			assertEquals(await store.get(2), 333n);

			await store.pin();
			await store.flush();
		}
		{
			using store = await IndexStore.open({ path, codec: U64, itemsPerChunk: BIG });
			assertEquals(await readAll(store), [10n, 222n, 333n]);
		}
	});
});
