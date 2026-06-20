/**
 * Tests for reading at an interior byte offset within a blob that was
 * appended to the current batch buffer — exactly the pattern used in
 * chain.ts:280:
 *
 *   const txSpenderOffset = await batch.tx.get(txPointer + Bytes32.stride.size, U40);
 *
 * i.e. append a multi-field blob, then read back a field at a non-zero
 * offset within that same blob, all within a single open batch.
 */
import { assertEquals } from "@std/assert";
import { U32 } from "@nomadshiba/codec";
import { U40 } from "~/codec/primitives/U40.ts";
import { BlobStore } from "~/libs/storage/BlobStore.ts";

async function withStore(
	opts: { maxDiskChunkSize?: number; maxMemoryChunkSize?: number },
	fn: (store: BlobStore) => Promise<void>,
): Promise<void> {
	const path = await Deno.makeTempDir({ prefix: "blobstore-interior-" });
	const store = await BlobStore.open({
		path,
		maxDiskChunkSize: opts.maxDiskChunkSize ?? 1024,
		maxMemoryChunkSize: opts.maxMemoryChunkSize ?? 1024,
	});
	try {
		await fn(store);
	} finally {
		store.close();
		await Deno.remove(path, { recursive: true }).catch(() => {});
	}
}

// ---------------------------------------------------------------------------
// batch.get at an interior offset within a same-batch append
// ---------------------------------------------------------------------------

Deno.test("batch.get reads a field at interior offset within a same-batch blob (fresh store)", async () => {
	// Simulates: txPointer = batch.tx.append(txBytes); ... batch.tx.get(txPointer + 32, U40)
	// on a fresh store where prevsize = 0.
	await withStore({}, async (store) => {
		const b = store.batch();

		// Build a fake "tx": 32 bytes of prefix + 5-byte U40 field + trailing bytes.
		const prefix = new Uint8Array(32).fill(0xaa);
		const spenderValue = 12345;
		const spenderBytes = U40.encode(spenderValue);
		const suffix = new Uint8Array(4).fill(0xbb);

		const blob = new Uint8Array(prefix.length + spenderBytes.length + suffix.length);
		blob.set(prefix, 0);
		blob.set(spenderBytes, 32);
		blob.set(suffix, 37);

		const txPointer = b.append(blob);
		assertEquals(txPointer, 0); // fresh store

		const readBack = await b.get(txPointer + 32, U40);
		assertEquals(readBack, spenderValue, "interior U40 read from batch buffer incorrect");

		b.discard();
	});
});

Deno.test("batch.get reads interior offset when prevsize > 0 (prior disk data)", async () => {
	// Same as above but with existing data on disk so prevsize != 0.
	await withStore({}, async (store) => {
		// Write some data to disk first.
		{
			const b = store.batch();
			b.append(U32.encode(0xdeadbeef)); // 4 bytes at offset 0
			b.apply();
		}
		await store.pin();
		await store.flush();

		// Now the disk has 4 bytes. Simulate appending a tx and reading its interior.
		const b = store.batch();
		const prefix = new Uint8Array(32).fill(0xcc);
		const spenderValue = 98765;
		const spenderBytes = U40.encode(spenderValue);
		const suffix = new Uint8Array(4).fill(0xdd);

		const blob = new Uint8Array(prefix.length + spenderBytes.length + suffix.length);
		blob.set(prefix, 0);
		blob.set(spenderBytes, 32);
		blob.set(suffix, 37);

		const txPointer = b.append(blob);
		assertEquals(txPointer, 4, "txPointer should start right after the 4 bytes already on disk");

		const readBack = await b.get(txPointer + 32, U40);
		assertEquals(readBack, spenderValue, "interior U40 read across disk/batch boundary incorrect");

		b.discard();
	});
});

Deno.test("batch.get reads interior offset on second blob when two blobs appended", async () => {
	// Simulates two txs in the same block: append tx0, then tx1.
	// tx1 spends tx0 → reads back tx0's spender field at tx0Pointer + 32.
	await withStore({}, async (store) => {
		const b = store.batch();

		// tx0: 32-byte prefix + U40 spender=100 + 4-byte suffix
		const tx0Spender = 100;
		const tx0 = new Uint8Array(32 + 5 + 4);
		tx0.fill(0x11, 0, 32);
		tx0.set(U40.encode(tx0Spender), 32);
		tx0.fill(0x22, 37);

		const tx0Pointer = b.append(tx0); // should be 0

		// tx1: 32-byte prefix + U40 spender=101 + 4-byte suffix
		const tx1Spender = 101;
		const tx1 = new Uint8Array(32 + 5 + 4);
		tx1.fill(0x33, 0, 32);
		tx1.set(U40.encode(tx1Spender), 32);
		tx1.fill(0x44, 37);

		const tx1Pointer = b.append(tx1);
		assertEquals(tx1Pointer, tx0.length, "tx1 pointer should immediately follow tx0");

		// tx1 now "reads" tx0's spender field:
		const readTx0Spender = await b.get(tx0Pointer + 32, U40);
		assertEquals(readTx0Spender, tx0Spender, "reading tx0.spender from batch buffer gave wrong value");

		// And can also read tx1's own spender field:
		const readTx1Spender = await b.get(tx1Pointer + 32, U40);
		assertEquals(readTx1Spender, tx1Spender, "reading tx1.spender from batch buffer gave wrong value");

		b.discard();
	});
});

Deno.test("batch.get interior offset: read spanning two memory chunks", async () => {
	// Force tiny memory chunks so the U40 field straddles a chunk boundary.
	// chunk size = 34 bytes: prefix (32) + first 2 bytes of U40 in chunk 0,
	// last 3 bytes of U40 spill into chunk 1.
	await withStore({ maxMemoryChunkSize: 34 }, async (store) => {
		const b = store.batch();

		const spenderValue = 0x0102030405; // distinct bytes make byte-order bugs obvious
		const blob = new Uint8Array(32 + 5 + 4);
		blob.fill(0xff, 0, 32);
		blob.set(U40.encode(spenderValue), 32);
		blob.fill(0x00, 37);

		const txPointer = b.append(blob);

		const readBack = await b.get(txPointer + 32, U40);
		assertEquals(
			readBack,
			spenderValue,
			"U40 spanning chunk boundary not reassembled correctly",
		);

		b.discard();
	});
});

Deno.test("batch.get interior offset: read spanning memory chunk and extra padding", async () => {
	// chunk size = 35: prefix (32) fits in chunk 0 with 3 bytes to spare.
	// U40 bytes 0-2 land in chunk 0, bytes 3-4 spill to chunk 1.
	await withStore({ maxMemoryChunkSize: 35 }, async (store) => {
		const b = store.batch();

		const spenderValue = 0xfedcba9876;
		const blob = new Uint8Array(32 + 5 + 4);
		blob.fill(0xee, 0, 32);
		blob.set(U40.encode(spenderValue), 32);
		blob.fill(0x00, 37);

		const txPointer = b.append(blob);

		const readBack = await b.get(txPointer + 32, U40);
		assertEquals(
			readBack,
			spenderValue,
			"U40 read fails when bytes straddle memory chunk at offset 32",
		);

		b.discard();
	});
});
