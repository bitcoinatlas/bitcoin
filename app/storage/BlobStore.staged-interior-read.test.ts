/**
 * Tests for reading at an interior offset from STAGED data (applied but not
 * flushed). This exercises the path where blocks 1-N are applied to _staged
 * and block N+1 reads back a field from block K's tx (K < N+1).
 *
 * This is distinct from BlobStore.batch-interior-read.test.ts which tests
 * reads within the OPEN BATCH buffer.
 */
import { assertEquals } from "@std/assert";
import { U40 } from "~/codec/primitives/U40.ts";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { BlobStore } from "~/storage/BlobStore.ts";

function makeTx(spender: number): StoredTx {
	return {
		txId: new Uint8Array(32).fill(0xab),
		spender,
		version: 1,
		locktime: { kind: "none" },
		inputs: [],
		outputs: [],
	};
}

async function withStore(
	opts: { maxDiskChunkSize?: number; maxMemoryChunkSize?: number },
	fn: (store: BlobStore) => Promise<void>,
): Promise<void> {
	const path = await Deno.makeTempDir({ prefix: "blobstore-staged-" });
	const store = await BlobStore.open({
		path,
		maxDiskChunkSize: opts.maxDiskChunkSize ?? 1024 * 1024,
		maxMemoryChunkSize: opts.maxMemoryChunkSize ?? 1024 * 1024,
	});
	try {
		await fn(store);
	} finally {
		store.close();
		await Deno.remove(path, { recursive: true }).catch(() => {});
	}
}

Deno.test("batch.get reads interior offset from STAGED region (prior block's tx)", async () => {
	// Simulates: block K applied → staged; block K+1 batch opened; reads block K tx's spender.
	await withStore({}, async (store) => {
		// Block K: append a tx with spender=42, apply to staged.
		let blockKTxPointer: number;
		const blockKSpender = 42;
		{
			const b = store.batch();
			b.append(new Uint8Array([0x01])); // block count prefix
			const txBytes = StoredTx.encode(makeTx(blockKSpender));
			blockKTxPointer = b.append(txBytes);
			b.apply();
		}

		// Block K+1: open a new batch, read block K's spender field from staged.
		{
			const b = store.batch();
			b.append(new Uint8Array([0x01])); // block K+1 count prefix

			// Simulate chain.ts:280 — reading block K's spender from staged via batch.get
			const txSpenderOffset = await b.get(blockKTxPointer + Bytes32.stride.size, U40);
			assertEquals(
				txSpenderOffset,
				blockKSpender,
				"reading spender field from STAGED region via batch.get gave wrong value",
			);

			b.discard();
		}
	});
});

Deno.test("batch.get reads interior offset from staged region across many prior blocks", async () => {
	// Simulates 169 coinbase-only blocks applied to staged, then reading
	// an early block's spender field from within a new batch.
	await withStore({}, async (store) => {
		const txPointers: number[] = [];

		// Apply 169 "blocks", each with one tx.
		for (let h = 1; h <= 169; h++) {
			const b = store.batch();
			b.append(new Uint8Array([0x01]));
			const txBytes = StoredTx.encode(makeTx(h)); // spender = height for distinctness
			txPointers.push(b.append(txBytes));
			b.apply();
		}

		// Block 170: open a batch and read back block 9's tx's spender field.
		// (Block 9 = index 8 in txPointers array since we started at h=1)
		const block9TxPointer = txPointers[8]!; // h=9
		const expectedSpender = 9; // we stored spender = h = 9

		{
			const b = store.batch();
			b.append(new Uint8Array([0x01]));

			const txSpenderOffset = await b.get(block9TxPointer + Bytes32.stride.size, U40);
			assertEquals(
				txSpenderOffset,
				expectedSpender,
				`block 9 spender field read wrong after 169 blocks staged`,
			);

			b.discard();
		}
	});
});

Deno.test("batch.get reads staged interior offset near a memory chunk boundary", async () => {
	// Small memory chunk to force block tx data to span multiple chunks.
	// Each "tx" is 32+5+0 = 37 bytes minimum; set chunk to 40 so boundary
	// falls in the middle of the second tx.
	await withStore({ maxMemoryChunkSize: 40 }, async (store) => {
		// Block 0 append: 1-byte prefix + 37-byte tx → 38 bytes total, fits in chunk 0.
		let tx0Pointer: number;
		{
			const b = store.batch();
			b.append(new Uint8Array([0x01]));
			const txBytes = StoredTx.encode(makeTx(7));
			tx0Pointer = b.append(txBytes);
			b.apply();
		}

		// Block 1 append: another tx, lands in chunk 1 (since chunk 0 is full at 38 bytes).
		{
			const b = store.batch();
			b.append(new Uint8Array([0x01]));
			const txBytes = StoredTx.encode(makeTx(99));
			b.append(txBytes);
			b.apply();
		}

		// Block 2 reads back block 0's tx spender from staged (chunk 0).
		{
			const b = store.batch();
			b.append(new Uint8Array([0x01]));

			const txSpenderOffset = await b.get(tx0Pointer + Bytes32.stride.size, U40);
			assertEquals(txSpenderOffset, 7, "spender from chunk 0 of staged region read wrong");

			b.discard();
		}
	});
});
