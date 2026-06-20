/**
 * Integration test: simulates the chain.ts:280 pattern end-to-end using
 * real StoredTx encode and BlobStore batch reads.
 *
 * Specifically tests that:
 *   1. tx0 is appended to a batch with spender=S
 *   2. tx1 can read back S by doing batch.get(tx0Pointer + Bytes32.stride.size, U40)
 *   3. spenderIndex = S + vout resolves to the correct entry
 *
 * This is the exact computation that crashed with index=0xFFFFFFFF.
 */
import { assertEquals } from "@std/assert";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U40 } from "~/codec/primitives/U40.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { BlobStore } from "~/libs/storage/BlobStore.ts";

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

async function withStore(fn: (store: BlobStore) => Promise<void>): Promise<void> {
	const path = await Deno.makeTempDir({ prefix: "blobstore-chain-" });
	const store = await BlobStore.open({
		path,
		maxDiskChunkSize: 1024,
		maxMemoryChunkSize: 1024,
	});
	try {
		await fn(store);
	} finally {
		store.close();
		await Deno.remove(path, { recursive: true }).catch(() => {});
	}
}

Deno.test("StoredTx: spender field round-trips through BlobStore batch append+get", async () => {
	// chain.ts flow:
	//   tx0.spender = spender.length()  → e.g. 0
	//   txPointer0  = batch.tx.append(StoredTx.encode(tx0))
	//   ...
	//   txSpenderOffset = await batch.tx.get(txPointer0 + Bytes32.stride.size, U40)
	//   spenderIndex    = txSpenderOffset + vout
	//
	// This test verifies the round-trip is exact.

	await withStore(async (store) => {
		const b = store.batch();

		// Simulate a block-count prefix byte (like StoredTxs.counter.encode(1) = [0x01])
		b.append(new Uint8Array([0x01]));

		const tx0SpenderIndex = 0; // spender store was empty
		const tx0 = makeTx(tx0SpenderIndex);
		const tx0Bytes = StoredTx.encode(tx0);
		const tx0Pointer = b.append(tx0Bytes);

		// chain.ts:280 — tx1 reading tx0's spender field
		const txSpenderOffset = await b.get(tx0Pointer + Bytes32.stride.size, U40);
		assertEquals(txSpenderOffset, tx0SpenderIndex, "spender index read from BlobStore batch is wrong");

		// chain.ts:281
		const vout = 0;
		const spenderIndex = txSpenderOffset + vout;
		assertEquals(spenderIndex, 0, "spenderIndex is wrong");

		b.discard();
	});
});

Deno.test("StoredTx: spender field reads correctly when tx0 is not the first blob", async () => {
	// Tests when the block has a VarInt prefix + tx0 is not at offset 0.
	await withStore(async (store) => {
		const b = store.batch();

		// Block with 2 txs: VarInt(2) = 0x02
		const blockPointer = b.append(new Uint8Array([0x02]));
		assertEquals(blockPointer, 0);

		const tx0Spender = 7; // spender store had 7 entries before tx0
		const tx0 = makeTx(tx0Spender);
		const tx0Bytes = StoredTx.encode(tx0);
		const tx0Pointer = b.append(tx0Bytes);
		assertEquals(tx0Pointer, 1, "tx0 should start at offset 1 (after 1-byte VarInt)");

		const tx1Spender = 7 + tx0.outputs.length; // = 7 (no outputs)
		const tx1 = makeTx(tx1Spender);
		const tx1Bytes = StoredTx.encode(tx1);
		const tx1Pointer = b.append(tx1Bytes);
		assertEquals(tx1Pointer, 1 + tx0Bytes.length, "tx1 should start right after tx0");

		// tx1's input spends tx0 vout=0 → read tx0's spender field
		const readSpender = await b.get(tx0Pointer + Bytes32.stride.size, U40);
		assertEquals(readSpender, tx0Spender, "tx0.spender read from interior offset is wrong");

		const spenderIndex = readSpender + 0; // vout=0
		assertEquals(spenderIndex, 7);

		b.discard();
	});
});

Deno.test("StoredTx: spender field reads correctly with existing disk data (cross-session)", async () => {
	// Simulates a previously flushed genesis block (some bytes on disk),
	// then a new block being processed. The genesis tx's spender must be
	// readable from a new batch at the cross-session pointer.
	await withStore(async (store) => {
		// --- session 1: write genesis block ---
		{
			const b = store.batch();
			b.append(new Uint8Array([0x01])); // block prefix: 1 tx

			const genesisTx = makeTx(0); // spender=0 (spender store was empty)
			const genesisTxBytes = StoredTx.encode(genesisTx);
			const genesisTxPointer = b.append(genesisTxBytes);
			assertEquals(genesisTxPointer, 1);

			b.apply();
		}
		await store.pin();
		await store.flush();

		// --- session 2: process block 1 that spends genesis output 0 ---
		{
			const b = store.batch();
			const blockPointer = b.append(new Uint8Array([0x01])); // 1 tx
			// genesis is on disk at pointer=1; block 1 starts after genesis block bytes
			const genesisTx = makeTx(0);
			const genesisTxOnDiskPointer = 1;

			// Simulate what chain.ts:280 does for the input that spends genesis:vout=0
			const txSpenderOffset = await b.get(genesisTxOnDiskPointer + Bytes32.stride.size, U40);
			assertEquals(txSpenderOffset, 0, "genesis spender should be 0");

			const spenderIndex = txSpenderOffset + 0; // vout=0
			assertEquals(spenderIndex, 0);

			// Append the spending tx
			const spendingTx = makeTx(1); // spender=1 after genesis output
			const spendingTxBytes = StoredTx.encode(spendingTx);
			b.append(spendingTxBytes);

			b.discard();
		}
	});
});
