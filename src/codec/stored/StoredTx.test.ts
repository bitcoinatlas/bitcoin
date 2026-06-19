/**
 * StoredTx encode/decode tests.
 *
 * Focus: the `spender` field (U40) round-trips correctly for boundary values,
 * and the byte offset of `spender` within an encoded tx is exactly
 * Bytes32.stride.size (32) — which is what chain.ts:280 hard-codes.
 */
import { assertEquals } from "@std/assert";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U40 } from "~/codec/primitives/U40.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";

/** Minimal valid StoredTx with the given spender index. */
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

Deno.test("StoredTx: spender=0 round-trips correctly", () => {
	const tx = makeTx(0);
	const encoded = StoredTx.encode(tx);
	const [decoded] = StoredTx.decode(encoded);
	assertEquals(decoded.spender, 0);
});

Deno.test("StoredTx: spender=1 round-trips correctly", () => {
	const tx = makeTx(1);
	const encoded = StoredTx.encode(tx);
	const [decoded] = StoredTx.decode(encoded);
	assertEquals(decoded.spender, 1);
});

Deno.test("StoredTx: spender=large value round-trips correctly", () => {
	const spender = 1_000_000_000; // 1 billion — well within U40 range
	const tx = makeTx(spender);
	const encoded = StoredTx.encode(tx);
	const [decoded] = StoredTx.decode(encoded);
	assertEquals(decoded.spender, spender);
});

Deno.test("StoredTx: spender field sits exactly at offset Bytes32.stride.size", () => {
	// chain.ts:280 reads the spender field with:
	//   batch.tx.get(txPointer + Bytes32.stride.size, U40)
	// This test pins the byte offset so a layout change breaks loudly here.
	const spender = 42;
	const tx = makeTx(spender);
	const encoded = StoredTx.encode(tx);

	const expectedOffset = Bytes32.stride.size; // 32
	const [readBack] = U40.decode(encoded.subarray(expectedOffset));
	assertEquals(
		readBack,
		spender,
		`spender field is not at byte offset ${expectedOffset}; layout has changed`,
	);
});

Deno.test("StoredTx: spender offset invariant holds for tx with non-empty outputs", () => {
	// The spender field must be at a fixed offset regardless of output content —
	// it sits BEFORE vout[], so this should always hold.
	const tx: StoredTx = {
		txId: new Uint8Array(32).fill(0x01),
		spender: 77,
		version: 2,
		locktime: { kind: "none" },
		inputs: [],
		outputs: [
			{
				value: 50_000_000n,
				scriptPubKey: {
					kind: "p2wpkh",
					value: new Uint8Array(20).fill(0xcc),
				},
			},
		],
	};

	const encoded = StoredTx.encode(tx);
	const [readBack] = U40.decode(encoded.subarray(Bytes32.stride.size));
	assertEquals(readBack, 77);
});
