import { assertEquals, assertObjectMatch } from "@std/assert";
import type { TxInput } from "~/lib/chain/TxInput.ts";
import { StoredTx } from "~/lib/codec/stored/StoredTx.ts";
import { TxOutput } from "~/lib/codec/stored/StoredTxOutput.ts";

function makeStoredTx(): StoredTx {
	const txId = new Uint8Array(32);
	for (let i = 0; i < 32; i++) txId[i] = i;

	const scriptHash = new Uint8Array(20).fill(0xab);
	const vout: TxOutput[] = [
		{ value: 5000000000n, spentBy: null, scriptPubKey: { kind: "p2pkh", value: scriptHash } },
		{ value: 1000000n, spentBy: 1, scriptPubKey: { kind: "p2sh", value: new Uint8Array(20).fill(0x12) } },
	];

	const vin: TxInput[] = [
		{
			prevOut: { txId: { kind: "coinbase" }, vout: 0xffffffff },
			scriptSig: new Uint8Array([0x03, 0x01, 0x02, 0x03]),
			sequence: { kind: "final" },
			witness: [],
		},
	];

	return {
		txId,
		version: 1,
		lockTime: { kind: "none" },
		vout,
		vin,
	};
}

Deno.test("StoredTx roundtrip - coinbase tx", () => {
	const tx = makeStoredTx();
	const encoded = StoredTx.encode(tx);
	const [decoded] = StoredTx.decode(encoded);

	assertObjectMatch(decoded, tx);
});

Deno.test("StoredTx roundtrip - txId is preserved exactly", () => {
	const tx = makeStoredTx();
	const encoded = StoredTx.encode(tx);
	const [decoded] = StoredTx.decode(encoded);
	assertEquals(decoded.txId, tx.txId);
});

Deno.test("StoredTx roundtrip - with block locktime", () => {
	const tx = makeStoredTx();
	tx.lockTime = { kind: "block", height: 300000 };
	const encoded = StoredTx.encode(tx);
	const [decoded] = StoredTx.decode(encoded);
	assertEquals(decoded.lockTime, tx.lockTime);
});

Deno.test("StoredTx roundtrip - with timestamp locktime", () => {
	const tx = makeStoredTx();
	tx.lockTime = { kind: "time", timestamp: 1400000000 };
	const encoded = StoredTx.encode(tx);
	const [decoded] = StoredTx.decode(encoded);
	assertEquals(decoded.lockTime, tx.lockTime);
});

Deno.test("StoredTx roundtrip - version 2", () => {
	const tx = makeStoredTx();
	tx.version = 2;
	const encoded = StoredTx.encode(tx);
	const [decoded] = StoredTx.decode(encoded);
	assertEquals(decoded.version, 2);
});

Deno.test("StoredTx roundtrip - multiple inputs (resolved pointers)", () => {
	const tx = makeStoredTx();
	tx.vin = [
		{
			prevOut: { txId: { kind: "pointer", value: 111111 }, vout: 0 },
			scriptSig: new Uint8Array([0x47, 0x30]),
			sequence: { kind: "final" },
			witness: [],
		},
		{
			prevOut: { txId: { kind: "pointer", value: 222222 }, vout: 1 },
			scriptSig: new Uint8Array([0x48, 0x30]),
			sequence: { kind: "final" },
			witness: [],
		},
	];
	const encoded = StoredTx.encode(tx);
	const [decoded] = StoredTx.decode(encoded);
	assertEquals(decoded.vin.length, 2);
	assertEquals(decoded.vin[0]!.prevOut.txId.kind, "pointer");
	assertEquals((decoded.vin[0]!.prevOut.txId as { kind: "pointer"; value: number }).value, 111111);
	assertEquals(decoded.vin[1]!.prevOut.vout, 1);
});

Deno.test("StoredTx encode is deterministic", () => {
	const tx = makeStoredTx();
	const a = StoredTx.encode(tx);
	const b = StoredTx.encode(tx);
	assertEquals(a, b);
});
