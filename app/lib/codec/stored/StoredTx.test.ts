import { assertEquals } from "@std/assert";
import { StoredTx } from "~/lib/codec/stored/StoredTx.ts";
import { TxOutput } from "~/lib/chain/TxOutput.ts";
import { TxInput } from "~/lib/chain/TxInput.ts";

function makeStoredTx(): import("~/lib/codec/stored/StoredTx.ts").StoredTx {
	const txId = new Uint8Array(32);
	for (let i = 0; i < 32; i++) txId[i] = i;

	const scriptHash = new Uint8Array(20).fill(0xab);
	const vout = [
		new TxOutput({ value: 5000000000n, spent: false, scriptPubKey: { kind: "p2pkh", value: scriptHash } }),
		new TxOutput({ value: 1000000n, spent: true, scriptPubKey: { kind: "p2sh", value: new Uint8Array(20).fill(0x12) } }),
	];

	const vin = [
		new TxInput({
			prevOut: { txId: { kind: "coinbase" }, vout: 0xffffffff },
			scriptSig: new Uint8Array([0x03, 0x01, 0x02, 0x03]),
			sequence: { kind: "final" },
			witness: [],
		}),
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

	assertEquals(decoded.txId, tx.txId);
	assertEquals(decoded.version, tx.version);
	assertEquals(decoded.lockTime, tx.lockTime);
	assertEquals(decoded.vout.length, tx.vout.length);
	assertEquals(decoded.vin.length, tx.vin.length);
	assertEquals(decoded.vout[0]!.data.value, tx.vout[0]!.data.value);
	assertEquals(decoded.vout[1]!.data.value, tx.vout[1]!.data.value);
	assertEquals(decoded.vout[0]!.data.spent, tx.vout[0]!.data.spent);
	assertEquals(decoded.vout[1]!.data.spent, tx.vout[1]!.data.spent);
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
		new TxInput({
			prevOut: { txId: { kind: "pointer", value: 111111 }, vout: 0 },
			scriptSig: new Uint8Array([0x47, 0x30]),
			sequence: { kind: "final" },
			witness: [],
		}),
		new TxInput({
			prevOut: { txId: { kind: "pointer", value: 222222 }, vout: 1 },
			scriptSig: new Uint8Array([0x48, 0x30]),
			sequence: { kind: "final" },
			witness: [],
		}),
	];
	const encoded = StoredTx.encode(tx);
	const [decoded] = StoredTx.decode(encoded);
	assertEquals(decoded.vin.length, 2);
	assertEquals(decoded.vin[0]!.data.prevOut.txId.kind, "pointer");
	assertEquals((decoded.vin[0]!.data.prevOut.txId as { kind: "pointer"; value: number }).value, 111111);
	assertEquals(decoded.vin[1]!.data.prevOut.vout, 1);
});

Deno.test("StoredTx encode is deterministic", () => {
	const tx = makeStoredTx();
	const a = StoredTx.encode(tx);
	const b = StoredTx.encode(tx);
	assertEquals(a, b);
});
