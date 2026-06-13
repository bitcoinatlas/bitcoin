import { assertEquals, assertObjectMatch } from "@std/assert";
import type { TxInput } from "~/chain/TxInput.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxOutput, TxOutput } from "~/codec/stored/StoredTxOutput.ts";

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

// --- basic roundtrips ---

Deno.test("StoredTx roundtrip - coinbase tx", () => {
	const tx = makeStoredTx();
	const [decoded] = StoredTx.decode(StoredTx.encode(tx));
	assertObjectMatch(decoded, tx);
});

Deno.test("StoredTx roundtrip - txId is preserved exactly", () => {
	const tx = makeStoredTx();
	const [decoded] = StoredTx.decode(StoredTx.encode(tx));
	assertEquals(decoded.txId, tx.txId);
});

// --- locktime variants ---

Deno.test("StoredTx roundtrip - with block locktime", () => {
	const tx = makeStoredTx();
	tx.lockTime = { kind: "block", height: 300000 };
	const [decoded] = StoredTx.decode(StoredTx.encode(tx));
	assertObjectMatch(decoded.lockTime, tx.lockTime);
});

Deno.test("StoredTx roundtrip - with timestamp locktime", () => {
	const tx = makeStoredTx();
	tx.lockTime = { kind: "time", timestamp: 1400000000 };
	const [decoded] = StoredTx.decode(StoredTx.encode(tx));
	assertObjectMatch(decoded.lockTime, tx.lockTime);
});

// --- version variants ---

Deno.test("StoredTx roundtrip - version 2", () => {
	const tx = makeStoredTx();
	tx.version = 2;
	const [decoded] = StoredTx.decode(StoredTx.encode(tx));
	assertEquals(decoded.version, 2);
});

Deno.test("StoredTx roundtrip - non-standard version (raw-encoded pack)", () => {
	const tx = makeStoredTx();
	tx.version = 3;
	const [decoded] = StoredTx.decode(StoredTx.encode(tx));
	assertEquals(decoded.version, 3);
	assertObjectMatch(decoded.lockTime, { kind: "none" });
});

// --- multiple inputs ---

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
	const [decoded] = StoredTx.decode(StoredTx.encode(tx));
	assertEquals(decoded.vin.length, 2);
	assertEquals(decoded.vin[0]!.prevOut.txId.kind, "pointer");
	assertEquals((decoded.vin[0]!.prevOut.txId as { kind: "pointer"; value: number }).value, 111111);
	assertEquals(decoded.vin[1]!.prevOut.vout, 1);
});

// --- multiple outputs ---

Deno.test("StoredTx roundtrip - multiple outputs including opreturn", () => {
	const tx = makeStoredTx();
	tx.vout = [
		{ value: 5000000000n, spentBy: null, scriptPubKey: { kind: "p2pkh", value: new Uint8Array(20).fill(0xaa) } },
		{
			value: 0n,
			spentBy: null,
			scriptPubKey: { kind: "opreturn", value: new Uint8Array([0x6a, 0x04, 0x01, 0x02, 0x03, 0x04]) },
		},
	];
	const [decoded] = StoredTx.decode(StoredTx.encode(tx));
	assertEquals(decoded.vout.length, 2);
	assertEquals(decoded.vout[1]!.spentBy, null);
	assertObjectMatch(decoded.vout[1]!, tx.vout[1]!);
});

Deno.test("StoredTx roundtrip - zero outputs, zero inputs (degenerate)", () => {
	const txId = new Uint8Array(32).fill(0xff);
	const tx: StoredTx = { txId, version: 1, lockTime: { kind: "none" }, vout: [], vin: [] };
	const [decoded] = StoredTx.decode(StoredTx.encode(tx));
	assertEquals(decoded.vout.length, 0);
	assertEquals(decoded.vin.length, 0);
	assertEquals(decoded.txId, txId);
});

// --- decode consumes correct byte count ---

Deno.test("StoredTx decode reports correct consumed bytes with trailing data", () => {
	const tx = makeStoredTx();
	const encoded = StoredTx.encode(tx);
	const padded = new Uint8Array(encoded.length + 16);
	padded.set(encoded, 0);
	const [, size] = StoredTx.decode(padded);
	assertEquals(size, encoded.length);
});

// --- encodeWithOffsets ---

Deno.test("StoredTx encodeWithOffsets - vout and vin offsets count matches lengths", () => {
	const tx = makeStoredTx();
	const { bytes, offsets } = StoredTx.encodeWithOffsets(tx);
	assertEquals(offsets.vout.length, tx.vout.length);
	assertEquals(offsets.vin.length, tx.vin.length);
	// bytes should be identical to plain encode
	assertEquals(bytes, StoredTx.encode(tx));
});

Deno.test("StoredTx encodeWithOffsets - vout offsets are within bounds and strictly increasing", () => {
	const tx = makeStoredTx();
	const { bytes, offsets } = StoredTx.encodeWithOffsets(tx);
	for (let i = 0; i < offsets.vout.length; i++) {
		assertEquals(offsets.vout[i]! >= 0 && offsets.vout[i]! < bytes.length, true);
		if (i > 0) assertEquals(offsets.vout[i]! > offsets.vout[i - 1]!, true);
	}
});

Deno.test("StoredTx encodeWithOffsets - vin offsets are within bounds and strictly increasing", () => {
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
			scriptSig: new Uint8Array([0x48, 0x30, 0x00]),
			sequence: { kind: "final" },
			witness: [],
		},
	];
	const { bytes, offsets } = StoredTx.encodeWithOffsets(tx);
	for (let i = 0; i < offsets.vin.length; i++) {
		assertEquals(offsets.vin[i]! >= 0 && offsets.vin[i]! < bytes.length, true);
		if (i > 0) assertEquals(offsets.vin[i]! > offsets.vin[i - 1]!, true);
	}
});

Deno.test("StoredTx encodeWithOffsets - first vout offset equals header size", () => {
	const tx = makeStoredTx();
	// The header is: 32 bytes txId + LockTimeVersionPack (1 byte for v1/none) + 1 byte vout count VarInt
	// = 32 + 1 + 1 = 34
	const { offsets } = StoredTx.encodeWithOffsets(tx);
	assertEquals(offsets.vout[0], 34);
});

Deno.test("StoredTx encodeWithOffsets - first vin offset follows vout section", () => {
	const tx = makeStoredTx();
	const { offsets } = StoredTx.encodeWithOffsets(tx);
	// vout section ends right before the vin count byte, so the first vin item
	// starts after that count byte.
	let voutEnd = 34;
	for (const output of tx.vout) {
		voutEnd += StoredTxOutput.encode(output).length;
	}
	assertEquals(offsets.vin[0], voutEnd + 1);
});

Deno.test("StoredTx encodeWithOffsets - empty vout yields empty offsets array", () => {
	const tx = makeStoredTx();
	tx.vout = [];
	const { offsets } = StoredTx.encodeWithOffsets(tx);
	assertEquals(offsets.vout, []);
	assertEquals(offsets.vin.length, tx.vin.length);
});

Deno.test("StoredTx encodeWithOffsets - empty vin yields empty vin offsets array", () => {
	const tx = makeStoredTx();
	tx.vin = [];
	const { offsets } = StoredTx.encodeWithOffsets(tx);
	assertEquals(offsets.vin, []);
	assertEquals(offsets.vout.length, tx.vout.length);
});

// --- determinism ---

Deno.test("StoredTx encode is deterministic", () => {
	const tx = makeStoredTx();
	assertEquals(StoredTx.encode(tx), StoredTx.encode(tx));
});
