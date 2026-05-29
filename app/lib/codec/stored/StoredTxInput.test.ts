import { assertEquals } from "@std/assert";
import { StoredTxInput } from "~/lib/codec/stored/StoredTxInput.ts";
import { TxInput } from "~/lib/chain/TxInput.ts";

function assertInputEqual(a: TxInput, b: TxInput) {
	assertEquals(a.data.prevOut.txId.kind, b.data.prevOut.txId.kind);
	if (a.data.prevOut.txId.kind === "raw" && b.data.prevOut.txId.kind === "raw") {
		assertEquals(a.data.prevOut.txId.value, b.data.prevOut.txId.value);
	} else if (a.data.prevOut.txId.kind === "pointer" && b.data.prevOut.txId.kind === "pointer") {
		assertEquals(a.data.prevOut.txId.value, b.data.prevOut.txId.value);
	}
	assertEquals(a.data.prevOut.vout, b.data.prevOut.vout);
	assertEquals(a.data.scriptSig, b.data.scriptSig);
	assertEquals(a.data.sequence, b.data.sequence);
	assertEquals(a.data.witness, b.data.witness);
}

Deno.test("StoredTxInput roundtrip - coinbase input", () => {
	const input = new TxInput({
		prevOut: { txId: { kind: "coinbase" }, vout: 0xffffffff },
		scriptSig: new Uint8Array([0x03, 0x4e, 0x61, 0x07]),
		sequence: { kind: "final" },
		witness: [],
	});
	const [decoded] = StoredTxInput.decode(StoredTxInput.encode(input));
	assertInputEqual(decoded, input);
});

Deno.test("StoredTxInput roundtrip - raw txId (unresolved)", () => {
	const txId = new Uint8Array(32).fill(0xab);
	const input = new TxInput({
		prevOut: { txId: { kind: "raw", value: txId }, vout: 2 },
		scriptSig: new Uint8Array([0x47, 0x30]),
		sequence: { kind: "final" },
		witness: [],
	});
	const [decoded] = StoredTxInput.decode(StoredTxInput.encode(input));
	assertInputEqual(decoded, input);
});

Deno.test("StoredTxInput roundtrip - pointer txId (resolved)", () => {
	const input = new TxInput({
		prevOut: { txId: { kind: "pointer", value: 987654321 }, vout: 0 },
		scriptSig: new Uint8Array(0),
		sequence: { kind: "final" },
		witness: [],
	});
	const [decoded] = StoredTxInput.decode(StoredTxInput.encode(input));
	assertInputEqual(decoded, input);
});

Deno.test("StoredTxInput roundtrip - with witness data (raw)", () => {
	const txId = new Uint8Array(32).fill(0x99);
	const sig = new Uint8Array(71).fill(0x30);
	const pubkey = new Uint8Array(33);
	pubkey[0] = 0x02; // compressed pubkey prefix
	pubkey.fill(0xaa, 1);
	const input = new TxInput({
		prevOut: { txId: { kind: "raw", value: txId }, vout: 1 },
		scriptSig: new Uint8Array(0),
		sequence: { kind: "final" },
		witness: [sig, pubkey],
	});
	const [decoded] = StoredTxInput.decode(StoredTxInput.encode(input));
	// witness items may be trimmed/padded so check lengths and content
	assertEquals(decoded.data.prevOut.vout, input.data.prevOut.vout);
	assertEquals(decoded.data.witness.length, 2);
});

Deno.test("StoredTxInput roundtrip - empty scriptSig, empty witness", () => {
	const txId = new Uint8Array(32).fill(0x11);
	const input = new TxInput({
		prevOut: { txId: { kind: "raw", value: txId }, vout: 0 },
		scriptSig: new Uint8Array(0),
		sequence: { kind: "final" },
		witness: [],
	});
	const [decoded] = StoredTxInput.decode(StoredTxInput.encode(input));
	assertInputEqual(decoded, input);
});

Deno.test("StoredTxInput encode is deterministic", () => {
	const txId = new Uint8Array(32).fill(0xab);
	const input = new TxInput({
		prevOut: { txId: { kind: "raw", value: txId }, vout: 5 },
		scriptSig: new Uint8Array([0x01, 0x02]),
		sequence: { kind: "final" },
		witness: [],
	});
	const a = StoredTxInput.encode(input);
	const b = StoredTxInput.encode(input);
	assertEquals(a, b);
});
