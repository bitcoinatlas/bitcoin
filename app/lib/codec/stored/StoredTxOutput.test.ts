import { assertEquals, assertObjectMatch } from "@std/assert";
import { StoredScriptPubKey, StoredTxOutput, TxOutput } from "~/lib/codec/stored/StoredTxOutput.ts";

function makeOutput(
	value: bigint,
	spentBy: number | null,
	scriptPubKey: StoredScriptPubKey,
): TxOutput {
	return { value, spentBy, scriptPubKey };
}

Deno.test("StoredTxOutput roundtrip - p2pkh unspent", () => {
	const hash = new Uint8Array(20).fill(0xab);
	const out = makeOutput(5000000000n, null, { kind: "p2pkh", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertObjectMatch(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - p2pkh spent", () => {
	const hash = new Uint8Array(20).fill(0xcd);
	const out = makeOutput(1n, 1, { kind: "p2pkh", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertObjectMatch(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - p2sh", () => {
	const hash = new Uint8Array(20).fill(0x12);
	const out = makeOutput(100000n, null, { kind: "p2sh", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertObjectMatch(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - p2wpkh", () => {
	const hash = new Uint8Array(20).fill(0x34);
	const out = makeOutput(21000000_00000000n, null, { kind: "p2wpkh", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertObjectMatch(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - p2wsh", () => {
	const hash = new Uint8Array(32).fill(0x56);
	const out = makeOutput(999999999n, 1, { kind: "p2wsh", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertObjectMatch(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - p2tr", () => {
	const hash = new Uint8Array(32).fill(0x78);
	const out = makeOutput(546n, null, { kind: "p2tr", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertObjectMatch(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - raw script", () => {
	const script = new Uint8Array([0x51, 0xac]); // OP_1 OP_CHECKSIG (won't match any pattern)
	const out = makeOutput(0n, null, { kind: "raw", value: script });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertObjectMatch(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - pointer scriptPubKey", () => {
	const out = makeOutput(50_00000000n, null, { kind: "pointer", value: 123456789 });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertObjectMatch(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - zero value", () => {
	const hash = new Uint8Array(20).fill(0x00);
	const out = makeOutput(0n, null, { kind: "p2pkh", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertEquals(decoded.value, 0n);
});

Deno.test("StoredTxOutput roundtrip - max 51-bit value", () => {
	const hash = new Uint8Array(20).fill(0xff);
	const out = makeOutput((1n << 51n) - 1n, null, { kind: "p2pkh", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertEquals(decoded.value, (1n << 51n) - 1n);
});

Deno.test("StoredTxOutput encode is deterministic", () => {
	const hash = new Uint8Array(20).fill(0xab);
	const out = makeOutput(5000000000n, null, { kind: "p2pkh", value: hash });
	const a = StoredTxOutput.encode(out);
	const b = StoredTxOutput.encode(out);
	assertEquals(a, b);
});
