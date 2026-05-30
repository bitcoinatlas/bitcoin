import { assertEquals } from "@std/assert";
import { StoredTxOutput } from "~/lib/codec/stored/StoredTxOutput.ts";
import type { TxOutput } from "~/lib/chain/TxOutput.ts";

function makeOutput(value: bigint, spent: boolean, scriptPubKey: import("~/lib/codec/stored/StoredTxOutput.ts").StoredScriptPubKey): TxOutput {
	return { value, spent, scriptPubKey };
}

function assertOutputEqual(a: TxOutput, b: TxOutput) {
	assertEquals(a.value, b.value);
	assertEquals(a.spent, b.spent);
	assertEquals(a.scriptPubKey.kind, b.scriptPubKey.kind);
	if (a.scriptPubKey.kind !== "pointer" && b.scriptPubKey.kind !== "pointer") {
		assertEquals(
			(a.scriptPubKey as { value: Uint8Array }).value,
			(b.scriptPubKey as { value: Uint8Array }).value,
		);
	} else if (a.scriptPubKey.kind === "pointer" && b.scriptPubKey.kind === "pointer") {
		assertEquals(a.scriptPubKey.value, b.scriptPubKey.value);
	}
}

Deno.test("StoredTxOutput roundtrip - p2pkh unspent", () => {
	const hash = new Uint8Array(20).fill(0xab);
	const out = makeOutput(5000000000n, false, { kind: "p2pkh", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertOutputEqual(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - p2pkh spent", () => {
	const hash = new Uint8Array(20).fill(0xcd);
	const out = makeOutput(1n, true, { kind: "p2pkh", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertOutputEqual(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - p2sh", () => {
	const hash = new Uint8Array(20).fill(0x12);
	const out = makeOutput(100000n, false, { kind: "p2sh", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertOutputEqual(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - p2wpkh", () => {
	const hash = new Uint8Array(20).fill(0x34);
	const out = makeOutput(21000000_00000000n, false, { kind: "p2wpkh", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertOutputEqual(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - p2wsh", () => {
	const hash = new Uint8Array(32).fill(0x56);
	const out = makeOutput(999999999n, true, { kind: "p2wsh", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertOutputEqual(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - p2tr", () => {
	const hash = new Uint8Array(32).fill(0x78);
	const out = makeOutput(546n, false, { kind: "p2tr", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertOutputEqual(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - raw script", () => {
	const script = new Uint8Array([0x51, 0xac]); // OP_1 OP_CHECKSIG (won't match any pattern)
	const out = makeOutput(0n, false, { kind: "raw", value: script });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertOutputEqual(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - pointer scriptPubKey", () => {
	const out = makeOutput(50_00000000n, false, { kind: "pointer", value: 123456789 });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertOutputEqual(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - zero value", () => {
	const hash = new Uint8Array(20).fill(0x00);
	const out = makeOutput(0n, false, { kind: "p2pkh", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertEquals(decoded.value, 0n);
});

Deno.test("StoredTxOutput roundtrip - max 51-bit value", () => {
	const hash = new Uint8Array(20).fill(0xff);
	const out = makeOutput((1n << 51n) - 1n, false, { kind: "p2pkh", value: hash });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertEquals(decoded.value, (1n << 51n) - 1n);
});

Deno.test("StoredTxOutput encode is deterministic", () => {
	const hash = new Uint8Array(20).fill(0xab);
	const out = makeOutput(5000000000n, false, { kind: "p2pkh", value: hash });
	const a = StoredTxOutput.encode(out);
	const b = StoredTxOutput.encode(out);
	assertEquals(a, b);
});
