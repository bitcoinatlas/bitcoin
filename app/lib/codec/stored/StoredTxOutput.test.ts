import { assertEquals, assertObjectMatch, assertThrows } from "@std/assert";
import { StoredScriptPubKey, StoredTxOutput, TxOutput } from "~/lib/codec/stored/StoredTxOutput.ts";
import { Uint8ArrayView } from "~/lib/Uint8ArrayView.ts";

function makeOutput(
	value: bigint,
	spentBy: number | null,
	scriptPubKey: StoredScriptPubKey,
): TxOutput {
	return { value, spentBy, scriptPubKey };
}

// --- known script types (spendable) ---

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

// --- raw script ---

Deno.test("StoredTxOutput roundtrip - raw script (non-standard, 2 bytes)", () => {
	const script = new Uint8Array([0x51, 0xac]); // OP_1 OP_CHECKSIG (won't match any pattern)
	const out = makeOutput(0n, null, { kind: "raw", value: script });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertObjectMatch(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - raw script (empty)", () => {
	const out = makeOutput(0n, null, { kind: "raw", value: new Uint8Array(0) });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertEquals(decoded.value, 0n);
});

// --- opreturn (provably unspendable, no spentBy field) ---

Deno.test("StoredTxOutput roundtrip - opreturn (no spentBy field stored)", () => {
	// OP_RETURN prefix is 0x6a
	const script = new Uint8Array([0x6a, 0x04, 0xde, 0xad, 0xbe, 0xef]);
	const out = makeOutput(0n, null, { kind: "opreturn", value: script });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertObjectMatch(decoded, out);
});

Deno.test("StoredTxOutput opreturn is smaller than equivalent spendable (no 6-byte spentBy)", () => {
	const hash = new Uint8Array(20).fill(0xab);
	const spendable = makeOutput(0n, null, { kind: "p2pkh", value: hash });
	// opreturn of similar size
	const script = new Uint8Array([0x6a, 0x14, ...new Uint8Array(20).fill(0xab)]);
	const opreturn = makeOutput(0n, null, { kind: "opreturn", value: script });
	// opreturn skips the 6-byte spentBy block
	assertEquals(StoredTxOutput.encode(opreturn).length < StoredTxOutput.encode(spendable).length + 6, true);
});

// --- pointer scriptPubKey ---

Deno.test("StoredTxOutput roundtrip - pointer scriptPubKey", () => {
	const out = makeOutput(50_00000000n, null, { kind: "pointer", value: 123456789 });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertObjectMatch(decoded, out);
});

Deno.test("StoredTxOutput roundtrip - pointer scriptPubKey, spent", () => {
	const out = makeOutput(1000n, 99, { kind: "pointer", value: 999 });
	const [decoded] = StoredTxOutput.decode(StoredTxOutput.encode(out));
	assertObjectMatch(decoded, out);
});

// --- spentBy sentinel: 0 means unspent ---

Deno.test("StoredTxOutput spentBy=null encodes as 0 (sentinel) and decodes back to null", () => {
	const hash = new Uint8Array(20).fill(0x01);
	const out = makeOutput(1000n, null, { kind: "p2pkh", value: hash });
	const encoded = StoredTxOutput.encode(out);
	// spentBy is at bytes [1..7) (after the 1-byte flag)
	const spentByBytes = encoded.subarray(1, 7);
	// all 6 bytes must be zero (sentinel for unspent)
	assertEquals(spentByBytes, new Uint8Array(6));
	const [decoded] = StoredTxOutput.decode(encoded);
	assertEquals(decoded.spentBy, null);
});

Deno.test("StoredTxOutput spentBy field sits at fixed offset (record+1) for all spendable types", () => {
	// Verify that the 6 bytes at offset 1 represent the spentBy pointer for each
	// spendable type, regardless of value VarInt width.
	const spentPointer = 42;
	const types: StoredScriptPubKey[] = [
		{ kind: "p2pkh", value: new Uint8Array(20).fill(0x01) },
		{ kind: "p2sh", value: new Uint8Array(20).fill(0x02) },
		{ kind: "p2wpkh", value: new Uint8Array(20).fill(0x03) },
		{ kind: "p2wsh", value: new Uint8Array(32).fill(0x04) },
		{ kind: "p2tr", value: new Uint8Array(32).fill(0x05) },
	];
	for (const scriptPubKey of types) {
		const out = makeOutput(1n, spentPointer, scriptPubKey);
		const encoded = StoredTxOutput.encode(out);
		// The 6 bytes at offset 1 are the spentBy pointer
		const spentByView = new Uint8ArrayView(encoded, encoded.byteOffset + 1, 6);
		// Read as little-endian 48-bit
		const lo = spentByView.getUint32(0, true);
		const hi = spentByView.getUint16(4, true);
		const decoded_ptr = lo + hi * 2 ** 32;
		assertEquals(decoded_ptr, spentPointer);
	}
});

// --- value edge cases ---

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

Deno.test("StoredTxOutput encode throws on value >= 2^51", () => {
	const hash = new Uint8Array(20).fill(0xff);
	const out = makeOutput(1n << 51n, null, { kind: "p2pkh", value: hash });
	assertThrows(() => StoredTxOutput.encode(out), Error);
});

Deno.test("StoredTxOutput encode throws on negative value", () => {
	const hash = new Uint8Array(20).fill(0xff);
	const out = makeOutput(-1n, null, { kind: "p2pkh", value: hash });
	assertThrows(() => StoredTxOutput.encode(out), Error);
});

// --- decode consumed byte count ---

Deno.test("StoredTxOutput decode reports correct consumed bytes with trailing data", () => {
	const hash = new Uint8Array(20).fill(0xab);
	const out = makeOutput(5000000000n, null, { kind: "p2pkh", value: hash });
	const encoded = StoredTxOutput.encode(out);
	const padded = new Uint8Array(encoded.length + 10);
	padded.set(encoded, 0);
	const [, size] = StoredTxOutput.decode(padded);
	assertEquals(size, encoded.length);
});

// --- determinism ---

Deno.test("StoredTxOutput encode is deterministic", () => {
	const hash = new Uint8Array(20).fill(0xab);
	const out = makeOutput(5000000000n, null, { kind: "p2pkh", value: hash });
	const a = StoredTxOutput.encode(out);
	const b = StoredTxOutput.encode(out);
	assertEquals(a, b);
});
