import { assertEquals, assertObjectMatch } from "@std/assert";
import { decodeHex, encodeHex } from "@std/encoding";
import { WireTx } from "~/codec/wire/WireTx.ts";

// Real block 1 coinbase tx hex from Bitcoin network
const BLOCK1_COINBASE_HEX = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff" +
	"0704ffff001d0104ffffffff0100f2052a01000000434104" +
	"96b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a" +
	"604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000";

Deno.test("WireTx decode block1 coinbase - txId matches known value", () => {
	const raw = decodeHex(BLOCK1_COINBASE_HEX);
	const [tx] = WireTx.decode(raw);
	// Known txId for block 1 coinbase (big-endian display)
	const expectedTxId = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";
	const actualHex = encodeHex(tx.txId.slice().reverse());
	assertEquals(actualHex, expectedTxId);
});

Deno.test("WireTx decode block1 coinbase - fields correct", () => {
	const raw = decodeHex(BLOCK1_COINBASE_HEX);
	const [tx] = WireTx.decode(raw);
	assertEquals(tx.version, 1);
	assertEquals(tx.inputs.length, 1);
	assertEquals(tx.outputs.length, 1);
	assertEquals(tx.outputs[0]!.value, 5000000000n);
	assertObjectMatch(tx.locktime, { kind: "none" });
	assertEquals(tx.witness.length, 0);
});

Deno.test("WireTx encode/decode roundtrip - pre-segwit tx bytes identical", () => {
	const raw = decodeHex(BLOCK1_COINBASE_HEX);
	const [tx, size] = WireTx.decode(raw);
	assertEquals(size, raw.length);
	const reencoded = WireTx.encode(tx);
	assertEquals(reencoded, raw);
});

Deno.test("WireTx decode/encode roundtrip preserves txId - pre-segwit", () => {
	const raw = decodeHex(BLOCK1_COINBASE_HEX);
	const [tx1] = WireTx.decode(raw);
	const reencoded = WireTx.encode(tx1);
	const [tx2] = WireTx.decode(reencoded);
	assertEquals(tx2.txId, tx1.txId);
});

Deno.test("WireTx segwit tx - txId is 32 bytes and deterministic", () => {
	const raw = makeSegwitTxBytes();
	const [tx] = WireTx.decode(raw);
	assertEquals(tx.witness.length > 0, true);
	assertEquals(tx.txId.length, 32);
	// Decode again — same txId
	const [tx2] = WireTx.decode(raw);
	assertEquals(tx2.txId, tx.txId);
});

// Segwit tx example: a simple p2wpkh spend
// version(4) + marker(2) + inputs + outputs + witness + locktime
// Constructing a minimal valid segwit tx manually
function makeSegwitTxBytes(): Uint8Array {
	const version = Uint8Array.of(0x02, 0x00, 0x00, 0x00);
	const marker = Uint8Array.of(0x00, 0x01);
	// 1 input
	const inputCount = Uint8Array.of(0x01);
	const prevTxId = new Uint8Array(32).fill(0xaa);
	const vout = Uint8Array.of(0x00, 0x00, 0x00, 0x00);
	const scriptSigLen = Uint8Array.of(0x00); // empty
	const sequence = Uint8Array.of(0xff, 0xff, 0xff, 0xff);
	// 1 output
	const outputCount = Uint8Array.of(0x01);
	const value = Uint8Array.of(0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00); // 10000 sat
	const spk = Uint8Array.of(0x16, 0x00, 0x14, ...new Uint8Array(20).fill(0xbb)); // p2wpkh
	// witness for 1 input: 2 items (sig 72 bytes, pubkey 33 bytes)
	const witnessItemCount = Uint8Array.of(0x02);
	const sig = new Uint8Array(72).fill(0x30);
	const sigLen = Uint8Array.of(sig.length);
	const pubkey = new Uint8Array(33);
	pubkey[0] = 0x02;
	pubkey.fill(0xcc, 1);
	const pubkeyLen = Uint8Array.of(pubkey.length);
	const locktime = Uint8Array.of(0x00, 0x00, 0x00, 0x00);

	const parts = [
		version,
		marker,
		inputCount,
		prevTxId,
		vout,
		scriptSigLen,
		sequence,
		outputCount,
		value,
		spk,
		witnessItemCount,
		sigLen,
		sig,
		pubkeyLen,
		pubkey,
		locktime,
	];
	const total = parts.reduce((s, p) => s + p.length, 0);
	const out = new Uint8Array(total);
	let pos = 0;
	for (const p of parts) {
		out.set(p, pos);
		pos += p.length;
	}
	return out;
}

Deno.test("WireTx decode segwit tx - hasWitness, 1 input 1 output", () => {
	const raw = makeSegwitTxBytes();
	const [tx] = WireTx.decode(raw);
	assertEquals(tx.version, 2);
	assertEquals(tx.inputs.length, 1);
	assertEquals(tx.outputs.length, 1);
	assertEquals(tx.witness.length, 1);
	assertEquals(tx.witness[0]!.length, 2);
});

Deno.test("WireTx encode/decode roundtrip - segwit tx bytes identical", () => {
	const raw = makeSegwitTxBytes();
	const [tx, size] = WireTx.decode(raw);
	assertEquals(size, raw.length);
	const reencoded = WireTx.encode(tx);
	assertEquals(reencoded, raw);
});

Deno.test("WireTx encode is deterministic", () => {
	const raw = decodeHex(BLOCK1_COINBASE_HEX);
	const [tx] = WireTx.decode(raw);
	assertEquals(WireTx.encode(tx), WireTx.encode(tx));
});
