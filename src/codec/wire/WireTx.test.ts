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

// ─────────────────────────────────────────────────────────────────────────────
// Ground-truth vectors. Hashes are in *internal* (little-endian) byte order —
// i.e. the raw sha256d output, NOT the reversed big-endian form explorers show.
// Both were verified independently against Bitcoin Core / block data:
//   - segwit: BIP143 P2WPKH example tx (display txid eb83f2…9ad2)
//   - legacy: block-170 Satoshi→Hal Finney tx (display txid f4184fc5…31e9e16)
// ─────────────────────────────────────────────────────────────────────────────

function hexToBytes(hex: string): Uint8Array {
	const out = new Uint8Array(hex.length / 2);
	for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
	return out;
}

function toHex(bytes: Uint8Array): string {
	let s = "";
	for (const b of bytes) s += b.toString(16).padStart(2, "0");
	return s;
}

/** Reverse a 32-byte hash to the big-endian form block explorers display. */
function displayHash(internalLE: Uint8Array): string {
	return toHex(internalLE.slice().reverse());
}

const VECTORS = {
	segwit: {
		label: "P2WPKH segwit (BIP143 example): 1 input, 1 output, 1 witness item stack of 2",
		raw: hexToBytes(
			"01000000000101" +
				"8ac60eb9575db5b2d987e29f301b5b819ea83a5c6579d282d189cc04b8e151ef01000000" +
				"00ffffffff01" +
				"7ff7f105000000001976a91479091972186c449eb1ded22b78e40d009bdf008988ac" +
				"0247304402203609e17b84f6a7d30c80bfa610b5b4542f32a8a0d5447a12fb1366d7f01cc44a" +
				"0220573a954c4518331561406f90300e8f3358f51928d43c212a8caed02de67eebee01" +
				"21025476c2e83188368da1ff3e292e7acafcdb3566bb0ad253f62fdec7cdb5de5dbb" +
				"00000000",
		),
		txId: hexToBytes("d29a8fefe564a7d70e7789401ec961b559eac74fe147e02e16274fee0af283eb"),
		wtxId: hexToBytes("2f6494560c2886adc6ea5b9a0a2703ec0d115c9ffd49738bab2556821178d686"),
		displayTxId: "eb83f20aee4f27162ee047e14fc7ea59b561c91e4089770ed7a764e5ef8f9ad2",
	},
	legacy: {
		label: "legacy non-segwit (block 170, Satoshi→Hal): 1 input, 2 outputs, no witness",
		raw: hexToBytes(
			"0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd370400000000" +
				"4847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd41" +
				"0220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff" +
				"0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414" +
				"e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac" +
				"00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0" +
				"eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000",
		),
		txId: hexToBytes("169e1e83e930853391bc6f35f605c6754cfead57cf8387639d3b4096c54f18f4"),
		wtxId: hexToBytes("169e1e83e930853391bc6f35f605c6754cfead57cf8387639d3b4096c54f18f4"),
		displayTxId: "f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16",
	},
} as const;

// ── segwit ──────────────────────────────────────────────────────────────────

Deno.test(`WireTx / decode ${VECTORS.segwit.label}`, async (t) => {
	const v = VECTORS.segwit;
	const [tx, consumed] = WireTx.decode(v.raw);

	await t.step("consumes the whole buffer", () => {
		assertEquals(consumed, v.raw.length);
	});

	await t.step("parses structure", () => {
		assertEquals(tx.inputs.length, 1);
		assertEquals(tx.outputs.length, 1);
		assertEquals(tx.witness.length, 1); // one input → one witness stack
		assertEquals(tx.witness[0]!.length, 2); // sig + pubkey
	});

	await t.step("txId matches (legacy serialization, marker+witness stripped)", () => {
		assertEquals(toHex(tx.txId), toHex(v.txId));
		assertEquals(displayHash(tx.txId), v.displayTxId);
	});

	await t.step("wtxId matches (full witness serialization)", () => {
		assertEquals(toHex(tx.wtxId), toHex(v.wtxId));
	});

	await t.step("txId ≠ wtxId for a witness tx", () => {
		assertEquals(toHex(tx.txId) === toHex(tx.wtxId), false);
	});
});

// ── legacy ──────────────────────────────────────────────────────────────────

Deno.test(`WireTx / decode ${VECTORS.legacy.label}`, async (t) => {
	const v = VECTORS.legacy;
	const [tx, consumed] = WireTx.decode(v.raw);

	await t.step("consumes the whole buffer", () => {
		assertEquals(consumed, v.raw.length);
	});

	await t.step("parses structure with no witness", () => {
		assertEquals(tx.inputs.length, 1);
		assertEquals(tx.outputs.length, 2);
		assertEquals(tx.witness.length, 0);
	});

	await t.step("txId matches", () => {
		assertEquals(toHex(tx.txId), toHex(v.txId));
		assertEquals(displayHash(tx.txId), v.displayTxId);
	});

	await t.step("txId === wtxId for a witness-less tx (BIP141)", () => {
		assertEquals(toHex(tx.txId), toHex(tx.wtxId));
	});
});

// ── round-trip ────────────────────────────────────────────────────────────────
// encode(decode(raw)) must reproduce the original wire bytes exactly, including
// the segwit marker/flag and witness. This is what would catch an off-by-one in
// the bodyStart/bodyEnd slicing that still produced a valid-looking hash.

for (const [name, v] of Object.entries(VECTORS)) {
	Deno.test(`WireTx / round-trip encode(decode()) is byte-identical — ${name}`, () => {
		const [tx] = WireTx.decode(v.raw);
		const reencoded = WireTx.encode(tx);
		assertEquals(toHex(reencoded), toHex(v.raw));
	});

	Deno.test(`WireTx / encodeInto matches encode — ${name}`, () => {
		const [tx] = WireTx.decode(v.raw);
		const expected = WireTx.encode(tx);
		const target = new Uint8Array(expected.length + 8); // padded to catch overruns
		const written = WireTx.encodeInto(tx, target, 4); // non-zero offset on purpose
		assertEquals(written, expected.length);
		assertEquals(toHex(target.subarray(4, 4 + written)), toHex(expected));
	});
}
