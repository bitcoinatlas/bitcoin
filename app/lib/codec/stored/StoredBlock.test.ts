import { assertEquals } from "@std/assert";
import { StoredBlock } from "~/lib/codec/stored/StoredBlock.ts";

function makeHash(fill: number): Uint8Array {
	return new Uint8Array(32).fill(fill);
}

const SAMPLE_BLOCK: StoredBlock = {
	header: {
		version: 1,
		prevHash: makeHash(1),
		merkleRoot: makeHash(2),
		timestamp: 1231469665,
		bits: 0x1d00ffff,
		nonce: 2083236893,
		hash: makeHash(3),
	},
	pointer: 999_888_777,
};

Deno.test("StoredBlock encode/decode roundtrip", () => {
	const encoded = StoredBlock.encode(SAMPLE_BLOCK);
	const [decoded] = StoredBlock.decode(encoded);
	assertEquals(decoded.pointer, SAMPLE_BLOCK.pointer);
	assertEquals(decoded.header.version, SAMPLE_BLOCK.header.version);
	assertEquals(decoded.header.bits, SAMPLE_BLOCK.header.bits);
	assertEquals(decoded.header.nonce, SAMPLE_BLOCK.header.nonce);
	assertEquals(decoded.header.timestamp, SAMPLE_BLOCK.header.timestamp);
	assertEquals(decoded.header.prevHash, SAMPLE_BLOCK.header.prevHash);
	assertEquals(decoded.header.merkleRoot, SAMPLE_BLOCK.header.merkleRoot);
	// hash is computed from the encoded header bytes (sha256d), not stored directly
	assertEquals(decoded.header.hash.length, 32);
});

Deno.test("StoredBlock encode is deterministic", () => {
	const a = StoredBlock.encode(SAMPLE_BLOCK);
	const b = StoredBlock.encode(SAMPLE_BLOCK);
	assertEquals(a, b);
});
