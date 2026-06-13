import { assertEquals, assertObjectMatch } from "@std/assert";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";

function makeHash(fill: number): Uint8Array {
	return new Uint8Array(32).fill(fill);
}

const SAMPLE_HEADER: StoredBlockHeader = {
	version: 1,
	prevHash: makeHash(1),
	merkleRoot: makeHash(2),
	timestamp: 1231469665,
	bits: 0x1d00ffff,
	nonce: 2083236893,
	hash: makeHash(3),
};

Deno.test("StoredBlockHeader encode/decode roundtrip", () => {
	const encoded = StoredBlockHeader.encode(SAMPLE_HEADER);
	const [decoded] = StoredBlockHeader.decode(encoded);
	assertObjectMatch(decoded, SAMPLE_HEADER);
	// hash is computed from the encoded header bytes (sha256d), not stored directly
	assertEquals(decoded.hash.length, 32);
});

Deno.test("StoredBlockHeader encode is deterministic", () => {
	const a = StoredBlockHeader.encode(SAMPLE_HEADER);
	const b = StoredBlockHeader.encode(SAMPLE_HEADER);
	assertEquals(a, b);
});
