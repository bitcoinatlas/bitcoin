import { assertEquals } from "@std/assert";
import { decodeHex, encodeHex } from "@std/encoding";
import { WireBlockHeader } from "~/lib/codec/wire/WireBlockHeader.ts";

// Block 1 header (raw 80 bytes)
const BLOCK1_HEADER_HEX = "01000000" + // version
	"6fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000" + // prevHash (LE)
	"982051fd1e4ba744bbbe680e1fee14677ba1a3c3540bf7b1cdb606e857233e0e" + // merkleRoot (LE)
	"61bc6649" + // timestamp
	"ffff001d" + // bits
	"01e36299"; // nonce

Deno.test("WireBlockHeader decode block1 - hash matches known value", () => {
	const raw = decodeHex(BLOCK1_HEADER_HEX);
	const [header] = WireBlockHeader.decode(raw);
	// Known block 1 hash (big-endian display)
	const expected = "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048";
	assertEquals(encodeHex(header.hash.slice().reverse()), expected);
});

Deno.test("WireBlockHeader decode block1 - fields correct", () => {
	const raw = decodeHex(BLOCK1_HEADER_HEX);
	const [header] = WireBlockHeader.decode(raw);
	assertEquals(header.version, 1);
	assertEquals(header.bits, 0x1d00ffff);
	assertEquals(header.nonce, 2573394689); // 0x9962e301
	assertEquals(header.timestamp, 1231469665); // 0x4966bc61
});

Deno.test("WireBlockHeader encode/decode roundtrip - bytes identical", () => {
	const raw = decodeHex(BLOCK1_HEADER_HEX);
	const [header, size] = WireBlockHeader.decode(raw);
	assertEquals(size, 80);
	const reencoded = WireBlockHeader.encode(header);
	assertEquals(reencoded, raw);
});

Deno.test("WireBlockHeader roundtrip preserves hash", () => {
	const raw = decodeHex(BLOCK1_HEADER_HEX);
	const [h1] = WireBlockHeader.decode(raw);
	const [h2] = WireBlockHeader.decode(WireBlockHeader.encode(h1));
	assertEquals(h2.hash, h1.hash);
});

Deno.test("WireBlockHeader encode is deterministic", () => {
	const raw = decodeHex(BLOCK1_HEADER_HEX);
	const [header] = WireBlockHeader.decode(raw);
	assertEquals(WireBlockHeader.encode(header), WireBlockHeader.encode(header));
});
