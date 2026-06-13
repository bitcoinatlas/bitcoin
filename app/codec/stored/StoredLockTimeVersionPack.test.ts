import { assertEquals, assertObjectMatch, assertThrows } from "@std/assert";
import { LockTimeVersionPack } from "~/codec/stored/StoredLockTimeVersionPack.ts";

// TAG constants mirror the implementation
const TAG_RAW = 0;
const TAG_V1_NONE = 1;
const TAG_V2_NONE = 2;
const TAG_V1_SOME = 3;
const TAG_V2_SOME = 4;

// --- roundtrip helpers ---

function rt(value: { version: number; lockTime: Parameters<typeof LockTimeVersionPack.encode>[0]["lockTime"] }) {
	const encoded = LockTimeVersionPack.encode(value);
	const [decoded, size] = LockTimeVersionPack.decode(encoded);
	return { decoded, size, encoded };
}

// --- tag byte selection ---

Deno.test("LockTimeVersionPack encodes v1 + no locktime as TAG_V1_NONE (1 byte)", () => {
	const { encoded } = rt({ version: 1, lockTime: { kind: "none" } });
	assertEquals(encoded.length, 1);
	assertEquals(encoded[0], TAG_V1_NONE);
});

Deno.test("LockTimeVersionPack encodes v2 + no locktime as TAG_V2_NONE (1 byte)", () => {
	const { encoded } = rt({ version: 2, lockTime: { kind: "none" } });
	assertEquals(encoded.length, 1);
	assertEquals(encoded[0], TAG_V2_NONE);
});

Deno.test("LockTimeVersionPack encodes v1 + block locktime as TAG_V1_SOME (5 bytes)", () => {
	const { encoded } = rt({ version: 1, lockTime: { kind: "block", height: 100 } });
	assertEquals(encoded.length, 5); // 1 tag + 4 LockTime
	assertEquals(encoded[0], TAG_V1_SOME);
});

Deno.test("LockTimeVersionPack encodes v2 + time locktime as TAG_V2_SOME (5 bytes)", () => {
	const { encoded } = rt({ version: 2, lockTime: { kind: "time", timestamp: 1_500_000_000 } });
	assertEquals(encoded.length, 5);
	assertEquals(encoded[0], TAG_V2_SOME);
});

Deno.test("LockTimeVersionPack encodes non-standard version as TAG_RAW (9 bytes)", () => {
	const { encoded } = rt({ version: 3, lockTime: { kind: "none" } });
	assertEquals(encoded.length, 9); // 1 tag + 4 version + 4 LockTime
	assertEquals(encoded[0], TAG_RAW);
});

// --- roundtrips ---

Deno.test("LockTimeVersionPack roundtrip - v1, locktime none", () => {
	const { decoded, size } = rt({ version: 1, lockTime: { kind: "none" } });
	assertEquals(decoded.version, 1);
	assertObjectMatch(decoded.lockTime, { kind: "none" });
	assertEquals(size, 1);
});

Deno.test("LockTimeVersionPack roundtrip - v2, locktime none", () => {
	const { decoded, size } = rt({ version: 2, lockTime: { kind: "none" } });
	assertEquals(decoded.version, 2);
	assertObjectMatch(decoded.lockTime, { kind: "none" });
	assertEquals(size, 1);
});

Deno.test("LockTimeVersionPack roundtrip - v1, block locktime height=300000", () => {
	const { decoded, size } = rt({ version: 1, lockTime: { kind: "block", height: 300_000 } });
	assertEquals(decoded.version, 1);
	assertObjectMatch(decoded.lockTime, { kind: "block", height: 300_000 });
	assertEquals(size, 5);
});

Deno.test("LockTimeVersionPack roundtrip - v2, time locktime timestamp=1400000000", () => {
	const { decoded, size } = rt({ version: 2, lockTime: { kind: "time", timestamp: 1_400_000_000 } });
	assertEquals(decoded.version, 2);
	assertObjectMatch(decoded.lockTime, { kind: "time", timestamp: 1_400_000_000 });
	assertEquals(size, 5);
});

Deno.test("LockTimeVersionPack roundtrip - version 3 (raw), block locktime", () => {
	const { decoded, size } = rt({ version: 3, lockTime: { kind: "block", height: 42 } });
	assertEquals(decoded.version, 3);
	assertObjectMatch(decoded.lockTime, { kind: "block", height: 42 });
	assertEquals(size, 9);
});

Deno.test("LockTimeVersionPack roundtrip - version 0 (raw), no locktime", () => {
	const { decoded, size } = rt({ version: 0, lockTime: { kind: "none" } });
	assertEquals(decoded.version, 0);
	assertObjectMatch(decoded.lockTime, { kind: "none" });
	assertEquals(size, 9);
});

// --- decode consumes only what it writes ---

Deno.test("LockTimeVersionPack decode reports correct consumed bytes with trailing data", () => {
	const encoded = LockTimeVersionPack.encode({ version: 1, lockTime: { kind: "none" } });
	const padded = new Uint8Array(encoded.length + 10);
	padded.set(encoded, 0);
	const [, size] = LockTimeVersionPack.decode(padded);
	assertEquals(size, 1);
});

Deno.test("LockTimeVersionPack decode reports correct consumed bytes for TAG_V1_SOME", () => {
	const encoded = LockTimeVersionPack.encode({ version: 1, lockTime: { kind: "block", height: 1 } });
	const padded = new Uint8Array(encoded.length + 10);
	padded.set(encoded, 0);
	const [, size] = LockTimeVersionPack.decode(padded);
	assertEquals(size, 5);
});

// --- unknown tag throws ---

Deno.test("LockTimeVersionPack decode throws on unknown tag byte", () => {
	const bad = new Uint8Array([99, 0, 0, 0, 0, 0, 0, 0, 0]);
	assertThrows(() => LockTimeVersionPack.decode(bad), Error, "Unknown LockTimeVersionPack tag");
});

// --- determinism ---

Deno.test("LockTimeVersionPack encode is deterministic", () => {
	const value = { version: 2, lockTime: { kind: "time" as const, timestamp: 1_600_000_000 } };
	assertEquals(LockTimeVersionPack.encode(value), LockTimeVersionPack.encode(value));
});
