import { assertEquals } from "@std/assert";
import { StoredPointer } from "~/lib/codec/stored/StoredPointer.ts";

Deno.test("StoredPointer roundtrip - zero", () => {
	const val = 0;
	const [decoded] = StoredPointer.decode(StoredPointer.encode(val));
	assertEquals(decoded, val);
});

Deno.test("StoredPointer roundtrip - small", () => {
	const val = 12345;
	const [decoded] = StoredPointer.decode(StoredPointer.encode(val));
	assertEquals(decoded, val);
});

Deno.test("StoredPointer roundtrip - large", () => {
	const val = 0xffffffffffff; // max u48
	const [decoded] = StoredPointer.decode(StoredPointer.encode(val));
	assertEquals(decoded, val);
});

Deno.test("StoredPointer roundtrip - typical blob offset", () => {
	const val = 18_500_000_000; // ~18.5 GB
	const [decoded] = StoredPointer.decode(StoredPointer.encode(val));
	assertEquals(decoded, val);
});

Deno.test("StoredPointer encode is 6 bytes", () => {
	assertEquals(StoredPointer.encode(42).length, 6);
});
