import { assertEquals } from "@std/assert";
import { WireSegwitMarker } from "~/lib/codec/wire/WireSegwitMarker.ts";

Deno.test("WireSegwitMarker encode true = 0x00 0x01", () => {
	assertEquals(WireSegwitMarker.encode(true), Uint8Array.of(0x00, 0x01));
});

Deno.test("WireSegwitMarker encode false = empty", () => {
	assertEquals(WireSegwitMarker.encode(false), new Uint8Array(0));
});

Deno.test("WireSegwitMarker decode 0x00 0x01 = true, consumed 2", () => {
	const [val, size] = WireSegwitMarker.decode(Uint8Array.of(0x00, 0x01, 0x99));
	assertEquals(val, true);
	assertEquals(size, 2);
});

Deno.test("WireSegwitMarker decode non-marker = false, consumed 0", () => {
	const [val, size] = WireSegwitMarker.decode(Uint8Array.of(0x01, 0x00));
	assertEquals(val, false);
	assertEquals(size, 0);
});

Deno.test("WireSegwitMarker decode empty = false, consumed 0", () => {
	const [val, size] = WireSegwitMarker.decode(new Uint8Array(0));
	assertEquals(val, false);
	assertEquals(size, 0);
});

Deno.test("WireSegwitMarker roundtrip true", () => {
	const [val] = WireSegwitMarker.decode(WireSegwitMarker.encode(true));
	assertEquals(val, true);
});

Deno.test("WireSegwitMarker roundtrip false", () => {
	const [val] = WireSegwitMarker.decode(WireSegwitMarker.encode(false));
	assertEquals(val, false);
});
