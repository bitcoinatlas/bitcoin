import { assertEquals } from "@std/assert";
import { ScriptNum } from "~/codec/primitives/ScriptNum.ts";

function hex(b: Uint8Array): string {
	return [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
}

// Known CScriptNum encodings (little-endian, sign-magnitude, minimal).
Deno.test("ScriptNum encode - known values", () => {
	assertEquals(hex(ScriptNum.encode(0)), "");
	assertEquals(hex(ScriptNum.encode(1)), "01");
	assertEquals(hex(ScriptNum.encode(127)), "7f");
	assertEquals(hex(ScriptNum.encode(128)), "8000"); // sign-bit pad
	assertEquals(hex(ScriptNum.encode(255)), "ff00");
	assertEquals(hex(ScriptNum.encode(256)), "0001");
	assertEquals(hex(ScriptNum.encode(-1)), "81");
	assertEquals(hex(ScriptNum.encode(-127)), "ff");
	assertEquals(hex(ScriptNum.encode(-128)), "8080");
});

// The BIP34 activation height, the case that motivated this codec.
Deno.test("ScriptNum encode - BIP34 heights", () => {
	assertEquals(hex(ScriptNum.encode(227931)), "5b7a03"); // 0x37a5b LE
	assertEquals(hex(ScriptNum.encode(500000)), "20a107"); // 0x07a120 LE
});

Deno.test("ScriptNum roundtrip - positive and negative", () => {
	for (const v of [0, 1, 127, 128, 255, 256, 65535, 227931, 500000, 16777216, -1, -128, -300000]) {
		const [decoded, read] = ScriptNum.decode(ScriptNum.encode(v));
		assertEquals(decoded, v, `roundtrip ${v}`);
		assertEquals(read, ScriptNum.encode(v).length, `bytesRead ${v}`);
	}
});

Deno.test("ScriptNum roundtrip - large safe integer", () => {
	const v = 1983702; // one of Core's noted post-BIP34 indicated heights
	const [decoded] = ScriptNum.decode(ScriptNum.encode(v));
	assertEquals(decoded, v);
});
