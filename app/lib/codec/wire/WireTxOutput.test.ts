import { assertEquals } from "@std/assert";
import { WireTxOutput } from "~/lib/codec/wire/WireTxOutput.ts";

Deno.test("WireTxOutput roundtrip - p2pkh script", () => {
	const out = {
		value: 5000000000n,
		scriptPubKey: Uint8Array.of(0x76, 0xa9, 0x14, ...new Uint8Array(20).fill(0xab), 0x88, 0xac),
	};
	const [decoded] = WireTxOutput.decode(WireTxOutput.encode(out));
	assertEquals(decoded.value, out.value);
	assertEquals(decoded.scriptPubKey, out.scriptPubKey);
});

Deno.test("WireTxOutput roundtrip - zero value", () => {
	const out = { value: 0n, scriptPubKey: new Uint8Array(0) };
	const [decoded] = WireTxOutput.decode(WireTxOutput.encode(out));
	assertEquals(decoded.value, 0n);
	assertEquals(decoded.scriptPubKey, new Uint8Array(0));
});

Deno.test("WireTxOutput roundtrip - max satoshis (21M BTC)", () => {
	const out = { value: 2100000000000000n, scriptPubKey: Uint8Array.of(0x51) };
	const [decoded] = WireTxOutput.decode(WireTxOutput.encode(out));
	assertEquals(decoded.value, 2100000000000000n);
});

Deno.test("WireTxOutput roundtrip - p2wpkh script", () => {
	const out = {
		value: 546n,
		scriptPubKey: Uint8Array.of(0x00, 0x14, ...new Uint8Array(20).fill(0x33)),
	};
	const [decoded] = WireTxOutput.decode(WireTxOutput.encode(out));
	assertEquals(decoded.scriptPubKey, out.scriptPubKey);
});

Deno.test("WireTxOutput encode is deterministic", () => {
	const out = { value: 100000n, scriptPubKey: Uint8Array.of(0x51, 0xac) };
	assertEquals(WireTxOutput.encode(out), WireTxOutput.encode(out));
});
