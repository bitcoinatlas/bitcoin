import { assertEquals } from "@std/assert";
import { WireTxInput } from "~/codec/wire/WireTxInput.ts";

function makeTxId(fill: number): Uint8Array {
	return new Uint8Array(32).fill(fill);
}

Deno.test("WireTxInput roundtrip - normal input", () => {
	const input = {
		prevOut: { txId: makeTxId(0xab), vout: 2 },
		scriptSig: Uint8Array.of(0x47, 0x30, 0x44),
		sequence: { kind: "final" as const },
	};
	const [decoded] = WireTxInput.decode(WireTxInput.encode(input));
	assertEquals(decoded.prevOut.txId, input.prevOut.txId);
	assertEquals(decoded.prevOut.vout, input.prevOut.vout);
	assertEquals(decoded.scriptSig, input.scriptSig);
	assertEquals(decoded.sequence, input.sequence);
});

Deno.test("WireTxInput roundtrip - coinbase (all-zero txId, vout 0xffffffff)", () => {
	const input = {
		prevOut: { txId: makeTxId(0x00), vout: 0xffffffff },
		scriptSig: Uint8Array.of(0x03, 0x4e, 0x61, 0x07),
		sequence: { kind: "final" as const },
	};
	const [decoded] = WireTxInput.decode(WireTxInput.encode(input));
	assertEquals(decoded.prevOut.txId, input.prevOut.txId);
	assertEquals(decoded.prevOut.vout, 0xffffffff);
});

Deno.test("WireTxInput roundtrip - empty scriptSig", () => {
	const input = {
		prevOut: { txId: makeTxId(0x11), vout: 0 },
		scriptSig: new Uint8Array(0),
		sequence: { kind: "final" as const },
	};
	const [decoded] = WireTxInput.decode(WireTxInput.encode(input));
	assertEquals(decoded.scriptSig, input.scriptSig);
});

Deno.test("WireTxInput roundtrip - txId bytes preserved exactly", () => {
	const txId = new Uint8Array(32);
	for (let i = 0; i < 32; i++) txId[i] = i;
	const input = {
		prevOut: { txId, vout: 5 },
		scriptSig: new Uint8Array(0),
		sequence: { kind: "final" as const },
	};
	const [decoded] = WireTxInput.decode(WireTxInput.encode(input));
	assertEquals(decoded.prevOut.txId, txId);
});

Deno.test("WireTxInput encode is deterministic", () => {
	const input = {
		prevOut: { txId: makeTxId(0xcc), vout: 1 },
		scriptSig: Uint8Array.of(0x01),
		sequence: { kind: "final" as const },
	};
	assertEquals(WireTxInput.encode(input), WireTxInput.encode(input));
});
