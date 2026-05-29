import { assertEquals } from "@std/assert";
import { detectWitnessPattern, reconstructWitness, StoredWitness, StoredWitnessPattern } from "~/lib/codec/stored/StoredWitness.ts";

function makeBytes(len: number, fill: number): Uint8Array {
	return new Uint8Array(len).fill(fill);
}

// p2wpkh: [sig(71-73), compressed pubkey(33)]
function makeP2WPKHItems(): Uint8Array[] {
	const sig = makeBytes(72, 0x30);
	const pubkey = new Uint8Array(33);
	pubkey[0] = 0x02;
	pubkey.fill(0xaa, 1);
	return [sig, pubkey];
}

// p2trKeyPath: [sig(64)]
function makeP2TRItems(): Uint8Array[] {
	return [makeBytes(64, 0x55)];
}

// raw: anything unrecognized
function makeRawItems(): Uint8Array[] {
	return [makeBytes(5, 0x01), makeBytes(10, 0x02), makeBytes(3, 0x03)];
}

Deno.test("StoredWitness roundtrip - empty (no witness)", () => {
	const items: Uint8Array[] = [];
	const [decoded] = StoredWitness.decode(StoredWitness.encode(items));
	assertEquals(decoded, items);
});

Deno.test("StoredWitness roundtrip - p2wpkh", () => {
	const items = makeP2WPKHItems();
	const [decoded] = StoredWitness.decode(StoredWitness.encode(items));
	assertEquals(decoded.length, 2);
	// sig may be trimmed; check pubkey is intact
	assertEquals(decoded[1], items[1]);
});

Deno.test("StoredWitness roundtrip - p2tr key path sig(64)", () => {
	const items = makeP2TRItems();
	const [decoded] = StoredWitness.decode(StoredWitness.encode(items));
	assertEquals(decoded.length, 1);
	assertEquals(decoded[0], items[0]);
});

Deno.test("StoredWitness roundtrip - p2tr key path sig(65)", () => {
	const items = [makeBytes(65, 0x66)];
	const [decoded] = StoredWitness.decode(StoredWitness.encode(items));
	assertEquals(decoded.length, 1);
	assertEquals(decoded[0], items[0]);
});

Deno.test("StoredWitness roundtrip - raw items", () => {
	const items = makeRawItems();
	const [decoded] = StoredWitness.decode(StoredWitness.encode(items));
	assertEquals(decoded.length, items.length);
	for (let i = 0; i < items.length; i++) {
		assertEquals(decoded[i], items[i]);
	}
});

Deno.test("StoredWitnessPattern roundtrip - raw", () => {
	const pattern = detectWitnessPattern(makeRawItems());
	assertEquals(pattern.kind, "raw");
	const encoded = StoredWitnessPattern.encode(pattern);
	const [decoded] = StoredWitnessPattern.decode(encoded);
	assertEquals(decoded.kind, "raw");
});

Deno.test("StoredWitnessPattern roundtrip - p2wpkh", () => {
	const pattern = detectWitnessPattern(makeP2WPKHItems());
	assertEquals(pattern.kind, "p2wpkh");
	const encoded = StoredWitnessPattern.encode(pattern);
	const [decoded] = StoredWitnessPattern.decode(encoded);
	assertEquals(decoded.kind, "p2wpkh");
	if (decoded.kind === "p2wpkh" && pattern.kind === "p2wpkh") {
		assertEquals(decoded.value.pubkey, pattern.value.pubkey);
	}
});

Deno.test("detectWitnessPattern + reconstructWitness - p2wpkh sig lengths 71-73", () => {
	for (const sigLen of [71, 72, 73]) {
		const sig = makeBytes(sigLen, 0x30);
		const pubkey = new Uint8Array(33);
		pubkey[0] = 0x02;
		pubkey.fill(0xcc, 1);
		const items = [sig, pubkey];
		const pattern = detectWitnessPattern(items);
		assertEquals(pattern.kind, "p2wpkh");
		const reconstructed = reconstructWitness(pattern);
		assertEquals(reconstructed[0]!.length, sigLen); // trimmed back to original
		assertEquals(reconstructed[1], pubkey);
	}
});

Deno.test("StoredWitness encode is deterministic", () => {
	const items = makeP2WPKHItems();
	const a = StoredWitness.encode(items);
	const b = StoredWitness.encode(items);
	assertEquals(a, b);
});
