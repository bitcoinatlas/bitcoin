import { assertEquals, assertObjectMatch } from "@std/assert";
import { StoredTxInput } from "~/lib/codec/stored/StoredTxInput.ts";
import type { TxInput } from "~/lib/chain/TxInput.ts";

function assertInputEqual(a: TxInput, b: TxInput) {
	assertEquals(a.prevOut.txId.kind, b.prevOut.txId.kind);
	if (a.prevOut.txId.kind === "raw" && b.prevOut.txId.kind === "raw") {
		assertEquals(a.prevOut.txId.value, b.prevOut.txId.value);
	} else if (a.prevOut.txId.kind === "pointer" && b.prevOut.txId.kind === "pointer") {
		assertEquals(a.prevOut.txId.value, b.prevOut.txId.value);
	}
	assertEquals(a.prevOut.vout, b.prevOut.vout);
	assertEquals(a.scriptSig, b.scriptSig);
	assertObjectMatch(a.sequence, b.sequence);
	assertEquals(a.witness, b.witness);
}

// --- coinbase ---

Deno.test("StoredTxInput roundtrip - coinbase input", () => {
	const input: TxInput = {
		prevOut: { txId: { kind: "coinbase" }, vout: 0xffffffff },
		scriptSig: new Uint8Array([0x03, 0x4e, 0x61, 0x07]),
		sequence: { kind: "final" },
		witness: [],
	};
	const [decoded] = StoredTxInput.decode(StoredTxInput.encode(input));
	assertInputEqual(decoded, input);
});

// --- raw txId ---

Deno.test("StoredTxInput roundtrip - raw txId (unresolved)", () => {
	const txId = new Uint8Array(32).fill(0xab);
	const input: TxInput = {
		prevOut: { txId: { kind: "raw", value: txId }, vout: 2 },
		scriptSig: new Uint8Array([0x47, 0x30]),
		sequence: { kind: "final" },
		witness: [],
	};
	const [decoded] = StoredTxInput.decode(StoredTxInput.encode(input));
	assertInputEqual(decoded, input);
});

// --- pointer txId ---

Deno.test("StoredTxInput roundtrip - pointer txId (resolved)", () => {
	const input: TxInput = {
		prevOut: { txId: { kind: "pointer", value: 987654321 }, vout: 0 },
		scriptSig: new Uint8Array(0),
		sequence: { kind: "final" },
		witness: [],
	};
	const [decoded] = StoredTxInput.decode(StoredTxInput.encode(input));
	assertInputEqual(decoded, input);
});

// --- sequence variants ---

Deno.test("StoredTxInput roundtrip - sequence final (0xffffffff, tag-encoded)", () => {
	// final is the most common — should be stored as a tag-only value
	const txId = new Uint8Array(32).fill(0x01);
	const input: TxInput = {
		prevOut: { txId: { kind: "raw", value: txId }, vout: 0 },
		scriptSig: new Uint8Array(0),
		sequence: { kind: "final" },
		witness: [],
	};
	const encoded = StoredTxInput.encode(input);
	const [decoded] = StoredTxInput.decode(encoded);
	assertObjectMatch(decoded.sequence, { kind: "final" });
});

Deno.test("StoredTxInput roundtrip - sequence 0xfffffffe (tag-encoded)", () => {
	const txId = new Uint8Array(32).fill(0x02);
	const input: TxInput = {
		prevOut: { txId: { kind: "raw", value: txId }, vout: 0 },
		scriptSig: new Uint8Array(0),
		sequence: { kind: "disable", unused: 0xfffffffe & 0x7fffffff },
		witness: [],
	};
	const encoded = StoredTxInput.encode(input);
	const [decoded] = StoredTxInput.decode(encoded);
	assertEquals(decoded.sequence.kind, "disable");
});

Deno.test("StoredTxInput roundtrip - sequence explicit (block relative lock)", () => {
	const txId = new Uint8Array(32).fill(0x03);
	const sequence = { kind: "enable" as const, relativeLock: { kind: "block" as const, blocks: 6 }, unused: 0 };
	const input: TxInput = {
		prevOut: { txId: { kind: "raw", value: txId }, vout: 0 },
		scriptSig: new Uint8Array(0),
		sequence,
		witness: [],
	};
	const [decoded] = StoredTxInput.decode(StoredTxInput.encode(input));
	assertEquals(decoded.sequence.kind, "enable");
	if (decoded.sequence.kind === "enable" && decoded.sequence.relativeLock.kind === "block") {
		assertEquals(decoded.sequence.relativeLock.blocks, 6);
	}
});

Deno.test("StoredTxInput roundtrip - sequence explicit (time relative lock, 1024s)", () => {
	const txId = new Uint8Array(32).fill(0x04);
	const sequence = {
		kind: "enable" as const,
		relativeLock: { kind: "time" as const, seconds: 1024 }, // 2 * 512
		unused: 0,
	};
	const input: TxInput = {
		prevOut: { txId: { kind: "raw", value: txId }, vout: 0 },
		scriptSig: new Uint8Array(0),
		sequence,
		witness: [],
	};
	const [decoded] = StoredTxInput.decode(StoredTxInput.encode(input));
	assertEquals(decoded.sequence.kind, "enable");
	if (decoded.sequence.kind === "enable" && decoded.sequence.relativeLock.kind === "time") {
		assertEquals(decoded.sequence.relativeLock.seconds, 1024);
	}
});

// --- witness ---

Deno.test("StoredTxInput roundtrip - with witness data (raw)", () => {
	const txId = new Uint8Array(32).fill(0x99);
	const sig = new Uint8Array(71).fill(0x30);
	const pubkey = new Uint8Array(33);
	pubkey[0] = 0x02; // compressed pubkey prefix
	pubkey.fill(0xaa, 1);
	const input: TxInput = {
		prevOut: { txId: { kind: "raw", value: txId }, vout: 1 },
		scriptSig: new Uint8Array(0),
		sequence: { kind: "final" },
		witness: [sig, pubkey],
	};
	const [decoded] = StoredTxInput.decode(StoredTxInput.encode(input));
	assertEquals(decoded.prevOut.vout, input.prevOut.vout);
	assertEquals(decoded.witness.length, 2);
	assertEquals(decoded.witness[0], sig);
	assertEquals(decoded.witness[1], pubkey);
});

// --- edge cases ---

Deno.test("StoredTxInput roundtrip - empty scriptSig, empty witness", () => {
	const txId = new Uint8Array(32).fill(0x11);
	const input: TxInput = {
		prevOut: { txId: { kind: "raw", value: txId }, vout: 0 },
		scriptSig: new Uint8Array(0),
		sequence: { kind: "final" },
		witness: [],
	};
	const [decoded] = StoredTxInput.decode(StoredTxInput.encode(input));
	assertInputEqual(decoded, input);
});

Deno.test("StoredTxInput roundtrip - large vout index", () => {
	const input: TxInput = {
		prevOut: { txId: { kind: "pointer", value: 1 }, vout: 65535 },
		scriptSig: new Uint8Array(0),
		sequence: { kind: "final" },
		witness: [],
	};
	const [decoded] = StoredTxInput.decode(StoredTxInput.encode(input));
	assertEquals(decoded.prevOut.vout, 65535);
});

Deno.test("StoredTxInput roundtrip - max pointer value", () => {
	// StoredPointer is u48, max = 2^48 - 1 = 281474976710655
	const maxPointer = 281474976710655;
	const input: TxInput = {
		prevOut: { txId: { kind: "pointer", value: maxPointer }, vout: 0 },
		scriptSig: new Uint8Array(0),
		sequence: { kind: "final" },
		witness: [],
	};
	const [decoded] = StoredTxInput.decode(StoredTxInput.encode(input));
	if (decoded.prevOut.txId.kind === "pointer") {
		assertEquals(decoded.prevOut.txId.value, maxPointer);
	}
});

// --- decode consumes correct byte count ---

Deno.test("StoredTxInput decode reports correct consumed bytes (coinbase)", () => {
	const input: TxInput = {
		prevOut: { txId: { kind: "coinbase" }, vout: 0xffffffff },
		scriptSig: new Uint8Array([0x03]),
		sequence: { kind: "final" },
		witness: [],
	};
	const encoded = StoredTxInput.encode(input);
	const padded = new Uint8Array(encoded.length + 8);
	padded.set(encoded, 0);
	const [, size] = StoredTxInput.decode(padded);
	assertEquals(size, encoded.length);
});

Deno.test("StoredTxInput decode reports correct consumed bytes (raw txId)", () => {
	const txId = new Uint8Array(32).fill(0xcc);
	const input: TxInput = {
		prevOut: { txId: { kind: "raw", value: txId }, vout: 0 },
		scriptSig: new Uint8Array([0x01, 0x02]),
		sequence: { kind: "final" },
		witness: [],
	};
	const encoded = StoredTxInput.encode(input);
	const padded = new Uint8Array(encoded.length + 8);
	padded.set(encoded, 0);
	const [, size] = StoredTxInput.decode(padded);
	assertEquals(size, encoded.length);
});

// --- determinism ---

Deno.test("StoredTxInput encode is deterministic", () => {
	const txId = new Uint8Array(32).fill(0xab);
	const input: TxInput = {
		prevOut: { txId: { kind: "raw", value: txId }, vout: 5 },
		scriptSig: new Uint8Array([0x01, 0x02]),
		sequence: { kind: "final" },
		witness: [],
	};
	const a = StoredTxInput.encode(input);
	const b = StoredTxInput.encode(input);
	assertEquals(a, b);
});
