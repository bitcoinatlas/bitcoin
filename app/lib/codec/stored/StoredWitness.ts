import { ArrayCodec, BytesCodec, Codec, StructCodec, UnionCodec } from "@nomadshiba/codec";
import { CompactSize } from "~/lib/codec/primitives.ts";

// ── Fixed-size byte array helpers ─────────────────────────────────────────────

const Sig73 = new BytesCodec({ size: 73 });
const Sig65 = new BytesCodec({ size: 65 });
const Pubkey = new BytesCodec({ size: 33 });
const Script34 = new BytesCodec({ size: 34 });
const Script71 = new BytesCodec({ size: 71 });
const Script105 = new BytesCodec({ size: 105 });
const Script39 = new BytesCodec({ size: 39 });

// ── Raw witness fallback ──────────────────────────────────────────────────────
//
// Raw is stored as a CompactSize-prefixed array of CompactSize-prefixed items,
// i.e. the standard Bitcoin witness wire format verbatim.
// We reuse ArrayCodec with CompactSize as the count/length codec so decoding
// is handled uniformly and we never hand-roll offset arithmetic here.

const RawWitnessItemCodec = new BytesCodec({ lengthCodec: CompactSize });
const RawWitnessCodec = new ArrayCodec(RawWitnessItemCodec, { countCodec: CompactSize });

// ── Struct codecs for each recognized pattern ─────────────────────────────────

const P2WPKHCodec = new StructCodec({
	sig: Sig73,
	pubkey: Pubkey,
});

const P2TRKeyPathCodec = new StructCodec({
	sig: Sig65,
});

const P2WSH1of1Codec = new StructCodec({
	sig: Sig73,
	script: Script34,
});

const P2WSH2of2Codec = new StructCodec({
	sig1: Sig73,
	sig2: Sig73,
	script: Script71,
});

const P2WSH2of3Codec = new StructCodec({
	sig1: Sig73,
	sig2: Sig73,
	script: Script105,
});

const P2WSH3of3Codec = new StructCodec({
	sig1: Sig73,
	sig2: Sig73,
	sig3: Sig73,
	script: Script105,
});

const P2WSH1of2Codec = new StructCodec({
	sig: Sig73,
	script: Script71,
});

const P2WSH1of3Codec = new StructCodec({
	sig: Sig73,
	script: Script105,
});

const P2WSHTimelockCodec = new StructCodec({
	sig: Sig73,
	script: Script39,
});

// ── Union ─────────────────────────────────────────────────────────────────────
//
// UnionCodec assigns indices alphabetically. We need stable explicit indices
// matching the original format (raw=0, p2wpkh=1, …), so we use a custom
// U8-indexed union via indexCodec but rely on the alphabetical sort being
// deterministic. Since the original code used a manual Record<string,number>
// map, we replicate the same ordering by naming variants accordingly.
//
// Alphabetical order of the keys below:
//   p2trKeyPath=0, p2wsh1of1=1, p2wsh1of2=2, p2wsh1of3=3,
//   p2wsh2of2=4,   p2wsh2of3=5, p2wsh3of3=6, p2wshTimelock=7,
//   p2wpkh=8,      raw=9
//
// That's NOT the same as the original. We need to preserve the original wire
// format (raw=0, p2wpkh=1, …) for backward compat. The cleanest solution is
// to keep the union internal and use a transform layer for the tag byte, OR
// prefix each key name so alphabetical order matches the desired numeric order.
// We go with explicit 0-padded numeric prefixes on the variant names so the
// sort is unambiguous and matches the wire format exactly.

const StoredWitnessUnion = new UnionCodec({
	"0_raw": RawWitnessCodec,
	"1_p2wpkh": P2WPKHCodec,
	"2_p2trKeyPath": P2TRKeyPathCodec,
	"3_p2wsh1of1": P2WSH1of1Codec,
	"4_p2wsh2of2": P2WSH2of2Codec,
	"5_p2wsh2of3": P2WSH2of3Codec,
	"6_p2wsh3of3": P2WSH3of3Codec,
	"7_p2wsh1of2": P2WSH1of2Codec,
	"8_p2wsh1of3": P2WSH1of3Codec,
	"9_p2wshTimelock": P2WSHTimelockCodec,
});

type StoredWitnessUnionOutput = typeof StoredWitnessUnion extends UnionCodec<infer T>
	? import("@nomadshiba/codec").UnionOutput<T>
	: never;

// ── Padding helpers ───────────────────────────────────────────────────────────
//
// Sigs are padded to fixed width on encode (trailing zeros) and trimmed on
// decode by stripping trailing zeros. This is safe because valid DER sigs and
// Schnorr sigs never end in 0x00.

function padTo(src: Uint8Array, size: number): Uint8Array {
	if (src.length === size) return src;
	const out = new Uint8Array(size);
	out.set(src);
	return out;
}

function trimTrailingZeros(src: Uint8Array): Uint8Array {
	let len = src.length;
	while (len > 0 && src[len - 1] === 0) len--;
	return src.subarray(0, len);
}

// ── Pattern detection ─────────────────────────────────────────────────────────

function detectPattern(items: Uint8Array[]): StoredWitnessUnionOutput {
	// P2WPKH: [sig(71-73), pubkey(33)]
	if (items.length === 2) {
		const [sig, pubkey] = [items[0]!, items[1]!];
		if (
			sig.length >= 71 && sig.length <= 73 &&
			pubkey.length === 33 &&
			(pubkey[0] === 0x02 || pubkey[0] === 0x03)
		) {
			return { kind: "1_p2wpkh", value: { sig: padTo(sig, 73), pubkey } };
		}
	}

	// P2TR key path: [sig(64|65)]
	if (items.length === 1) {
		const sig = items[0]!;
		if (sig.length === 64 || sig.length === 65) {
			return { kind: "2_p2trKeyPath", value: { sig: padTo(sig, 65) } };
		}
	}

	// P2WSH patterns: [OP_0(empty), ...sigs, script] where script ends with OP_CHECKMULTISIG (0xae)
	if (items.length >= 2 && items[0]!.length === 0) {
		const script = items[items.length - 1]!;

		if (script.length >= 34 && script[script.length - 1] === 0xae) {
			// 1-of-1: OP_1 ... OP_1 OP_CHECKMULTISIG, script=34
			if (
				items.length === 3 && script.length === 34 &&
				script[0] === 0x51 && script[script.length - 2] === 0x51
			) {
				const sig = items[1]!;
				if (sig.length >= 71 && sig.length <= 73) {
					return { kind: "3_p2wsh1of1", value: { sig: padTo(sig, 73), script } };
				}
			}
			// 2-of-2: OP_2 ... OP_2 OP_CHECKMULTISIG, script=71
			if (
				items.length === 4 && script.length === 71 &&
				script[0] === 0x52 && script[script.length - 2] === 0x52
			) {
				const [sig1, sig2] = [items[1]!, items[2]!];
				if (sig1.length >= 71 && sig1.length <= 73 && sig2.length >= 71 && sig2.length <= 73) {
					return { kind: "4_p2wsh2of2", value: { sig1: padTo(sig1, 73), sig2: padTo(sig2, 73), script } };
				}
			}
			// 2-of-3: OP_2 ... OP_3 OP_CHECKMULTISIG, script=105
			if (
				items.length === 4 && script.length === 105 &&
				script[0] === 0x52 && script[script.length - 2] === 0x53
			) {
				const [sig1, sig2] = [items[1]!, items[2]!];
				if (sig1.length >= 71 && sig1.length <= 73 && sig2.length >= 71 && sig2.length <= 73) {
					return { kind: "5_p2wsh2of3", value: { sig1: padTo(sig1, 73), sig2: padTo(sig2, 73), script } };
				}
			}
			// 3-of-3: OP_3 ... OP_3 OP_CHECKMULTISIG, script=105
			if (
				items.length === 5 && script.length === 105 &&
				script[0] === 0x53 && script[script.length - 2] === 0x53
			) {
				const [sig1, sig2, sig3] = [items[1]!, items[2]!, items[3]!];
				if (
					sig1.length >= 71 && sig1.length <= 73 &&
					sig2.length >= 71 && sig2.length <= 73 &&
					sig3.length >= 71 && sig3.length <= 73
				) {
					return {
						kind: "6_p2wsh3of3",
						value: { sig1: padTo(sig1, 73), sig2: padTo(sig2, 73), sig3: padTo(sig3, 73), script },
					};
				}
			}
			// 1-of-2: OP_1 ... OP_2 OP_CHECKMULTISIG, script=71
			if (
				items.length === 3 && script.length === 71 &&
				script[0] === 0x51 && script[script.length - 2] === 0x52
			) {
				const sig = items[1]!;
				if (sig.length >= 71 && sig.length <= 73) {
					return { kind: "7_p2wsh1of2", value: { sig: padTo(sig, 73), script } };
				}
			}
			// 1-of-3: OP_1 ... OP_3 OP_CHECKMULTISIG, script=105
			if (
				items.length === 3 && script.length === 105 &&
				script[0] === 0x51 && script[script.length - 2] === 0x53
			) {
				const sig = items[1]!;
				if (sig.length >= 71 && sig.length <= 73) {
					return { kind: "8_p2wsh1of3", value: { sig: padTo(sig, 73), script } };
				}
			}
		}
	}

	// Timelock: [sig(71-73), script(39)] where script contains OP_CHECKLOCKTIMEVERIFY(0xb1) or OP_CHECKSEQUENCEVERIFY(0xb2)
	if (items.length === 2) {
		const [sig, script] = [items[0]!, items[1]!];
		if (
			sig.length >= 71 && sig.length <= 73 &&
			script.length === 39 &&
			(script.includes(0xb1) || script.includes(0xb2))
		) {
			return { kind: "9_p2wshTimelock", value: { sig: padTo(sig, 73), script } };
		}
	}

	// Raw fallback: store as standard Bitcoin witness wire format via ArrayCodec
	return { kind: "0_raw", value: items };
}

// ── Reconstruction ────────────────────────────────────────────────────────────

function reconstructWitness(stored: StoredWitnessUnionOutput): Uint8Array[] {
	switch (stored.kind) {
		case "0_raw":
			return stored.value;

		case "1_p2wpkh":
			return [
				trimTrailingZeros(stored.value.sig),
				stored.value.pubkey,
			];

		case "2_p2trKeyPath":
			return [trimTrailingZeros(stored.value.sig)];

		case "3_p2wsh1of1":
			return [
				new Uint8Array(0),
				trimTrailingZeros(stored.value.sig),
				stored.value.script,
			];

		case "4_p2wsh2of2":
			return [
				new Uint8Array(0),
				trimTrailingZeros(stored.value.sig1),
				trimTrailingZeros(stored.value.sig2),
				stored.value.script,
			];

		case "5_p2wsh2of3":
			return [
				new Uint8Array(0),
				trimTrailingZeros(stored.value.sig1),
				trimTrailingZeros(stored.value.sig2),
				stored.value.script,
			];

		case "6_p2wsh3of3":
			return [
				new Uint8Array(0),
				trimTrailingZeros(stored.value.sig1),
				trimTrailingZeros(stored.value.sig2),
				trimTrailingZeros(stored.value.sig3),
				stored.value.script,
			];

		case "7_p2wsh1of2":
			return [
				new Uint8Array(0),
				trimTrailingZeros(stored.value.sig),
				stored.value.script,
			];

		case "8_p2wsh1of3":
			return [
				new Uint8Array(0),
				trimTrailingZeros(stored.value.sig),
				stored.value.script,
			];

		case "9_p2wshTimelock":
			return [
				trimTrailingZeros(stored.value.sig),
				stored.value.script,
			];
	}
}

// ── Public codec ──────────────────────────────────────────────────────────────

export class StoredWitnessCodec extends Codec<Uint8Array[]> {
	readonly stride = -1;

	encode(items: Uint8Array[]): Uint8Array<ArrayBuffer> {
		return StoredWitnessUnion.encode(detectPattern(items));
	}

	decode(bytes: Uint8Array): [Uint8Array[], number] {
		const [stored, bytesRead] = StoredWitnessUnion.decode(bytes);
		return [reconstructWitness(stored), bytesRead];
	}
}

export const StoredWitness = new StoredWitnessCodec();
