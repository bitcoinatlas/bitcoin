import { ArrayCodec, BytesCodec, Codec, EnumCodec, type EnumOutput, Stride, StructCodec } from "@nomadshiba/codec";
import { CompactSize } from "~/codec/primitives/CompactSize.ts";

// ── Fixed-size byte array codecs ──────────────────────────────────────────────

const Sig73 = new BytesCodec({ size: 73 });
const Sig65 = new BytesCodec({ size: 65 });
const Pubkey = new BytesCodec({ size: 33 });
const Script34 = new BytesCodec({ size: 34 });
const Script71 = new BytesCodec({ size: 71 });
const Script105 = new BytesCodec({ size: 105 });
const Script39 = new BytesCodec({ size: 39 });

// ── Raw witness fallback ──────────────────────────────────────────────────────

const RawWitnessItemCodec = new BytesCodec({ sizer: CompactSize });
const RawWitnessCodec = new ArrayCodec(RawWitnessItemCodec, { counter: CompactSize });

// ── Struct codecs for each recognized pattern ─────────────────────────────────

const P2WPKHCodec = new StructCodec({ sig: Sig73, pubkey: Pubkey });
const P2TRKeyPathCodec = new StructCodec({ sig: Sig65 });
const P2WSH1of1Codec = new StructCodec({ sig: Sig73, script: Script34 });
const P2WSH2of2Codec = new StructCodec({ sig1: Sig73, sig2: Sig73, script: Script71 });
const P2WSH2of3Codec = new StructCodec({ sig1: Sig73, sig2: Sig73, script: Script105 });
const P2WSH3of3Codec = new StructCodec({ sig1: Sig73, sig2: Sig73, sig3: Sig73, script: Script105 });
const P2WSH1of2Codec = new StructCodec({ sig: Sig73, script: Script71 });
const P2WSH1of3Codec = new StructCodec({ sig: Sig73, script: Script105 });
const P2WSHTimelockCodec = new StructCodec({ sig: Sig73, script: Script39 });

// ── Clean witness pattern types ───────────────────────────────────────────────

export type WitnessPattern =
	| { kind: "raw"; value: Uint8Array[] }
	| { kind: "p2wpkh"; value: { sig: Uint8Array; pubkey: Uint8Array } }
	| { kind: "p2trKeyPath"; value: { sig: Uint8Array } }
	| { kind: "p2wsh1of1"; value: { sig: Uint8Array; script: Uint8Array } }
	| { kind: "p2wsh2of2"; value: { sig1: Uint8Array; sig2: Uint8Array; script: Uint8Array } }
	| { kind: "p2wsh2of3"; value: { sig1: Uint8Array; sig2: Uint8Array; script: Uint8Array } }
	| { kind: "p2wsh3of3"; value: { sig1: Uint8Array; sig2: Uint8Array; sig3: Uint8Array; script: Uint8Array } }
	| { kind: "p2wsh1of2"; value: { sig: Uint8Array; script: Uint8Array } }
	| { kind: "p2wsh1of3"; value: { sig: Uint8Array; script: Uint8Array } }
	| { kind: "p2wshTimelock"; value: { sig: Uint8Array; script: Uint8Array } };

// ── Internal union (numbered keys for deterministic wire-format indices) ──────

const StoredWitnessUnion = new EnumCodec({
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

type UnionWitness = typeof StoredWitnessUnion extends EnumCodec<infer T> ? EnumOutput<T> : never;

const KIND_MAP: Record<UnionWitness["kind"], WitnessPattern["kind"]> = {
	"0_raw": "raw",
	"1_p2wpkh": "p2wpkh",
	"2_p2trKeyPath": "p2trKeyPath",
	"3_p2wsh1of1": "p2wsh1of1",
	"4_p2wsh2of2": "p2wsh2of2",
	"5_p2wsh2of3": "p2wsh2of3",
	"6_p2wsh3of3": "p2wsh3of3",
	"7_p2wsh1of2": "p2wsh1of2",
	"8_p2wsh1of3": "p2wsh1of3",
	"9_p2wshTimelock": "p2wshTimelock",
};

const REV_KIND_MAP: Record<WitnessPattern["kind"], UnionWitness["kind"]> = Object.fromEntries(
	Object.entries(KIND_MAP).map(([k, v]) => [v, k]),
) as Record<WitnessPattern["kind"], UnionWitness["kind"]>;

function toUnion(pattern: WitnessPattern): UnionWitness {
	return { kind: REV_KIND_MAP[pattern.kind], value: pattern.value } as UnionWitness;
}

function fromUnion(u: UnionWitness): WitnessPattern {
	return { kind: KIND_MAP[u.kind], value: u.value } as WitnessPattern;
}

// ── Padding helpers ───────────────────────────────────────────────────────────

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

export function detectWitnessPattern(items: Uint8Array[]): WitnessPattern {
	// P2WPKH: [sig(71-73), pubkey(33)]
	if (items.length === 2) {
		const [sig, pubkey] = [items[0]!, items[1]!];
		if (
			sig.length >= 71 && sig.length <= 73 &&
			pubkey.length === 33 &&
			(pubkey[0] === 0x02 || pubkey[0] === 0x03)
		) {
			return { kind: "p2wpkh", value: { sig: padTo(sig, 73), pubkey } };
		}
	}

	// P2TR key path: [sig(64|65)]
	if (items.length === 1) {
		const sig = items[0]!;
		if (sig.length === 64 || sig.length === 65) {
			return { kind: "p2trKeyPath", value: { sig: padTo(sig, 65) } };
		}
	}

	// P2WSH patterns: [OP_0(empty), ...sigs, script] where script ends with OP_CHECKMULTISIG (0xae)
	if (items.length >= 2 && items[0]!.length === 0) {
		const script = items[items.length - 1]!;

		if (script.length >= 34 && script[script.length - 1] === 0xae) {
			// 1-of-1
			if (
				items.length === 3 && script.length === 34 &&
				script[0] === 0x51 && script[script.length - 2] === 0x51
			) {
				const sig = items[1]!;
				if (sig.length >= 71 && sig.length <= 73) {
					return { kind: "p2wsh1of1", value: { sig: padTo(sig, 73), script } };
				}
			}
			// 2-of-2
			if (
				items.length === 4 && script.length === 71 &&
				script[0] === 0x52 && script[script.length - 2] === 0x52
			) {
				const [sig1, sig2] = [items[1]!, items[2]!];
				if (sig1.length >= 71 && sig1.length <= 73 && sig2.length >= 71 && sig2.length <= 73) {
					return { kind: "p2wsh2of2", value: { sig1: padTo(sig1, 73), sig2: padTo(sig2, 73), script } };
				}
			}
			// 2-of-3
			if (
				items.length === 4 && script.length === 105 &&
				script[0] === 0x52 && script[script.length - 2] === 0x53
			) {
				const [sig1, sig2] = [items[1]!, items[2]!];
				if (sig1.length >= 71 && sig1.length <= 73 && sig2.length >= 71 && sig2.length <= 73) {
					return { kind: "p2wsh2of3", value: { sig1: padTo(sig1, 73), sig2: padTo(sig2, 73), script } };
				}
			}
			// 3-of-3
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
						kind: "p2wsh3of3",
						value: { sig1: padTo(sig1, 73), sig2: padTo(sig2, 73), sig3: padTo(sig3, 73), script },
					};
				}
			}
			// 1-of-2
			if (
				items.length === 3 && script.length === 71 &&
				script[0] === 0x51 && script[script.length - 2] === 0x52
			) {
				const sig = items[1]!;
				if (sig.length >= 71 && sig.length <= 73) {
					return { kind: "p2wsh1of2", value: { sig: padTo(sig, 73), script } };
				}
			}
			// 1-of-3
			if (
				items.length === 3 && script.length === 105 &&
				script[0] === 0x51 && script[script.length - 2] === 0x53
			) {
				const sig = items[1]!;
				if (sig.length >= 71 && sig.length <= 73) {
					return { kind: "p2wsh1of3", value: { sig: padTo(sig, 73), script } };
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
			return { kind: "p2wshTimelock", value: { sig: padTo(sig, 73), script } };
		}
	}

	return { kind: "raw", value: items };
}

// ── Reconstruction ────────────────────────────────────────────────────────────

export function reconstructWitness(pattern: WitnessPattern): Uint8Array[] {
	switch (pattern.kind) {
		case "raw":
			return pattern.value;

		case "p2wpkh":
			return [
				trimTrailingZeros(pattern.value.sig),
				pattern.value.pubkey,
			];

		case "p2trKeyPath":
			return [trimTrailingZeros(pattern.value.sig)];

		case "p2wsh1of1":
			return [
				new Uint8Array(0),
				trimTrailingZeros(pattern.value.sig),
				pattern.value.script,
			];

		case "p2wsh2of2":
			return [
				new Uint8Array(0),
				trimTrailingZeros(pattern.value.sig1),
				trimTrailingZeros(pattern.value.sig2),
				pattern.value.script,
			];

		case "p2wsh2of3":
			return [
				new Uint8Array(0),
				trimTrailingZeros(pattern.value.sig1),
				trimTrailingZeros(pattern.value.sig2),
				pattern.value.script,
			];

		case "p2wsh3of3":
			return [
				new Uint8Array(0),
				trimTrailingZeros(pattern.value.sig1),
				trimTrailingZeros(pattern.value.sig2),
				trimTrailingZeros(pattern.value.sig3),
				pattern.value.script,
			];

		case "p2wsh1of2":
			return [
				new Uint8Array(0),
				trimTrailingZeros(pattern.value.sig),
				pattern.value.script,
			];

		case "p2wsh1of3":
			return [
				new Uint8Array(0),
				trimTrailingZeros(pattern.value.sig),
				pattern.value.script,
			];

		case "p2wshTimelock":
			return [
				trimTrailingZeros(pattern.value.sig),
				pattern.value.script,
			];
	}
}

// ── Codecs ────────────────────────────────────────────────────────────────────

export class StoredWitnessPatternCodec extends Codec<WitnessPattern> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	encode(pattern: WitnessPattern): Uint8Array<ArrayBuffer> {
		return StoredWitnessUnion.encode(toUnion(pattern));
	}

	public override encodeInto(pattern: WitnessPattern, target: Uint8Array, offset: number = 0): number {
		return StoredWitnessUnion.encodeInto(toUnion(pattern), target, offset);
	}

	override size(pattern: WitnessPattern): number {
		return StoredWitnessUnion.size(toUnion(pattern));
	}

	decode(bytes: Uint8Array): [WitnessPattern, number] {
		const [stored, bytesRead] = StoredWitnessUnion.decode(bytes);
		return [fromUnion(stored), bytesRead];
	}
}

export const StoredWitnessPattern = new StoredWitnessPatternCodec();

export class StoredWitnessCodec extends Codec<Uint8Array[]> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	encode(items: Uint8Array[]): Uint8Array<ArrayBuffer> {
		return StoredWitnessPattern.encode(detectWitnessPattern(items));
	}

	public override encodeInto(items: Uint8Array[], target: Uint8Array, offset: number = 0): number {
		return StoredWitnessPattern.encodeInto(detectWitnessPattern(items), target, offset);
	}

	public override size(items: Uint8Array[]): number {
		return StoredWitnessPattern.size(detectWitnessPattern(items));
	}

	public decode(bytes: Uint8Array): [Uint8Array[], number] {
		const [pattern, bytesRead] = StoredWitnessPattern.decode(bytes);
		return [reconstructWitness(pattern), bytesRead];
	}
}

export const StoredWitness = new StoredWitnessCodec();
