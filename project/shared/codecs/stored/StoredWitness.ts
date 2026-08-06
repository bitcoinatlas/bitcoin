import { ArrayCodec, BytesCodec, Codec, EnumCodec, Stride, StructCodec, Void } from "@nomadshiba/codec";
import { CompactSize } from "~/primitives/CompactSize.ts";

const Sig73 = new BytesCodec({ size: 73 });
const Sig65 = new BytesCodec({ size: 65 });
const Pubkey = new BytesCodec({ size: 33 });
const Script34 = new BytesCodec({ size: 34 });
const Script71 = new BytesCodec({ size: 71 });
const Script105 = new BytesCodec({ size: 105 });
const Script39 = new BytesCodec({ size: 39 });

const RawWitnessItem = new BytesCodec({ sizer: CompactSize });
const RawWitness = new ArrayCodec(RawWitnessItem, { counter: CompactSize });

const P2WPKH = new StructCodec({ sig: Sig73, pubkey: Pubkey });
const P2TRKeyPath = new StructCodec({ sig: Sig65 });
const P2WSH1of1 = new StructCodec({ sig: Sig73, script: Script34 });
const P2WSH2of2 = new StructCodec({ sig1: Sig73, sig2: Sig73, script: Script71 });
const P2WSH2of3 = new StructCodec({ sig1: Sig73, sig2: Sig73, script: Script105 });
const P2WSH3of3 = new StructCodec({ sig1: Sig73, sig2: Sig73, sig3: Sig73, script: Script105 });
const P2WSH1of2 = new StructCodec({ sig: Sig73, script: Script71 });
const P2WSH1of3 = new StructCodec({ sig: Sig73, script: Script105 });
const P2WSHTimelock = new StructCodec({ sig: Sig73, script: Script39 });
const WitnessEnum = new EnumCodec({
	none: Void,
	raw: RawWitness,
	p2wpkh: P2WPKH,
	p2trKeyPath: P2TRKeyPath,
	p2wsh1of1: P2WSH1of1,
	p2wsh2of2: P2WSH2of2,
	p2wsh2of3: P2WSH2of3,
	p2wsh3of3: P2WSH3of3,
	p2wsh1of2: P2WSH1of2,
	p2wsh1of3: P2WSH1of3,
	p2wshTimelock: P2WSHTimelock,
});

// ── Padding helpers ───────────────────────────────────────────────────────────

function padTo(src: Uint8Array, size: number): Uint8Array {
	if (src.length === size) return src;
	const out = new Uint8Array(size);
	out.set(src);
	return out;
}

function trimTrailingZeros(src: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
	let len = src.length;
	while (len > 0 && src[len - 1] === 0) len--;
	return src.subarray(0, len);
}

export function detectWitnessPattern(items: Uint8Array[]): Codec.InferInput<typeof WitnessEnum> {
	if (items.length === 0) return { kind: "none", value: null };

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

function reconstructWitness(pattern: Codec.InferOutput<typeof WitnessEnum>): Uint8Array<ArrayBuffer>[] {
	switch (pattern.kind) {
		case "none":
			return [];

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

export type StoredWitness = Codec.InferOutput<typeof WitnessEnum> & { raw(): Uint8Array<ArrayBuffer>[] };
type WitnessInput = Codec.InferInput<typeof WitnessEnum> | Uint8Array[];

export class StoredWitnessCodec extends Codec<StoredWitness, WitnessInput> {
	public readonly stride: Stride<"variable"> = { kind: "variable" };

	public encoder(pattern: WitnessInput, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(pattern: WitnessInput, target: Uint8Array, offset: number): number;
	public encoder(pattern: WitnessInput, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
		if (Array.isArray(pattern)) pattern = detectWitnessPattern(pattern);
		if (target === undefined) return WitnessEnum.encode(pattern);
		return WitnessEnum.encodeInto(pattern, target, offset);
	}

	public decoder(bytes: Uint8Array, offset: number): [StoredWitness, number] {
		const [pattern, bytesRead] = WitnessEnum.decode(bytes, offset);
		const stored = pattern as StoredWitness;
		let rawCache: Uint8Array<ArrayBuffer>[] | undefined;
		stored.raw = () => rawCache ??= reconstructWitness(pattern);
		return [stored, bytesRead];
	}
}

export const StoredWitness = new StoredWitnessCodec();
