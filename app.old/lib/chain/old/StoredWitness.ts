import { Bytes } from "~/lib/codec/bytes.ts";
import { EnumCodec, StructCodec } from "~/lib/codec/composites.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CompactSize } from "~/lib/CompactSize.ts";
import type { Impl } from "~/traits.ts";
import { dyn } from "~/traits.ts";
import { Stride } from "../../codec/mod.ts";

const sig73 = dyn(Bytes, Bytes.fixed(73));
const pubkey33 = dyn(Bytes, Bytes.fixed(33));
const schnorr65 = dyn(Bytes, Bytes.fixed(65));
const script34 = dyn(Bytes, Bytes.fixed(34));
const script71 = dyn(Bytes, Bytes.fixed(71));
const script105 = dyn(Bytes, Bytes.fixed(105));
const timelock = dyn(Bytes, Bytes.fixed(39));

const storedWitnessInternalCodec = EnumCodec.create({
	empty: dyn(StructCodec, StructCodec.create({})),
	p2wpkh: dyn(StructCodec, StructCodec.create({ sig: sig73, pubkey: pubkey33 })),
	p2trKeyPath: dyn(StructCodec, StructCodec.create({ sig: schnorr65 })),
	p2wsh1of1: dyn(StructCodec, StructCodec.create({ sig: sig73, script: script34 })),
	p2wsh2of2: dyn(StructCodec, StructCodec.create({ sig1: sig73, sig2: sig73, script: script71 })),
	p2wsh2of3: dyn(StructCodec, StructCodec.create({ sig1: sig73, sig2: sig73, script: script105 })),
	p2wsh3of3: dyn(StructCodec, StructCodec.create({ sig1: sig73, sig2: sig73, sig3: sig73, script: script105 })),
	p2wsh1of2: dyn(StructCodec, StructCodec.create({ sig: sig73, script: script71 })),
	p2wsh1of3: dyn(StructCodec, StructCodec.create({ sig: sig73, script: script105 })),
	p2wshTimelock: dyn(StructCodec, StructCodec.create({ sig: sig73, script: timelock })),
	raw: dyn(Bytes, Bytes.variable()),
});

type StoredWitnessCodec = { stride: Stride };

const StoredWitnessCodec = {
	create(): StoredWitnessCodec {
		return { stride: Stride.variable() };
	},
	encode(_self, data: Uint8Array[]) {
		const enumValue = detectPattern(data);
		return EnumCodec.encode(storedWitnessInternalCodec, enumValue);
	},
	decode(_self, bytes: Uint8Array) {
		const [enumValue, bytesRead] = EnumCodec.decode(storedWitnessInternalCodec, bytes);
		return [reconstructWitness(enumValue), bytesRead];
	},
} satisfies Impl<StoredWitnessCodec, Codec<StoredWitnessCodec, Uint8Array[]>>;

function detectPattern(items: Uint8Array[]): StoredWitnessEnum {
	if (items.length === 0) {
		return { kind: "empty", value: {} };
	}

	// P2WPKH
	if (items.length === 2) {
		const sig = items[0]!;
		const pubkey = items[1]!;
		if (
			sig.length >= 71 && sig.length <= 73 &&
			pubkey.length === 33 &&
			(pubkey[0] === 0x02 || pubkey[0] === 0x03)
		) {
			const paddedSig = new Uint8Array(73);
			paddedSig.set(sig);
			return { kind: "p2wpkh", value: { sig: paddedSig, pubkey } };
		}
	}

	// P2TR key path
	if (items.length === 1) {
		const sig = items[0]!;
		if (sig.length === 64 || sig.length === 65) {
			const paddedSig = new Uint8Array(65);
			paddedSig.set(sig);
			return { kind: "p2trKeyPath", value: { sig: paddedSig } };
		}
	}

	// P2WSH patterns (start with OP_0)
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
					const paddedSig = new Uint8Array(73);
					paddedSig.set(sig);
					return { kind: "p2wsh1of1", value: { sig: paddedSig, script } };
				}
			}
			// 2-of-2
			if (
				items.length === 4 && script.length === 71 &&
				script[0] === 0x52 && script[script.length - 2] === 0x52
			) {
				const sig1 = items[1]!, sig2 = items[2]!;
				if (sig1.length >= 71 && sig1.length <= 73 && sig2.length >= 71 && sig2.length <= 73) {
					const s1 = new Uint8Array(73), s2 = new Uint8Array(73);
					s1.set(sig1);
					s2.set(sig2);
					return { kind: "p2wsh2of2", value: { sig1: s1, sig2: s2, script } };
				}
			}
			// 2-of-3
			if (
				items.length === 4 && script.length === 105 &&
				script[0] === 0x52 && script[script.length - 2] === 0x53
			) {
				const sig1 = items[1]!, sig2 = items[2]!;
				if (sig1.length >= 71 && sig1.length <= 73 && sig2.length >= 71 && sig2.length <= 73) {
					const s1 = new Uint8Array(73), s2 = new Uint8Array(73);
					s1.set(sig1);
					s2.set(sig2);
					return { kind: "p2wsh2of3", value: { sig1: s1, sig2: s2, script } };
				}
			}
			// 3-of-3
			if (
				items.length === 5 && script.length === 105 &&
				script[0] === 0x53 && script[script.length - 2] === 0x53
			) {
				const sig1 = items[1]!, sig2 = items[2]!, sig3 = items[3]!;
				if (
					sig1.length >= 71 && sig1.length <= 73 &&
					sig2.length >= 71 && sig2.length <= 73 &&
					sig3.length >= 71 && sig3.length <= 73
				) {
					const s1 = new Uint8Array(73), s2 = new Uint8Array(73), s3 = new Uint8Array(73);
					s1.set(sig1);
					s2.set(sig2);
					s3.set(sig3);
					return { kind: "p2wsh3of3", value: { sig1: s1, sig2: s2, sig3: s3, script } };
				}
			}
			// 1-of-2
			if (
				items.length === 3 && script.length === 71 &&
				script[0] === 0x51 && script[script.length - 2] === 0x52
			) {
				const sig = items[1]!;
				if (sig.length >= 71 && sig.length <= 73) {
					const paddedSig = new Uint8Array(73);
					paddedSig.set(sig);
					return { kind: "p2wsh1of2", value: { sig: paddedSig, script } };
				}
			}
			// 1-of-3
			if (
				items.length === 3 && script.length === 105 &&
				script[0] === 0x51 && script[script.length - 2] === 0x53
			) {
				const sig = items[1]!;
				if (sig.length >= 71 && sig.length <= 73) {
					const paddedSig = new Uint8Array(73);
					paddedSig.set(sig);
					return { kind: "p2wsh1of3", value: { sig: paddedSig, script } };
				}
			}
		}
	}

	// Timelock
	if (items.length === 2) {
		const sig = items[0]!, script = items[1]!;
		if (
			sig.length >= 71 && sig.length <= 73 &&
			script.length === 39 &&
			(script.includes(0xb1) || script.includes(0xb2))
		) {
			const paddedSig = new Uint8Array(73);
			paddedSig.set(sig);
			return { kind: "p2wshTimelock", value: { sig: paddedSig, script } };
		}
	}

	// Raw fallback - encode items to wire format
	const chunks: Uint8Array[] = [CompactSize.encode(items.length)];
	for (const item of items) {
		chunks.push(CompactSize.encode(item.length));
		chunks.push(item);
	}
	const totalLength = chunks.reduce((sum, c) => sum + c.length, 0);
	const out = new Uint8Array(totalLength);

	{
		let offset = 0;
		for (const chunk of chunks) {
			out.set(chunk, offset);
			offset += chunk.length;
		}
		return { kind: "raw", value: out };
	}
}

function reconstructWitness(stored: StoredWitnessEnum): Uint8Array[] {
	const items: Uint8Array[] = [];

	switch (stored.kind) {
		case "empty":
			break;

		case "p2wpkh": {
			const sig = stored.value.sig;
			let sigLen = 73;
			while (sigLen > 0 && sig[sigLen - 1] === 0) sigLen--;
			items.push(sig.subarray(0, sigLen), stored.value.pubkey);
			break;
		}

		case "p2trKeyPath": {
			const sig = stored.value.sig;
			let sigLen = 65;
			while (sigLen > 0 && sig[sigLen - 1] === 0) sigLen--;
			items.push(sig.subarray(0, sigLen));
			break;
		}

		case "p2wsh1of1": {
			const sig = stored.value.sig;
			let sigLen = 73;
			while (sigLen > 0 && sig[sigLen - 1] === 0) sigLen--;
			items.push(new Uint8Array(0), sig.subarray(0, sigLen), stored.value.script);
			break;
		}

		case "p2wsh2of2": {
			const sig1 = stored.value.sig1, sig2 = stored.value.sig2;
			let s1Len = 73, s2Len = 73;
			while (s1Len > 0 && sig1[s1Len - 1] === 0) s1Len--;
			while (s2Len > 0 && sig2[s2Len - 1] === 0) s2Len--;
			items.push(
				new Uint8Array(0),
				sig1.subarray(0, s1Len),
				sig2.subarray(0, s2Len),
				stored.value.script,
			);
			break;
		}

		case "p2wsh2of3": {
			const sig1 = stored.value.sig1, sig2 = stored.value.sig2;
			let s1Len = 73, s2Len = 73;
			while (s1Len > 0 && sig1[s1Len - 1] === 0) s1Len--;
			while (s2Len > 0 && sig2[s2Len - 1] === 0) s2Len--;
			items.push(
				new Uint8Array(0),
				sig1.subarray(0, s1Len),
				sig2.subarray(0, s2Len),
				stored.value.script,
			);
			break;
		}

		case "p2wsh3of3": {
			const sig1 = stored.value.sig1, sig2 = stored.value.sig2, sig3 = stored.value.sig3;
			let s1Len = 73, s2Len = 73, s3Len = 73;
			while (s1Len > 0 && sig1[s1Len - 1] === 0) s1Len--;
			while (s2Len > 0 && sig2[s2Len - 1] === 0) s2Len--;
			while (s3Len > 0 && sig3[s3Len - 1] === 0) s3Len--;
			items.push(
				new Uint8Array(0),
				sig1.subarray(0, s1Len),
				sig2.subarray(0, s2Len),
				sig3.subarray(0, s3Len),
				stored.value.script,
			);
			break;
		}

		case "p2wsh1of2": {
			const sig = stored.value.sig;
			let sigLen = 73;
			while (sigLen > 0 && sig[sigLen - 1] === 0) sigLen--;
			items.push(new Uint8Array(0), sig.subarray(0, sigLen), stored.value.script);
			break;
		}

		case "p2wsh1of3": {
			const sig = stored.value.sig;
			let sigLen = 73;
			while (sigLen > 0 && sig[sigLen - 1] === 0) sigLen--;
			items.push(new Uint8Array(0), sig.subarray(0, sigLen), stored.value.script);
			break;
		}

		case "p2wshTimelock": {
			const sig = stored.value.sig;
			let sigLen = 73;
			while (sigLen > 0 && sig[sigLen - 1] === 0) sigLen--;
			items.push(sig.subarray(0, sigLen), stored.value.script);
			break;
		}

		case "raw": {
			// Decode raw witness bytes back to items
			const data = stored.value;
			let offset = 0;
			const [count, countOff] = CompactSize.decode(data, offset);
			offset = countOff;
			for (let i = 0; i < count; i++) {
				const [len, lenOff] = CompactSize.decode(data, offset);
				offset = lenOff;
				items.push(data.subarray(offset, offset + len));
				offset += len;
			}
			break;
		}
	}

	return items;
}
