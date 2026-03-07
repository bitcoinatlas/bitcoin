import { Codec } from "@nomadshiba/codec";
import { compactSize } from "~/lib/codec/primitives.ts";

// Internal enum representation for pattern detection
type StoredWitnessEnum =
	| { kind: "raw"; value: Uint8Array }
	| { kind: "p2wpkh"; value: { sig: Uint8Array; pubkey: Uint8Array } }
	| { kind: "p2trKeyPath"; value: { sig: Uint8Array } }
	| { kind: "p2wsh1of1"; value: { sig: Uint8Array; script: Uint8Array } }
	| { kind: "p2wsh2of2"; value: { sig1: Uint8Array; sig2: Uint8Array; script: Uint8Array } }
	| { kind: "p2wsh2of3"; value: { sig1: Uint8Array; sig2: Uint8Array; script: Uint8Array } }
	| { kind: "p2wsh3of3"; value: { sig1: Uint8Array; sig2: Uint8Array; sig3: Uint8Array; script: Uint8Array } }
	| { kind: "p2wsh1of2"; value: { sig: Uint8Array; script: Uint8Array } }
	| { kind: "p2wsh1of3"; value: { sig: Uint8Array; script: Uint8Array } }
	| { kind: "p2wshTimelock"; value: { sig: Uint8Array; script: Uint8Array } };

export class StoredWitnessCodec extends Codec<Uint8Array[]> {
	readonly stride = -1;

	encode(data: Uint8Array[]): Uint8Array {
		const enumValue = detectPattern(data);
		return encodeStoredWitnessEnum(enumValue);
	}

	decode(bytes: Uint8Array): [Uint8Array[], number] {
		const [enumValue, bytesRead] = decodeStoredWitnessEnum(bytes);
		return [reconstructWitness(enumValue), bytesRead];
	}
}

export const storedWitness = new StoredWitnessCodec();

// Pattern detection functions (extracted for clarity)
function detectPattern(items: Uint8Array[]): StoredWitnessEnum {
	if (items.length === 0) {
		return { kind: "raw", value: new Uint8Array(0) };
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
	const chunks: Uint8Array[] = [compactSize.encode(items.length)];
	for (const item of items) {
		chunks.push(compactSize.encode(item.length));
		chunks.push(item);
	}
	const totalLength = chunks.reduce((sum, c) => sum + c.length, 0);
	const out = new Uint8Array(totalLength);

	let offset = 0;
	for (const chunk of chunks) {
		out.set(chunk, offset);
		offset += chunk.length;
	}
	return { kind: "raw", value: out };
}

function encodeStoredWitnessEnum(enumValue: StoredWitnessEnum): Uint8Array {
	// Tag byte + payload
	const kindId = getKindId(enumValue.kind);
	const payload = encodePayload(enumValue);
	const result = new Uint8Array(1 + payload.length);
	result[0] = kindId;
	result.set(payload, 1);
	return result;
}

function decodeStoredWitnessEnum(bytes: Uint8Array): [StoredWitnessEnum, number] {
	const kindId = bytes[0]!;
	const [kind, payloadSize] = decodeKindPayload(bytes.subarray(1), kindId);
	return [kind, 1 + payloadSize];
}

function getKindId(kind: StoredWitnessEnum["kind"]): number {
	const ids: Record<string, number> = {
		raw: 0,
		p2wpkh: 1,
		p2trKeyPath: 2,
		p2wsh1of1: 3,
		p2wsh2of2: 4,
		p2wsh2of3: 5,
		p2wsh3of3: 6,
		p2wsh1of2: 7,
		p2wsh1of3: 8,
		p2wshTimelock: 9,
	};
	return ids[kind] ?? 0;
}

function encodePayload(enumValue: StoredWitnessEnum): Uint8Array {
	switch (enumValue.kind) {
		case "raw":
			return enumValue.value.slice();
		case "p2wpkh": {
			const { sig, pubkey } = enumValue.value;
			const out = new Uint8Array(73 + 33);
			out.set(sig, 0);
			out.set(pubkey, 73);
			return out;
		}
		case "p2trKeyPath": {
			const { sig } = enumValue.value;
			return sig.slice();
		}
		case "p2wsh1of1": {
			const { sig, script } = enumValue.value;
			const out = new Uint8Array(73 + 34);
			out.set(sig, 0);
			out.set(script, 73);
			return out;
		}
		case "p2wsh2of2": {
			const { sig1, sig2, script } = enumValue.value;
			const out = new Uint8Array(73 + 73 + 71);
			out.set(sig1, 0);
			out.set(sig2, 73);
			out.set(script, 146);
			return out;
		}
		case "p2wsh2of3": {
			const { sig1, sig2, script } = enumValue.value;
			const out = new Uint8Array(73 + 73 + 105);
			out.set(sig1, 0);
			out.set(sig2, 73);
			out.set(script, 146);
			return out;
		}
		case "p2wsh3of3": {
			const { sig1, sig2, sig3, script } = enumValue.value;
			const out = new Uint8Array(73 + 73 + 73 + 105);
			out.set(sig1, 0);
			out.set(sig2, 73);
			out.set(sig3, 146);
			out.set(script, 219);
			return out;
		}
		case "p2wsh1of2": {
			const { sig, script } = enumValue.value;
			const out = new Uint8Array(73 + 71);
			out.set(sig, 0);
			out.set(script, 73);
			return out;
		}
		case "p2wsh1of3": {
			const { sig, script } = enumValue.value;
			const out = new Uint8Array(73 + 105);
			out.set(sig, 0);
			out.set(script, 73);
			return out;
		}
		case "p2wshTimelock": {
			const { sig, script } = enumValue.value;
			const out = new Uint8Array(73 + 39);
			out.set(sig, 0);
			out.set(script, 73);
			return out;
		}
	}
}

function decodeKindPayload(data: Uint8Array, kindId: number): [StoredWitnessEnum, number] {
	switch (kindId) {
		case 0: // raw
			return [{ kind: "raw", value: data.slice() }, data.length];
		case 1: { // p2wpkh
			const sig = data.subarray(0, 73);
			const pubkey = data.subarray(73, 106);
			return [{ kind: "p2wpkh", value: { sig, pubkey } }, 106];
		}
		case 2: { // p2trKeyPath
			const sig = data.subarray(0, 65);
			return [{ kind: "p2trKeyPath", value: { sig } }, 65];
		}
		case 3: { // p2wsh1of1
			const sig = data.subarray(0, 73);
			const script = data.subarray(73, 107);
			return [{ kind: "p2wsh1of1", value: { sig, script } }, 107];
		}
		case 4: { // p2wsh2of2
			const sig1 = data.subarray(0, 73);
			const sig2 = data.subarray(73, 146);
			const script = data.subarray(146, 217);
			return [{ kind: "p2wsh2of2", value: { sig1, sig2, script } }, 217];
		}
		case 5: { // p2wsh2of3
			const sig1 = data.subarray(0, 73);
			const sig2 = data.subarray(73, 146);
			const script = data.subarray(146, 251);
			return [{ kind: "p2wsh2of3", value: { sig1, sig2, script } }, 251];
		}
		case 6: { // p2wsh3of3
			const sig1 = data.subarray(0, 73);
			const sig2 = data.subarray(73, 146);
			const sig3 = data.subarray(146, 219);
			const script = data.subarray(219, 324);
			return [{ kind: "p2wsh3of3", value: { sig1, sig2, sig3, script } }, 324];
		}
		case 7: { // p2wsh1of2
			const sig = data.subarray(0, 73);
			const script = data.subarray(73, 144);
			return [{ kind: "p2wsh1of2", value: { sig, script } }, 144];
		}
		case 8: { // p2wsh1of3
			const sig = data.subarray(0, 73);
			const script = data.subarray(73, 178);
			return [{ kind: "p2wsh1of3", value: { sig, script } }, 178];
		}
		case 9: { // p2wshTimelock
			const sig = data.subarray(0, 73);
			const script = data.subarray(73, 112);
			return [{ kind: "p2wshTimelock", value: { sig, script } }, 112];
		}
		default: // raw fallback
			return [{ kind: "raw", value: data.slice() }, data.length];
	}
}

function reconstructWitness(stored: StoredWitnessEnum): Uint8Array[] {
	const items: Uint8Array[] = [];

	switch (stored.kind) {
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
			const [count] = compactSize.decode(data.subarray(offset));
			offset += compactSize.encode(count).length;
			for (let i = 0; i < count; i++) {
				const [len] = compactSize.decode(data.subarray(offset));
				offset += compactSize.encode(len).length;
				items.push(data.subarray(offset, offset + len));
				offset += len;
			}
			break;
		}
	}

	return items;
}
