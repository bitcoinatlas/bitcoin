import { BytesCodec, Codec, StructCodec, u32LE } from "@nomadshiba/codec";
import { bytes32, u24LE } from "~/lib/codec/primitives.ts";
import { storedPointer } from "~/lib/codec/StoredPointer.ts";
import { storedWitness } from "~/lib/codec/StoredWitness.ts";

// Resolved prevOut points to a stored pointer
export type ResolvedPrevOut = {
	tx: number;
	vout: number;
};

// Unresolved prevOut uses txId directly
export type UnresolvedPrevOut = {
	txId: Uint8Array;
	vout: number;
};

export type StoredTxInput =
	| {
		kind: "resolved";
		value: {
			prevOut: ResolvedPrevOut;
			sequence: number;
			scriptSig: Uint8Array;
			witness: Uint8Array[];
		};
	}
	| {
		kind: "unresolved";
		value: {
			prevOut: UnresolvedPrevOut;
			sequence: number;
			scriptSig: Uint8Array;
			witness: Uint8Array[];
		};
	};

// Use BytesCodec for scriptSig length prefix
const scriptSigCodec = new BytesCodec();

// Nested struct codecs for prevOut
const resolvedPrevOutCodec = new StructCodec({
	tx: storedPointer,
	vout: u24LE,
});

const unresolvedPrevOutCodec = new StructCodec({
	txId: bytes32,
	vout: u24LE,
});

// Enum for prevOut type
type PrevOutEnum =
	| { kind: "resolved"; value: ResolvedPrevOut }
	| { kind: "unresolved"; value: UnresolvedPrevOut };

class PrevOutCodec extends Codec<PrevOutEnum> {
	readonly stride = -1;

	encode(value: PrevOutEnum): Uint8Array {
		if (value.kind === "resolved") {
			const encoded = resolvedPrevOutCodec.encode(value.value);
			const result = new Uint8Array(1 + encoded.length);
			result[0] = 0;
			result.set(encoded, 1);
			return result;
		} else {
			const encoded = unresolvedPrevOutCodec.encode(value.value);
			const result = new Uint8Array(1 + encoded.length);
			result[0] = 1;
			result.set(encoded, 1);
			return result;
		}
	}

	decode(data: Uint8Array): [PrevOutEnum, number] {
		const kind = data[0]!;
		if (kind === 0) {
			const [value] = resolvedPrevOutCodec.decode(data.subarray(1));
			return [{ kind: "resolved", value }, 1 + 6 + 3];
		} else {
			const [value] = unresolvedPrevOutCodec.decode(data.subarray(1));
			return [{ kind: "unresolved", value }, 1 + 32 + 3];
		}
	}
}

const prevOutCodec = new PrevOutCodec();

// Now build the full StoredTxInput codec
// StoredTxInput = prevOut + sequence + scriptSig + witness
// witness is stored as-is (it's already encoded via storedWitness)

export class StoredTxInputCodec extends Codec<StoredTxInput> {
	readonly stride = -1;

	encode(value: StoredTxInput): Uint8Array {
		// Build prevOut enum
		const prevOutEnum: PrevOutEnum = {
			kind: value.kind,
			value: value.value.prevOut,
		} as PrevOutEnum;

		const prevOutEncoded = prevOutCodec.encode(prevOutEnum);
		const sequenceEncoded = u32LE.encode(value.value.sequence);
		const scriptSigEncoded = scriptSigCodec.encode(value.value.scriptSig);
		const witnessEncoded = storedWitness.encode(value.value.witness);

		const totalLength = prevOutEncoded.length + sequenceEncoded.length + scriptSigEncoded.length + witnessEncoded.length;
		const result = new Uint8Array(totalLength);
		let offset = 0;

		result.set(prevOutEncoded, offset);
		offset += prevOutEncoded.length;
		result.set(sequenceEncoded, offset);
		offset += sequenceEncoded.length;
		result.set(scriptSigEncoded, offset);
		offset += scriptSigEncoded.length;
		result.set(witnessEncoded, offset);

		return result;
	}

	decode(data: Uint8Array): [StoredTxInput, number] {
		let offset = 0;

		// Decode prevOut (first byte is tag)
		const [prevOutEnum, prevOutBytes] = prevOutCodec.decode(data.subarray(offset));
		offset += prevOutBytes;

		const [sequence] = u32LE.decode(data.subarray(offset));
		offset += 4;

		const [scriptSig, scriptSigBytes] = scriptSigCodec.decode(data.subarray(offset));
		offset += scriptSigBytes;

		const [witness, witnessBytes] = storedWitness.decode(data.subarray(offset));
		offset += witnessBytes;

		const result: StoredTxInput = {
			kind: prevOutEnum.kind as "resolved" | "unresolved",
			value: {
				prevOut: prevOutEnum.value,
				sequence,
				scriptSig,
				witness,
			},
		} as StoredTxInput;

		return [result, offset];
	}
}

export const storedTxInput = new StoredTxInputCodec();
