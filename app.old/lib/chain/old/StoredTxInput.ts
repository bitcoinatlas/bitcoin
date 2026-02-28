import { dyn } from "~/traits.ts";
import type { BoundCodec } from "~/lib/codec/mod.ts";
import { asDyn } from "~/lib/codec/mod.ts";
import { U32 } from "~/lib/codec/primitives.ts";
import { Bytes } from "~/lib/codec/bytes.ts";
import { Bytes32, U24 } from "~/lib/codec/bitcoin.ts";
import { EnumCodec, type EnumValue, StructCodec } from "~/lib/codec/composites.ts";
import { StoredPointer } from "./StoredPointer.ts";
import { StoredWitness } from "./StoredWitness.ts";

// Dyn codecs for composition
const u32 = dyn(U32, U32.create());
const u24 = dyn(U24, U24.create());
const bytes32 = dyn(Bytes32, Bytes32.create());
const bytesVar = dyn(Bytes, Bytes.variable());
const storedPointerDyn = asDyn(StoredPointer);
const storedWitnessDyn = asDyn(StoredWitness);

// Nested struct codecs
const resolvedPrevOutCodec = StructCodec.create({ tx: storedPointerDyn, vout: u24 });
const unresolvedPrevOutCodec = StructCodec.create({ txId: bytes32, vout: u24 });

function structDyn<T extends Record<string, unknown>>(codec: ReturnType<typeof StructCodec.create<T>>) {
	return asDyn<T>({
		stride: codec.stride,
		encode: (v: T) => StructCodec.encode(codec, v),
		decode: (d: Uint8Array) => StructCodec.decode(codec, d),
	});
}

const resolvedCodec = StructCodec.create({
	prevOut: structDyn(resolvedPrevOutCodec),
	sequence: u32,
	scriptSig: bytesVar,
	witness: storedWitnessDyn,
});

const unresolvedCodec = StructCodec.create({
	prevOut: structDyn(unresolvedPrevOutCodec),
	sequence: u32,
	scriptSig: bytesVar,
	witness: storedWitnessDyn,
});

type StoredTxInputMap = {
	resolved: {
		prevOut: { tx: number; vout: number };
		sequence: number;
		scriptSig: Uint8Array;
		witness: Uint8Array[];
	};
	unresolved: {
		prevOut: { txId: Uint8Array; vout: number };
		sequence: number;
		scriptSig: Uint8Array;
		witness: Uint8Array[];
	};
};

export type StoredTxInput = EnumValue<StoredTxInputMap>;

const storedTxInputEnum = EnumCodec.create<StoredTxInputMap>({
	resolved: structDyn(resolvedCodec),
	unresolved: structDyn(unresolvedCodec),
});

export const StoredTxInput: BoundCodec<StoredTxInput> = {
	stride: storedTxInputEnum.stride,
	encode(value: StoredTxInput): Uint8Array {
		return EnumCodec.encode(storedTxInputEnum, value);
	},
	decode(data: Uint8Array): [StoredTxInput, number] {
		return EnumCodec.decode<StoredTxInputMap>(storedTxInputEnum, data);
	},
};
