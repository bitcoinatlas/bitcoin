import { Codec, StructCodec, U32LE } from "@nomadshiba/codec";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { sha256d } from "~/libs/hashes/sha256d.ts";

export type WireBlockHeader = Codec.InferOutput<typeof WireBlockHeader>;
export const WireBlockHeader = new StructCodec({
	version: U32LE,
	prevHash: Bytes32,
	merkleRoot: Bytes32,
	timestamp: U32LE,
	bits: U32LE,
	nonce: U32LE,
}).transform((value, bytes) => {
	const transformed = value as typeof value & { hash(): Uint8Array };
	let hash: Uint8Array | undefined;
	transformed.hash = () => hash ??= sha256d(bytes);
	return transformed;
});
