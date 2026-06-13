import { sha256 } from "@noble/hashes/sha2";
import { Codec, StructCodec, U32LE } from "@nomadshiba/codec";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";

export type WireBlockHeader = Codec.InferOutput<typeof WireBlockHeader>;
export const WireBlockHeader = new StructCodec({
	version: U32LE,
	prevHash: Bytes32,
	merkleRoot: Bytes32,
	timestamp: U32LE,
	bits: U32LE,
	nonce: U32LE,
}).transform((value, bytes) => {
	return {
		hash: sha256(sha256(bytes)),
		...value,
	};
});
