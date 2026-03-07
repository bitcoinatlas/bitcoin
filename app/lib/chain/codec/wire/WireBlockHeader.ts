import { Codec, StructCodec, U32LE } from "@nomadshiba/codec";
import { Bytes32 } from "~/lib/codec/primitives.ts";
import { sha256 } from "@noble/hashes/sha2";

export type WireBlockHeader = Codec.Infer<typeof WireBlockHeader>;
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
