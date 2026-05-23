import { Codec, StructCodec, U32LE } from "@nomadshiba/codec";
import { Bytes32 } from "~/lib/codec/primitives.ts";
import { sha256 } from "@noble/hashes/sha2";

const WireBlockHeaderBase = new StructCodec({
	version: U32LE,
	prevHash: Bytes32,
	merkleRoot: Bytes32,
	timestamp: U32LE,
	bits: U32LE,
	nonce: U32LE,
});

type WireBlockHeaderBase = Codec.InferOutput<typeof WireBlockHeaderBase>;

export type WireBlockHeader = WireBlockHeaderBase & { hash: Uint8Array };
export const WireBlockHeader = WireBlockHeaderBase.transform((value, bytes): WireBlockHeader => {
	return {
		hash: sha256(sha256(bytes)),
		...value,
	};
});
