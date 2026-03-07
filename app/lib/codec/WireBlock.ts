import { ArrayCodec, Codec, StructCodec, U32LE } from "@nomadshiba/codec";
import { WireTx } from "~/lib/codec/WireTx.ts";
import { Bytes32, CompactSize } from "~/lib/codec/primitives.ts";

export type WireBlock = Codec.Infer<typeof WireBlock>;
export const WireBlock = new StructCodec({
	header: new StructCodec({
		version: U32LE,
		prevHash: Bytes32,
		merkleRoot: Bytes32,
		timestamp: U32LE,
		bits: U32LE,
		nonce: U32LE,
	}),
	txs: new ArrayCodec(WireTx, { countCodec: CompactSize }),
});
