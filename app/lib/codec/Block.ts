import { ArrayCodec, Codec, StructCodec, U32LE } from "@nomadshiba/codec";
import { txCodec } from "~/lib/codec/Tx.ts";
import { Bytes32, CompactSize } from "~/lib/codec/primitives.ts";

export type BlockData = Codec.Infer<typeof blockCodec>;
export type BlockHeaderData = BlockData["header"];
export const blockCodec = new StructCodec({
	header: new StructCodec({
		version: U32LE,
		prevHash: Bytes32,
		merkleRoot: Bytes32,
		timestamp: U32LE,
		bits: U32LE,
		nonce: U32LE,
	}),
	txs: new ArrayCodec(txCodec, { countCodec: CompactSize }),
});
