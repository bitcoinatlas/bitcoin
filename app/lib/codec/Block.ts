import { ArrayCodec, Codec, StructCodec, u32LE } from "@nomadshiba/codec";
import { txCodec } from "~/lib/codec/Tx.ts";
import { bytes32, compactSize } from "~/lib/codec/primitives.ts";

export type BlockData = Codec.Infer<typeof blockCodec>;
export type BlockHeaderData = BlockData["header"];
export const blockCodec = new StructCodec({
	header: new StructCodec({
		version: u32LE,
		prevHash: bytes32,
		merkleRoot: bytes32,
		timestamp: u32LE,
		bits: u32LE,
		nonce: u32LE,
	}),
	txs: new ArrayCodec(txCodec, { countCodec: compactSize }),
});
