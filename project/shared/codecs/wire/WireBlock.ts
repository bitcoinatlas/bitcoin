import { ArrayCodec, Codec, StructCodec } from "@nomadshiba/codec";
import { WireTx } from "~/wire/WireTx.ts";
import { CompactSize } from "~/primitives/CompactSize.ts";
import { WireBlockHeader } from "~/wire/WireBlockHeader.ts";

export type WireBlock = Codec.InferOutput<typeof WireBlock>;
export const WireBlock = new StructCodec({
	header: WireBlockHeader,
	txs: new ArrayCodec(WireTx, { counter: CompactSize }),
});
