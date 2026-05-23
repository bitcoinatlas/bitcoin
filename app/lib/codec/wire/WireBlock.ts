import { ArrayCodec, Codec, StructCodec } from "@nomadshiba/codec";
import { WireTx } from "~/lib/codec/wire/WireTx.ts";
import { CompactSize } from "~/lib/codec/primitives.ts";
import { WireBlockHeader } from "~/lib/codec/wire/WireBlockHeader.ts";

export type WireBlock = Codec.InferOutput<typeof WireBlock>;
export const WireBlock = new StructCodec({
	header: WireBlockHeader,
	txs: new ArrayCodec(WireTx, { counter: CompactSize }),
});
