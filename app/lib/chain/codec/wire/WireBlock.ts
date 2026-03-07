import { ArrayCodec, Codec, StructCodec } from "@nomadshiba/codec";
import { WireTx } from "~/lib/chain/codec/wire/WireTx.ts";
import { CompactSize } from "~/lib/codec/primitives.ts";
import { WireBlockHeader } from "./WireBlockHeader.ts";

export type WireBlock = Codec.Infer<typeof WireBlock>;
export const WireBlock = new StructCodec({
	header: WireBlockHeader,
	txs: new ArrayCodec(WireTx, { countCodec: CompactSize }),
});
