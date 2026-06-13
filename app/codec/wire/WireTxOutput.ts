import { BytesCodec, Codec, StructCodec, U64LE } from "@nomadshiba/codec";
import { CompactSize } from "~/codec/primitives/CompactSize.ts";

export type WireTxOutput = Codec.InferOutput<typeof WireTxOutput>;
export const WireTxOutput = new StructCodec({
	value: U64LE,
	scriptPubKey: new BytesCodec({ sizer: CompactSize }),
});
