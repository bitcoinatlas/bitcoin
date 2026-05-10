import { BytesCodec, Codec, StructCodec, U64LE } from "@nomadshiba/codec";
import { CompactSize } from "~/lib/codec/primitives.ts";

export type WireTxOutput = Codec.Infer<typeof WireTxOutput>;
export const WireTxOutput = new StructCodec({
	value: U64LE,
	scriptPubKey: new BytesCodec({ lengthCodec: CompactSize }),
});
