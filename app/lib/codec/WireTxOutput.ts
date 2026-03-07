import { BytesCodec, StructCodec, U64LE } from "@nomadshiba/codec";
import { compactSize } from "~/lib/codec/primitives.ts";
import { Codec } from "@nomadshiba/codec";

export type WireTxOutput = Codec.Infer<typeof WireTxOutput>;
export const WireTxOutput = new StructCodec({
	value: U64LE,
	scriptPubKey: new BytesCodec({ lengthCodec: compactSize }),
});
