import { Codec, StructCodec, VarInt } from "@nomadshiba/codec";
import { U40 } from "~/codec/primitives/U40.ts";

export type StoredTxOutput = Codec.InferOutput<typeof StoredTxOutput>;
export const StoredTxOutput = new StructCodec({
	value: VarInt,
	scriptPubKey: U40, // pointer
});
