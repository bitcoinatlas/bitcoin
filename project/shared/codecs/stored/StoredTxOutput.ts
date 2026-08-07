import { Codec, StructCodec, VarInt } from "@nomadshiba/codec";
import { StoredPubkeyPointer } from "~/stored/StoredPubkeyPointer.ts";

export type StoredTxOutput = Codec.InferOutput<typeof StoredTxOutput>;
export const StoredTxOutput = new StructCodec({
	value: VarInt,
	scriptPubKey: StoredPubkeyPointer,
});
