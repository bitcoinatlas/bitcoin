import { BigVarInt, Codec, StructCodec } from "@nomadshiba/codec";
import { StoredPubkeyPointer } from "~/codec/stored/StoredPubkeyPointer.ts";

export type StoredTxOutput = Codec.InferOutput<typeof StoredTxOutput>;
export const StoredTxOutput = new StructCodec({
	value: BigVarInt,
	scriptPubKey: StoredPubkeyPointer,
});
