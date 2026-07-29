import { Codec, StructCodec, VarInt } from "@nomadshiba/codec";
import { StoredPubkeyCursor } from "~/codec/stored/StoredPubkeyCursor.ts";
import { StoredTxCursor } from "~/codec/stored/StoredTxCursor.ts";

export type StoredTxOutput = Codec.InferOutput<typeof StoredTxOutput>;
export const StoredTxOutput = new StructCodec({
	value: VarInt,
	scriptPubKey: StoredPubkeyCursor,
	previousOutputTx: StoredTxCursor,
});
