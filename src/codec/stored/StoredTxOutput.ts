import { Codec, StructCodec, VarInt } from "@nomadshiba/codec";
import { StoredPubkeyCursor } from "~/codec/stored/StoredPubkeyCursor.ts";
import { StoredTxIdCursor } from "~/codec/stored/StoredTxIdCursor.ts";
import { NullableNumaric } from "~/codec/primitives/NullableNumaric.ts";

export type StoredTxOutput = Codec.InferOutput<typeof StoredTxOutput>;
export const StoredTxOutput = new StructCodec({
	value: VarInt,
	scriptPubKey: StoredPubkeyCursor,
	/** Pointer to previous txid pointer of the pubkey. */
	previousOutputTx: new NullableNumaric(StoredTxIdCursor),
});
