import { Codec, StructCodec, VarInt } from "@nomadshiba/codec";
import { StoredPubkeyCursor } from "~/codec/stored/StoredPubkeyCursor.ts";
import { StoredTxIdCursor } from "~/codec/stored/StoredTxIdCursor.ts";

export type StoredTxOutput = Codec.InferOutput<typeof StoredTxOutput>;
export const StoredTxOutput = new StructCodec({
	value: VarInt,
	scriptPubKey: StoredPubkeyCursor,
	/** Pointer to previous txid pointer+1 of the pubkey. 0 means null */
	previousOutputTx: StoredTxIdCursor,
	// TODO: for things like this^ where 0 means null i should probably have a special codec that wraps any numaric codec
});
