import { Codec, StructCodec, VarInt } from "@nomadshiba/codec";
import { StoredPubkeyPointer } from "~/stored/StoredPubkeyPointer.ts";
import { StoredTxIdPointer } from "~/stored/StoredTxIdPointer.ts";
import { NullableNumaric } from "~/primitives/NullableNumaric.ts";

export type StoredTxOutput = Codec.InferOutput<typeof StoredTxOutput>;
export const StoredTxOutput = new StructCodec({
	value: VarInt,
	scriptPubKey: StoredPubkeyPointer,
	/** Pointer to previous txid pointer of the pubkey. */
	previousOutputTx: new NullableNumaric(StoredTxIdPointer),
});
