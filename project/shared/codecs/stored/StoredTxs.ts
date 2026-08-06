import { ArrayCodec, Codec, VarInt } from "@nomadshiba/codec";
import { StoredTx } from "~/stored/StoredTx.ts";

export type StoredTxs = Codec.InferOutput<typeof StoredTxs>;
export const StoredTxs = new ArrayCodec(StoredTx, { counter: VarInt });
