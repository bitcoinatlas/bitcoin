import { ArrayCodec, Codec } from "@nomadshiba/codec";
import { CompactSize } from "~/codec/primitives/CompactSize.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";

export type StoredTxs = Codec.InferOutput<typeof StoredTxs>;
export const StoredTxs = new ArrayCodec(StoredTx, { counter: CompactSize });
