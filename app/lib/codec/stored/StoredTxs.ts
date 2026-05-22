import { ArrayCodec, Codec } from "@nomadshiba/codec";
import { CompactSize } from "~/lib/codec/primitives.ts";
import { StoredTx } from "~/lib/codec/stored/StoredTx.ts";

export type StoredTxs = Codec.Infer<typeof StoredTxs>;
export const StoredTxs = new ArrayCodec(StoredTx, { counter: CompactSize });
