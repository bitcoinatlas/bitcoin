import { Codec } from "@nomadshiba/codec";
import { NullableNumaricCodec } from "~/primitives/NullableNumaric.ts";
import { StoredTxIdPointer } from "~/stored/StoredTxIdPointer.ts";

export type StoredPrevOutTxId = Codec.InferOutput<typeof StoredPrevOutTxId>;
export const StoredPrevOutTxId = new NullableNumaricCodec(StoredTxIdPointer);
