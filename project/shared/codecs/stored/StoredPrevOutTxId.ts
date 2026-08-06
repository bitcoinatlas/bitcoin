import { Codec } from "@nomadshiba/codec";
import { NullableNumaric } from "~/primitives/NullableNumaric.ts";
import { StoredTxIdPointer } from "~/stored/StoredTxIdPointer.ts";

export type StoredPrevOutTxId = Codec.InferOutput<typeof StoredPrevOutTxId>;
export const StoredPrevOutTxId = new NullableNumaric(StoredTxIdPointer);
