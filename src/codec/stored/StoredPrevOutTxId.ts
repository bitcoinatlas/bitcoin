import { Codec } from "@nomadshiba/codec";
import { NullableNumaric } from "~/codec/primitives/NullableNumaric.ts";
import { StoredTxIdPointer } from "~/codec/stored/StoredTxIdPointer.ts";

export type StoredPrevOutTxId = Codec.InferOutput<typeof StoredPrevOutTxId>;
export const StoredPrevOutTxId = new NullableNumaric(StoredTxIdPointer);
