import { Codec } from "@nomadshiba/codec";
import { NullableNumaric } from "~/codec/primitives/NullableNumaric.ts";
import { StoredTxIdCursor } from "~/codec/stored/StoredTxIdCursor.ts";

export type StoredPrevOutTxId = Codec.InferOutput<typeof StoredPrevOutTxId>;
export const StoredPrevOutTxId = new NullableNumaric(StoredTxIdCursor);
