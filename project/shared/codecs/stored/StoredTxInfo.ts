import { Codec, StructCodec } from "@nomadshiba/codec";
import { StoredTxPointer } from "~/stored/StoredTxPointer.ts";
import { U40 } from "~/primitives/U40.ts";

export type StoredTxInfo = Codec.InferOutput<typeof StoredTxInfo>;
export const StoredTxInfo = new StructCodec({ txPointer: StoredTxPointer, totalOutput: U40 });
