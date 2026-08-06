import { Codec } from "@nomadshiba/codec";
import { U48 } from "~/primitives/U48.ts";

export type StoredTxIdPointer = Codec.InferOutput<typeof StoredTxIdPointer>;
export const StoredTxIdPointer = U48;
