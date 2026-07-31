import { Codec } from "@nomadshiba/codec";
import { U48 } from "~/codec/primitives/U48.ts";

export type StoredTxIdPointer = Codec.InferOutput<typeof StoredTxIdPointer>;
export const StoredTxIdPointer = U48;
