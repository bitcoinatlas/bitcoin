import { Codec } from "@nomadshiba/codec";
import { U48 } from "~/codec/primitives/U48.ts";

export type StoredTxPointer = Codec.InferOutput<typeof StoredTxPointer>;
export const StoredTxPointer = U48;
