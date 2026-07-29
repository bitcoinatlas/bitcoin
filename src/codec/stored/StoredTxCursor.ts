import { Codec } from "@nomadshiba/codec";
import { U48 } from "~/codec/primitives/U48.ts";

export type StoredTxCursor = Codec.InferOutput<typeof StoredTxCursor>;
export const StoredTxCursor = U48;
