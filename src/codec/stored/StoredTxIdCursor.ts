import { Codec } from "@nomadshiba/codec";
import { U48 } from "~/codec/primitives/U48.ts";

// TODO: decide cursor/pointer terminology, you are keep mixing them.
export type StoredTxIdCursor = Codec.InferOutput<typeof StoredTxIdCursor>;
export const StoredTxIdCursor = U48;
