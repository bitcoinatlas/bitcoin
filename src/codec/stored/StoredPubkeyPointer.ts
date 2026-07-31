import { Codec } from "@nomadshiba/codec";
import { U40 } from "~/codec/primitives/U40.ts";

export type StoredPubkeyPointer = Codec.InferOutput<typeof StoredPubkeyPointer>;
export const StoredPubkeyPointer = U40;
