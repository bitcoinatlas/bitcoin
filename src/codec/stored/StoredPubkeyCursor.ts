import { Codec } from "@nomadshiba/codec";
import { U40 } from "~/codec/primitives/U40.ts";

export type StoredPubkeyCursor = Codec.InferOutput<typeof StoredPubkeyCursor>;
export const StoredPubkeyCursor = U40;
