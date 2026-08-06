import { Codec } from "@nomadshiba/codec";
import { U48 } from "@project/codecs";

export type StoredPubkeyPointer = Codec.InferOutput<typeof StoredPubkeyPointer>;
export const StoredPubkeyPointer = U48;
