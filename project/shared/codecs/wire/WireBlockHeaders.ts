import { ArrayCodec, Codec } from "@nomadshiba/codec";
import { WireBlockHeader } from "~/wire/WireBlockHeader.ts";

export type WireBlockHeaders = Codec.InferOutput<typeof WireBlockHeaders>;
export const WireBlockHeaders = new ArrayCodec(WireBlockHeader);
