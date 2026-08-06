import { ArrayCodec, Codec, VarInt } from "@nomadshiba/codec";
import { WireTx } from "~/wire/WireTx.ts";

export type WireTxs = Codec.InferOutput<typeof WireTxs>;
export const WireTxs = new ArrayCodec(WireTx, { counter: VarInt });
