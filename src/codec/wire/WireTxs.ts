import { ArrayCodec, Codec, VarInt } from "@nomadshiba/codec";
import { WireTx } from "~/codec/wire/WireTx.ts";

export type WireTxs = Codec.InferOutput<typeof WireTxs>;
export const WireTxs = new ArrayCodec(WireTx, { counter: VarInt });
