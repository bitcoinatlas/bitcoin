import { ArrayCodec } from "@nomadshiba/codec";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";

export const WireBlockHeaders = new ArrayCodec(WireBlockHeader);
