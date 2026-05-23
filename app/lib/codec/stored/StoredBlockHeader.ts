import { Codec } from "@nomadshiba/codec";
import { WireBlockHeader } from "~/lib/codec/wire/WireBlockHeader.ts";

export type StoredBlockHeader = Codec.InferOutput<typeof StoredBlockHeader>;
export const StoredBlockHeader = WireBlockHeader;
