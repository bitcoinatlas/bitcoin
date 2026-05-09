import { ArrayCodec, Codec, StructCodec } from "@nomadshiba/codec";
import { StoredTx } from "~/lib/chain/codec/stored/StoredTx.ts";
import { CompactSize } from "~/lib/codec/primitives.ts";

export type StoredBlock = Codec.Infer<typeof StoredBlock>;
export const StoredBlock = new StructCodec({ transactions: new ArrayCodec(StoredTx, { countCodec: CompactSize }) });
