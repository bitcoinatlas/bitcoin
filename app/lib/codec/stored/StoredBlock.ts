import { Codec, StructCodec } from "@nomadshiba/codec";
import { StoredBlockHeader } from "~/lib/codec/stored/StoredBlockHeader.ts";
import { StoredPointer } from "~/lib/codec/stored/StoredPointer.ts";

export type StoredBlock = Codec.Infer<typeof StoredBlock>;
export const StoredBlock = new StructCodec({
	header: StoredBlockHeader,
	pointer: StoredPointer,
});
