import { Codec, StructCodec, U32 } from "@nomadshiba/codec";
import { U56 } from "~/primitives/U56.ts";
import { StoredTxPointer } from "~/stored/StoredTxPointer.ts";

export type StoredBlockInfo = Codec.InferOutput<typeof StoredBlockInfo>;
export const StoredBlockInfo = new StructCodec({
	wireSize: U32,
	txPointer: StoredTxPointer,
	txCount: U32,
	reward: U56, // These are has to be fixed to be on ArrayStore, since this is per block size doesnt matter much anyway
});
