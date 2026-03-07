import { ArrayCodec, Codec, StructCodec } from "@nomadshiba/codec";
import { StoredCoinbaseTx } from "~/lib/chain/codec/stored/StoredCoinbaseTx.ts";
import { StoredTx } from "~/lib/chain/codec/stored/StoredTx.ts";
import { CompactSize } from "~/lib/codec/primitives.ts";

export type { StoredCoinbaseTx } from "./StoredCoinbaseTx.ts";

export type StoredBlock = Codec.Infer<typeof StoredBlock>;
export const StoredBlock = new StructCodec({
	coinbaseTx: StoredCoinbaseTx,
	transactions: new ArrayCodec(StoredTx, { countCodec: CompactSize }),
});
