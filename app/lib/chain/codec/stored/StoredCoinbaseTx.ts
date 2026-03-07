import { ArrayCodec, Codec, StructCodec, U32LE } from "@nomadshiba/codec";
import { StoredTxOutput } from "~/lib/chain/codec/stored/StoredTxOutput.ts";
import { Bytes32, CompactSize } from "~/lib/codec/primitives.ts";
import { TimeLock } from "../TimeLock.ts";
import { StoredTxInput } from "./StoredTxInput.ts";

// Per block optimizations like coinbase transaction, doesn't save that much space,
// But its easy to implement so why not. Why store 0s randomly in the middle of the chunk?
export type StoredCoinbaseTx = Codec.Infer<typeof StoredCoinbaseTx>;
export const StoredCoinbaseTx = new StructCodec({
	txId: Bytes32,
	version: U32LE,
	lockTime: TimeLock,
	coinbase: StoredTxInput,
	outputs: new ArrayCodec(StoredTxOutput, { countCodec: CompactSize }),
});
