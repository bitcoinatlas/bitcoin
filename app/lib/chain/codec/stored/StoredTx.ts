import { ArrayCodec, Codec, StructCodec, U32LE } from "@nomadshiba/codec";
import { StoredTxInput } from "~/lib/chain/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/lib/chain/codec/stored/StoredTxOutput.ts";
import { TimeLock } from "~/lib/chain/codec/TimeLock.ts";
import { Bytes32, CompactSize } from "~/lib/codec/primitives.ts";

// StoredTx binary layout (optimized for disk storage):
// - txId: 32 bytes (full hash)
// - version: 4 bytes (u32LE)
// - lockTime: 4 bytes (u32LE) - stored as raw number, converted to TimeLock on decode
// - vout[]: CompactSize count + StoredTxOutput[]
// - vin[]: CompactSize count + StoredTxInput[] (uses pointers for prevOut when resolved)

export type StoredTx = Codec.Infer<typeof StoredTx>;
export const StoredTx = new StructCodec({
	txId: Bytes32,
	version: U32LE,
	lockTime: TimeLock,
	vout: new ArrayCodec(StoredTxOutput, { countCodec: CompactSize }),
	vin: new ArrayCodec(StoredTxInput, { countCodec: CompactSize }),
});
