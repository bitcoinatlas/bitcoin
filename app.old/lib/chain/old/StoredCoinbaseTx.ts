import { dyn } from "~/traits.ts";
import type { BoundCodec } from "~/lib/codec/mod.ts";
import { asDyn } from "~/lib/codec/mod.ts";
import { I32, U32 } from "~/lib/codec/primitives.ts";
import { Bytes } from "~/lib/codec/bytes.ts";
import { Bytes32 } from "~/lib/codec/bitcoin.ts";
import { StructCodec, VectorCodec } from "~/lib/codec/composites.ts";
import { StoredTxOutput } from "~/lib/chain/primitives/StoredTxOutput.ts";

// Per block optimizations like coinbase tx, doesn't save that much space,
// But its easy to implement so why not. Why store 0s randomly in the middle of the chunk?
export type StoredCoinbaseTx = {
	txId: Uint8Array;
	version: number;
	lockTime: number;
	sequence: number;
	coinbase: Uint8Array;
	vout: StoredTxOutput[];
};

const i32 = dyn(I32, I32.create());
const u32 = dyn(U32, U32.create());
const bytes32 = dyn(Bytes32, Bytes32.create());
const bytesVar = dyn(Bytes, Bytes.variable());

const voutVec = VectorCodec.create(asDyn(StoredTxOutput));

const storedCoinbaseTxCodec = StructCodec.create<StoredCoinbaseTx>({
	txId: bytes32,
	version: i32,
	lockTime: u32,
	sequence: u32,
	coinbase: bytesVar,
	vout: asDyn<StoredTxOutput[]>({
		stride: voutVec.stride,
		encode: (v) => VectorCodec.encode(voutVec, v),
		decode: (d): [StoredTxOutput[], number] => VectorCodec.decode(voutVec, d),
	}),
});

export const StoredCoinbaseTx: BoundCodec<StoredCoinbaseTx> = {
	stride: storedCoinbaseTxCodec.stride,
	encode(value: StoredCoinbaseTx): Uint8Array {
		return StructCodec.encode(storedCoinbaseTxCodec, value);
	},
	decode(data: Uint8Array): [StoredCoinbaseTx, number] {
		return StructCodec.decode(storedCoinbaseTxCodec, data);
	},
};
