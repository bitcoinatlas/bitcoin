import { dyn } from "~/traits.ts";
import type { BoundCodec } from "~/lib/codec/mod.ts";
import { asDyn } from "~/lib/codec/mod.ts";
import { I32, U32 } from "~/lib/codec/primitives.ts";
import { Bytes32 } from "~/lib/codec/bitcoin.ts";
import { StructCodec, VectorCodec } from "~/lib/codec/composites.ts";
import { StoredTxOutput } from "~/lib/chain/primitives/StoredTxOutput.ts";
import { StoredTxInput } from "~/lib/chain/primitives/StoredTxInput.ts";

export type StoredTx = {
	// This is the only place where we store the full txId,
	// if we dont store it anywhere else, in order to find the txId,
	// we have to hash every tx until the coinbase txs of the utxo we are spending.
	txId: Uint8Array;
	version: number;
	lockTime: number;
	vout: StoredTxOutput[];
	vin: StoredTxInput[];
};

const i32 = dyn(I32, I32.create());
const u32 = dyn(U32, U32.create());
const bytes32 = dyn(Bytes32, Bytes32.create());

const voutVec = VectorCodec.create(asDyn(StoredTxOutput));
const vinVec = VectorCodec.create(asDyn(StoredTxInput));

const storedTxCodec = StructCodec.create<StoredTx>({
	txId: bytes32,
	version: i32,
	lockTime: u32,
	vout: asDyn<StoredTxOutput[]>({
		stride: voutVec.stride,
		encode: (v) => VectorCodec.encode(voutVec, v),
		decode: (d): [StoredTxOutput[], number] => VectorCodec.decode(voutVec, d),
	}),
	vin: asDyn<StoredTxInput[]>({
		stride: vinVec.stride,
		encode: (v) => VectorCodec.encode(vinVec, v),
		decode: (d): [StoredTxInput[], number] => VectorCodec.decode(vinVec, d),
	}),
});

export const StoredTx: BoundCodec<StoredTx> = {
	stride: storedTxCodec.stride,
	encode(value: StoredTx): Uint8Array {
		return StructCodec.encode(storedTxCodec, value);
	},
	decode(data: Uint8Array): [StoredTx, number] {
		return StructCodec.decode(storedTxCodec, data);
	},
};
