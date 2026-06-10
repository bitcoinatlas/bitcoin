import { ArrayCodec, Codec, EnumCodec, StructCodec, U32LE, U8, VarInt, Void } from "@nomadshiba/codec";
import { Bytes32 } from "~/lib/codec/primitives.ts";
import { StoredTxInput } from "~/lib/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/lib/codec/stored/StoredTxOutput.ts";
import { TimeLock } from "~/lib/codec/TimeLock.ts";

/**
 * StoredTx binary layout (optimized for disk storage)
 *
 * - txId: 32 bytes (full hash)
 * - lockTimeVersionPack: 1-byte EnumCodec discriminant (U8) + conditional payload.
 *     The discriminant folds the common (version, locktime-present) combinations
 *     into a single tag byte. The dominant case (v1/v2 + no locktime) stores ZERO
 *     payload bytes -- just the tag. Other cases carry only what they need:
 *       v1_none   -> version 1, locktime none      (Void payload)
 *       v2_none   -> version 2, locktime none      (Void payload)
 *       v1_some-> version 1, locktime set        (TimeLock payload)
 *       v2_some-> version 2, locktime set        (TimeLock payload)
 *       any    -> explicit version + locktime    (U32LE version + TimeLock)
 * - vout[]: VarInt count + StoredTxOutput[]
 * - vin[]:  VarInt count + StoredTxInput[] (pointers for prevOut when resolved)
 */

export type StoredTx = Codec.InferInput<typeof StoredTx>;
export type StoredTxWithMethods = Codec.InferOutput<typeof StoredTx>;

export type LockTimeVersionPack = { lockTime: TimeLock; version: number };

const Some = new StructCodec({ lockTime: TimeLock });
const None = new StructCodec({});

export const StoredTx = new StructCodec({
	txId: Bytes32,
	lockTimeVersionPack: new EnumCodec({
		raw: new StructCodec({ lockTime: TimeLock, version: U32LE }),
		v1_none: None.transform((): LockTimeVersionPack => ({ lockTime: { kind: "none" }, version: 0x1 })),
		v2_none: None.transform((): LockTimeVersionPack => ({ lockTime: { kind: "none" }, version: 0x2 })),
		v1_some: Some.transform(({ lockTime }): LockTimeVersionPack => ({ lockTime, version: 0x1 })),
		v2_some: Some.transform(({ lockTime }): LockTimeVersionPack => ({ lockTime, version: 0x2 })),
	}, { indexer: U8 }),
	vout: new ArrayCodec(StoredTxOutput, { counter: VarInt }),
	vin: new ArrayCodec(StoredTxInput, { counter: VarInt }),
});

/**
 * Encodes a StoredTx into bytes and returns the relative byte offset of each
 * vout item within that sequence.
 *
 * Add `txPointer` (the blob offset where the tx was appended) to each
 * `voutOffsets[i]` to get the absolute blob pointer for output i.
 *
 * The header (txId + lockTimeVersionPack) is variable-length now, so its size
 * is measured from the actual encoded bytes rather than summed from strides.
 */
export function encodeStoredTxWithOutputOffsets(tx: StoredTx): {
	bytes: Uint8Array;
	voutOffsets: number[];
} {
	const txIdBytes = StoredTx.shape.txId.encode(tx.txId);
	const packBytes = StoredTx.shape.lockTimeVersionPack.encode(tx.lockTimeVersionPack);

	const voutCountBytes = VarInt.encode(tx.vout.length);
	const encodedVouts = tx.vout.map((output) => StoredTxOutput.encode(output));
	const vinBytes = StoredTx.shape.vin.encode(tx.vin);

	const totalSize = txIdBytes.length +
		packBytes.length +
		voutCountBytes.length +
		encodedVouts.reduce((sum, v) => sum + v.length, 0) +
		vinBytes.length;

	const bytes = new Uint8Array(totalSize);
	const voutOffsets: number[] = [];

	let pos = 0;
	bytes.set(txIdBytes, pos);
	pos += txIdBytes.length;
	bytes.set(packBytes, pos);
	pos += packBytes.length;
	bytes.set(voutCountBytes, pos);
	pos += voutCountBytes.length;
	for (const encodedVout of encodedVouts) {
		voutOffsets.push(pos);
		bytes.set(encodedVout, pos);
		pos += encodedVout.length;
	}
	bytes.set(vinBytes, pos);

	return { bytes, voutOffsets };
}
