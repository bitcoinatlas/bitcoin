import { ArrayCodec, Codec, StructCodec, U32LE } from "@nomadshiba/codec";
import { StoredTxInput } from "~/lib/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/lib/codec/stored/StoredTxOutput.ts";
import { TimeLock } from "~/lib/codec/TimeLock.ts";
import { Bytes32, CompactSize } from "~/lib/codec/primitives.ts";

// StoredTx binary layout (optimized for disk storage):
// - txId: 32 bytes (full hash)
// - version: 4 bytes (u32LE)
// - lockTime: 4 bytes (u32LE) - stored as raw number, converted to TimeLock on decode
// - vout[]: CompactSize count + StoredTxOutput[]
// - vin[]: CompactSize count + StoredTxInput[] (uses pointers for prevOut when resolved)

export type StoredTx = Codec.InferOutput<typeof StoredTx>;
export const StoredTx = new StructCodec({
	txId: Bytes32,
	version: U32LE,
	lockTime: TimeLock,
	vout: new ArrayCodec(StoredTxOutput, { counter: CompactSize }),
	vin: new ArrayCodec(StoredTxInput, { counter: CompactSize }),
});

/**
 * Encodes a StoredTx into chunks and returns both the full byte sequence and
 * the relative byte offset of each vout item within that sequence.
 *
 * Add `txPointer` (the blob offset where the tx was appended) to each
 * `voutOffsets[i]` to get the absolute blob pointer for output i.
 */
export function encodeStoredTxWithOutputOffsets(tx: StoredTx): {
	bytes: Uint8Array;
	voutOffsets: number[];
} {
	const headerSize =
		StoredTx.shape.txId.stride.size +
		StoredTx.shape.version.stride.size +
		StoredTx.shape.lockTime.stride.size;

	const voutCountBytes = CompactSize.encode(tx.vout.length);
	const encodedVouts = tx.vout.map((output) => StoredTxOutput.encode(output));
	const vinBytes = StoredTx.shape.vin.encode(tx.vin);

	const totalSize =
		headerSize +
		voutCountBytes.length +
		encodedVouts.reduce((sum, v) => sum + v.length, 0) +
		vinBytes.length;

	const bytes = new Uint8Array(totalSize);
	const voutOffsets: number[] = [];

	let pos = 0;
	bytes.set(StoredTx.shape.txId.encode(tx.txId), pos); pos += StoredTx.shape.txId.stride.size;
	bytes.set(StoredTx.shape.version.encode(tx.version), pos); pos += StoredTx.shape.version.stride.size;
	bytes.set(StoredTx.shape.lockTime.encode(tx.lockTime), pos); pos += StoredTx.shape.lockTime.stride.size;
	bytes.set(voutCountBytes, pos); pos += voutCountBytes.length;
	for (const encodedVout of encodedVouts) {
		voutOffsets.push(pos);
		bytes.set(encodedVout, pos);
		pos += encodedVout.length;
	}
	bytes.set(vinBytes, pos);

	return { bytes, voutOffsets };
}
