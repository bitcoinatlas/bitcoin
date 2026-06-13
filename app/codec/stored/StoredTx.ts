import { ArrayCodec, Codec, VarInt } from "@nomadshiba/codec";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { LockTimeVersionPack } from "~/codec/stored/StoredLockTimeVersionPack.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/codec/stored/StoredTxOutput.ts";

/**
 * StoredTx binary layout (optimized for disk storage)
 *
 * - txId: 32 bytes (full hash)
 * - lockTime + version: packed into a 1-byte tag + conditional payload (see
 *     StoredLockTimeVersionPack). The tag folds the common (version,
 *     locktime-present) combinations together; the dominant case (v1/v2 + no
 *     locktime) stores ZERO payload bytes -- just the tag. Other cases carry
 *     only what they need:
 *       v1_none -> version 1, locktime none      (no payload)
 *       v2_none -> version 2, locktime none      (no payload)
 *       v1_some -> version 1, locktime set        (LockTime payload)
 *       v2_some -> version 2, locktime set        (LockTime payload)
 *       raw     -> explicit version + locktime    (U32LE version + LockTime)
 *   In the decoded object these surface as flat `lockTime` and `version`
 *   fields; on the wire they share the single packed tag.
 * - vout[]: VarInt count + StoredTxOutput[]
 * - vin[]:  VarInt count + StoredTxInput[] (pointers for prevOut when resolved)
 *
 * The header (txId + packed lockTime/version) is variable-length, so locating
 * any field past it requires measuring the encoded header rather than assuming
 * a fixed offset. `encodeWithOffsets` does this and reports each vout's and
 * each vin's relative byte offset.
 */

// Field codecs, referenced directly by encode/decode below.
const TXID = Bytes32;
const PACK = LockTimeVersionPack;
const VOUT = new ArrayCodec(StoredTxOutput, { counter: VarInt });
const VIN = new ArrayCodec(StoredTxInput, { counter: VarInt });

// lockTime/version are spread from the pack codec's output so they sit at the
// top level of StoredTx rather than nested under a `lockTimeVersionPack` key.
export type StoredTx =
	& {
		txId: Codec.InferOutput<typeof TXID>;
	}
	& Codec.InferOutput<typeof PACK>
	& {
		vout: Codec.InferOutput<typeof VOUT>;
		vin: Codec.InferOutput<typeof VIN>;
	};

/** Offsets reported by {@link StoredTxCodec.encodeWithOffsets}. */
export type StoredTxOffsets = { vout: number[]; vin: number[] };

/**
 * Codec for a stored transaction.
 *
 * encode/decode handle the plain path; encodeWithOffsets additionally reports
 * the relative byte offset of each vout and vin item: add `txPointer` (the blob
 * offset where the tx was appended) to each `offsets.vout[i]` or
 * `offsets.vin[i]` to get the absolute blob pointer for output i or input i.
 */
export class StoredTxCodec extends Codec<StoredTx> {
	public readonly stride = { kind: "variable" } as const;

	public encode(value: StoredTx, target?: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
		const txIdBytes = TXID.encode(value.txId);
		const packBytes = PACK.encode({ lockTime: value.lockTime, version: value.version });
		const voutBytes = VOUT.encode(value.vout);
		const vinBytes = VIN.encode(value.vin);

		const totalSize = txIdBytes.length +
			packBytes.length +
			voutBytes.length +
			vinBytes.length;

		const bytes = target ?? new Uint8Array(totalSize);

		let pos = 0;
		bytes.set(txIdBytes, pos);
		pos += txIdBytes.length;
		bytes.set(packBytes, pos);
		pos += packBytes.length;
		bytes.set(voutBytes, pos);
		pos += voutBytes.length;
		bytes.set(vinBytes, pos);

		return bytes;
	}

	public decode(data: Uint8Array): [StoredTx, number] {
		let pos = 0;

		const [txId, txIdSize] = TXID.decode(data.subarray(pos));
		pos += txIdSize;
		const [{ lockTime, version }, packSize] = PACK.decode(data.subarray(pos));
		pos += packSize;
		const [vout, voutSize] = VOUT.decode(data.subarray(pos));
		pos += voutSize;
		const [vin, vinSize] = VIN.decode(data.subarray(pos));
		pos += vinSize;

		return [{ txId, lockTime, version, vout, vin }, pos];
	}

	public encodeWithOffsets(
		value: StoredTx,
		target?: Uint8Array<ArrayBuffer>,
	): { bytes: Uint8Array<ArrayBuffer>; offsets: StoredTxOffsets } {
		const txIdBytes = TXID.encode(value.txId);
		const packBytes = PACK.encode({ lockTime: value.lockTime, version: value.version });
		const voutCountBytes = VarInt.encode(value.vout.length);
		const encodedVouts = value.vout.map((output) => StoredTxOutput.encode(output));
		const vinCountBytes = VarInt.encode(value.vin.length);
		const encodedVins = value.vin.map((input) => StoredTxInput.encode(input));

		const totalSize = txIdBytes.length +
			packBytes.length +
			voutCountBytes.length +
			encodedVouts.reduce((sum, v) => sum + v.length, 0) +
			vinCountBytes.length +
			encodedVins.reduce((sum, v) => sum + v.length, 0);

		const bytes = target ?? new Uint8Array(totalSize);
		const vout: number[] = [];
		const vin: number[] = [];

		let pos = 0;
		bytes.set(txIdBytes, pos);
		pos += txIdBytes.length;
		bytes.set(packBytes, pos);
		pos += packBytes.length;
		bytes.set(voutCountBytes, pos);
		pos += voutCountBytes.length;
		for (const encodedVout of encodedVouts) {
			vout.push(pos);
			bytes.set(encodedVout, pos);
			pos += encodedVout.length;
		}
		bytes.set(vinCountBytes, pos);
		pos += vinCountBytes.length;
		for (const encodedVin of encodedVins) {
			vin.push(pos);
			bytes.set(encodedVin, pos);
			pos += encodedVin.length;
		}

		return { bytes, offsets: { vout, vin } };
	}
}

export const StoredTx = new StoredTxCodec();
