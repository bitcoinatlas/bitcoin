import { ArrayCodec, Codec, VarInt } from "@nomadshiba/codec";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U40 } from "~/codec/primitives/U40.ts";
import { LockTimeVersionPack } from "~/codec/stored/StoredLockTimeVersionPack.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/codec/stored/StoredTxOutput.ts";

/**
 * StoredTx binary layout (optimized for disk storage)
 *
 * - txId: 32 bytes (full hash)
 * - locktime + version: packed into a 1-byte tag + conditional payload (see
 *     StoredLockTimeVersionPack). The tag folds the common (version,
 *     locktime-present) combinations together; the dominant case (v1/v2 + no
 *     locktime) stores ZERO payload bytes -- just the tag. Other cases carry
 *     only what they need:
 *       v1_none -> version 1, locktime none      (no payload)
 *       v2_none -> version 2, locktime none      (no payload)
 *       v1_some -> version 1, locktime set        (LockTime payload)
 *       v2_some -> version 2, locktime set        (LockTime payload)
 *       raw     -> explicit version + locktime    (U32LE version + LockTime)
 *   In the decoded object these surface as flat `locktime` and `version`
 *   fields; on the wire they share the single packed tag.
 * - vout[]: VarInt count + StoredTxOutput[]
 * - vin[]:  VarInt count + StoredTxInput[] (pointers for prevOut when resolved)
 *
 * The header (txId + packed locktime/version) is variable-length, so locating
 * any field past it requires measuring the encoded header rather than assuming
 * a fixed offset. `encodeWithOffsets` does this and reports each vout's and
 * each vin's relative byte offset.
 */

// Field codecs, referenced directly by encode/decode below.
const TXID = Bytes32;
const SPENDER = U40;
const PACK = LockTimeVersionPack;
const INPUTS = new ArrayCodec(StoredTxInput, { counter: VarInt });
const OUTPUTS = new ArrayCodec(StoredTxOutput, { counter: VarInt });

// locktime/version are spread from the pack codec's output so they sit at the
// top level of StoredTx rather than nested under a `locktimeVersionPack` key.
export type StoredTx =
	& {
		txId: Codec.InferOutput<typeof TXID>;
		spender: Codec.InferOutput<typeof SPENDER>;
	}
	& Codec.InferOutput<typeof PACK>
	& {
		inputs: Codec.InferOutput<typeof INPUTS>;
		outputs: Codec.InferOutput<typeof OUTPUTS>;
	};

/** Offsets reported by {@link StoredTxCodec.encodeWithOffsets}. */
export type StoredTxOffsets = { outputs: number[]; inputs: number[] };

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
		const spenderBytes = SPENDER.encode(value.spender);
		const packBytes = PACK.encode(value);
		const voutBytes = OUTPUTS.encode(value.outputs);
		const vinBytes = INPUTS.encode(value.inputs);

		const totalSize = txIdBytes.length +
			spenderBytes.length +
			packBytes.length +
			voutBytes.length +
			vinBytes.length;

		const bytes = target ?? new Uint8Array(totalSize);

		let pos = 0;
		bytes.set(txIdBytes, pos);
		pos += txIdBytes.length;
		bytes.set(spenderBytes, pos);
		pos += spenderBytes.length;
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
		const [spender, spenderSize] = SPENDER.decode(data.subarray(pos));
		pos += spenderSize;
		const [{ locktime, version }, packSize] = PACK.decode(data.subarray(pos));
		pos += packSize;
		const [vout, voutSize] = OUTPUTS.decode(data.subarray(pos));
		pos += voutSize;
		const [vin, vinSize] = INPUTS.decode(data.subarray(pos));
		pos += vinSize;

		return [{ txId, spender, locktime, version, outputs: vout, inputs: vin }, pos];
	}

	public encodeWithOffsets(
		value: StoredTx,
		target?: Uint8Array<ArrayBuffer>,
	): { bytes: Uint8Array<ArrayBuffer>; offsets: StoredTxOffsets } {
		const txIdBytes = TXID.encode(value.txId);
		const spenderBytes = SPENDER.encode(value.spender);
		const packBytes = PACK.encode({ locktime: value.locktime, version: value.version });
		const voutCountBytes = VarInt.encode(value.outputs.length);
		const encodedVouts = value.outputs.map((output) => StoredTxOutput.encode(output));
		const vinCountBytes = VarInt.encode(value.inputs.length);
		const encodedVins = value.inputs.map((input) => StoredTxInput.encode(input));

		const totalSize = txIdBytes.length +
			spenderBytes.length +
			packBytes.length +
			voutCountBytes.length +
			encodedVouts.reduce((sum, v) => sum + v.length, 0) +
			vinCountBytes.length +
			encodedVins.reduce((sum, v) => sum + v.length, 0);

		const bytes = target ?? new Uint8Array(totalSize);
		const outputs: number[] = [];
		const inputs: number[] = [];

		let pos = 0;
		bytes.set(txIdBytes, pos);
		pos += txIdBytes.length;
		bytes.set(spenderBytes, pos);
		pos += spenderBytes.length;
		bytes.set(packBytes, pos);
		pos += packBytes.length;
		bytes.set(voutCountBytes, pos);
		pos += voutCountBytes.length;
		for (const encodedVout of encodedVouts) {
			outputs.push(pos);
			bytes.set(encodedVout, pos);
			pos += encodedVout.length;
		}
		bytes.set(vinCountBytes, pos);
		pos += vinCountBytes.length;
		for (const encodedVin of encodedVins) {
			inputs.push(pos);
			bytes.set(encodedVin, pos);
			pos += encodedVin.length;
		}

		return { bytes, offsets: { outputs: outputs, inputs: inputs } };
	}
}

export const StoredTx = new StoredTxCodec();
