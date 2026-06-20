import { ArrayCodec, Codec, VarInt } from "@nomadshiba/codec";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U40 } from "~/codec/primitives/U40.ts";
import { LockTimeVersionPack } from "~/codec/stored/StoredLockTimeVersionPack.ts";
import { PrevOut, StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/codec/stored/StoredTxOutput.ts";
import { parseScriptPubKey, rawScriptPubKey } from "~/chain/ScriptPubKey.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";
import { equals } from "@std/bytes";
import { COINBASE_TXID, COINBASE_VOUT } from "~/constants.ts";
import { WireTxOutput } from "~/codec/wire/WireTxOutput.ts";
import { WireTxInput } from "~/codec/wire/WireTxInput.ts";
import { ChainStore } from "~/chain/ChainStore.ts";

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

	async toWire(storedTx: StoredTx, chainStore: ChainStore): Promise<WireTx> {
		const { txId, version, locktime } = storedTx;

		const inputs: WireTxInput[] = [];
		const witness: Uint8Array[][] = [];

		for (const input of storedTx.inputs) {
			const prevTxId = await chainStore.getPrevOutTxId(input);
			inputs.push({
				prevOut: {
					txId: prevTxId,
					vout: input.prevOut.vout,
				},
				scriptSig: input.scriptSig,
				sequence: input.sequence,
			});
			if (input.witness) witness.push(input.witness);
		}

		const outputs: WireTxOutput[] = [];
		for (const output of storedTx.outputs) {
			const scriptPubKey = await chainStore.getScriptPubKey(output);
			outputs.push({
				value: output.value,
				scriptPubKey: rawScriptPubKey(scriptPubKey),
			});
		}

		return { txId, version, locktime, inputs, outputs, witness };
	}

	fromWire(wireTx: WireTx): StoredTx {
		const inputs: StoredTxInput[] = wireTx.inputs.map((wireInput, i): StoredTxInput => {
			let txId: PrevOut["txId"];
			if (equals(wireInput.prevOut.txId, COINBASE_TXID) && wireInput.prevOut.vout === COINBASE_VOUT) {
				txId = { kind: "coinbase" };
			} else {
				txId = { kind: "raw", value: wireInput.prevOut.txId };
			}
			return {
				prevOut: { txId, vout: wireInput.prevOut.vout },
				scriptSig: wireInput.scriptSig,
				sequence: wireInput.sequence,
				witness: wireTx.witness[i] ?? [],
			};
		});

		const outputs: StoredTxOutput[] = [];
		for (const wireOutput of wireTx.outputs) {
			const scriptPubKey = parseScriptPubKey(wireOutput.scriptPubKey);
			const output: StoredTxOutput = { value: wireOutput.value, scriptPubKey };
			outputs.push(output);
		}

		const tx: StoredTx = {
			txId: wireTx.txId,
			spender: 0,
			version: wireTx.version,
			locktime: wireTx.locktime,
			inputs: inputs,
			outputs: outputs,
		};

		return tx;
	}

	public encode(value: StoredTx): Uint8Array<ArrayBuffer> {
		// Size-compute pass.
		const txIdBytes = TXID.encode(value.txId);
		const packBytes = PACK.encode(value);
		const voutBytes = OUTPUTS.encode(value.outputs);
		const vinBytes = INPUTS.encode(value.inputs);

		const totalSize = txIdBytes.length + SPENDER.stride.size +
			packBytes.length + voutBytes.length + vinBytes.length;

		const bytes = new Uint8Array(totalSize);
		let pos = 0;
		bytes.set(txIdBytes, pos);
		pos += txIdBytes.length;
		pos += SPENDER.encodeInto(value.spender, bytes, pos);
		bytes.set(packBytes, pos);
		pos += packBytes.length;
		bytes.set(voutBytes, pos);
		pos += voutBytes.length;
		bytes.set(vinBytes, pos);
		return bytes;
	}

	public override encodeInto(value: StoredTx, target: Uint8Array, offset: number = 0): number {
		const start = offset;
		offset += TXID.encodeInto(value.txId, target, offset);
		offset += SPENDER.encodeInto(value.spender, target, offset);
		offset += PACK.encodeInto(value, target, offset);
		offset += OUTPUTS.encodeInto(value.outputs, target, offset);
		offset += INPUTS.encodeInto(value.inputs, target, offset);
		return offset - start;
	}

	public override size(value: StoredTx): number {
		return TXID.stride.size + SPENDER.stride.size + PACK.size(value) +
			OUTPUTS.size(value.outputs) + INPUTS.size(value.inputs);
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

	/**
	 * Writes the tx directly into `target` at `offset` (no intermediate
	 * allocations) and returns each vout's and vin's tx-relative byte offset.
	 * Add `txPointer` (the blob offset where the tx starts) to each returned
	 * offset to get the absolute blob pointer for that output/input.
	 *
	 * Invariant: the number of bytes written equals `this.size(value)`.
	 */
	public encodeWithOffsets(value: StoredTx, target: Uint8Array, offset: number): StoredTxOffsets {
		const start = offset;
		offset += TXID.encodeInto(value.txId, target, offset);
		offset += SPENDER.encodeInto(value.spender, target, offset);
		offset += PACK.encodeInto(value, target, offset);

		const outputs: number[] = [];
		offset += VarInt.encodeInto(value.outputs.length, target, offset);
		for (const output of value.outputs) {
			outputs.push(offset - start);
			offset += StoredTxOutput.encodeInto(output, target, offset);
		}

		const inputs: number[] = [];
		offset += VarInt.encodeInto(value.inputs.length, target, offset);
		for (const input of value.inputs) {
			inputs.push(offset - start);
			offset += StoredTxInput.encodeInto(input, target, offset);
		}

		return { outputs, inputs };
	}
}

export const StoredTx = new StoredTxCodec();
