import { ArrayCodec, Codec, VarInt } from "@nomadshiba/codec";
import { ChainStore } from "~/chain/ChainStore.ts";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { LockTimeVersionPack } from "~/codec/stored/StoredLockTimeVersionPack.ts";
import { StoredScriptPubKey } from "~/codec/stored/StoredScriptPubkey.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/codec/stored/StoredTxOutput.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";
import { WireTxInput } from "~/codec/wire/WireTxInput.ts";
import { WireTxOutput } from "~/codec/wire/WireTxOutput.ts";

// Field codecs, referenced directly by encode/decode below.
const TXID = Bytes32;
const PACK = LockTimeVersionPack;
const INPUTS = new ArrayCodec(StoredTxInput, { counter: VarInt });
const OUTPUTS = new ArrayCodec(StoredTxOutput, { counter: VarInt });

// locktime/version are spread from the pack codec's output so they sit at the
// top level of StoredTx rather than nested under a `locktimeVersionPack` key.
export type StoredTx =
	& {
		txId: Codec.InferOutput<typeof TXID>;
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

	toWireBytes(storedTx: StoredTx, chainStore: ChainStore): Uint8Array<ArrayBuffer> {
		const { version, locktime } = storedTx;

		const inputs: WireTxInput[] = [];
		const witness: Uint8Array[][] = [];

		for (const input of storedTx.inputs) {
			const prevTxId = chainStore.getPrevOutTxId(input);
			inputs.push({
				prevOut: {
					txId: prevTxId,
					output: input.prevOut.output,
				},
				scriptSig: input.scriptSig,
				sequence: input.sequence,
			});
			if (input.witness) witness.push(input.witness);
		}

		const outputs: WireTxOutput[] = [];
		for (const output of storedTx.outputs) {
			const scriptPubKey = chainStore.atomic.stores.pubkeys.get(output.scriptPubKey, StoredScriptPubKey);
			outputs.push({
				value: output.value,
				scriptPubKey,
			});
		}

		return WireTx.encode({ inputs, locktime, outputs, version, witness });
	}

	public encoder(value: StoredTx, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(value: StoredTx, target: Uint8Array, offset: number): number;
	public encoder(value: StoredTx, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
		if (target === undefined) {
			// Size-compute pass.
			const txIdBytes = TXID.encode(value.txId);
			const packBytes = PACK.encode(value);
			const outputBytes = OUTPUTS.encode(value.outputs);
			const inputBytes = INPUTS.encode(value.inputs);

			const totalSize = txIdBytes.length + packBytes.length + outputBytes.length + inputBytes.length;

			const bytes = new Uint8Array(totalSize);
			let pos = 0;
			bytes.set(txIdBytes, pos);
			pos += txIdBytes.length;
			bytes.set(packBytes, pos);
			pos += packBytes.length;
			bytes.set(outputBytes, pos);
			pos += outputBytes.length;
			bytes.set(inputBytes, pos);
			return bytes;
		}

		offset = offset!;
		const start = offset;
		offset += TXID.encodeInto(value.txId, target, offset);
		offset += PACK.encodeInto(value, target, offset);
		offset += OUTPUTS.encodeInto(value.outputs, target, offset);
		offset += INPUTS.encodeInto(value.inputs, target, offset);
		return offset - start;
	}

	public decoder(data: Uint8Array, offset: number): [StoredTx, number] {
		let pos = offset;

		const [txId, txIdSize] = TXID.decode(data, pos);
		pos += txIdSize;
		const [{ locktime, version }, packSize] = PACK.decode(data, pos);
		pos += packSize;
		const [outputs, outputSize] = OUTPUTS.decode(data, pos);
		pos += outputSize;
		const [inputs, inputSize] = INPUTS.decode(data, pos);
		pos += inputSize;

		return [{ txId, locktime, version, outputs, inputs }, pos - offset];
	}

	/**
	 * Writes the tx directly into `target` at `offset` (no intermediate
	 * allocations) and returns each vout's and vin's tx-relative byte offset.
	 * Add `txPointer` (the blob offset where the tx starts) to each returned
	 * offset to get the absolute blob pointer for that output/input.
	 *
	 * Invariant: the number of bytes written equals the total encoded size.
	 */
	public encodeWithOffsets(value: StoredTx, target: Uint8Array, offset: number): StoredTxOffsets {
		const start = offset;
		offset += TXID.encodeInto(value.txId, target, offset);
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
