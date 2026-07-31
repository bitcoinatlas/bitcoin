import { ArrayCodec, Codec, VarInt } from "@nomadshiba/codec";
import { ChainStore } from "~/chain/ChainStore.ts";
import { LockTimeVersionPack } from "~/codec/stored/StoredLockTimeVersionPack.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/codec/stored/StoredTxOutput.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";
import { WireTxInput } from "~/codec/wire/WireTxInput.ts";
import { WireTxOutput } from "~/codec/wire/WireTxOutput.ts";

const PACK = LockTimeVersionPack;
const INPUTS = new ArrayCodec(StoredTxInput, { counter: VarInt });
const OUTPUTS = new ArrayCodec(StoredTxOutput, { counter: VarInt });

type Output =
	& Codec.InferOutput<typeof PACK>
	& { inputs: Codec.InferOutput<typeof INPUTS> }
	& { outputs: Codec.InferOutput<typeof OUTPUTS> };

type Input =
	& Codec.InferInput<typeof PACK>
	& { inputs: Codec.InferInput<typeof INPUTS> }
	& { outputs: Codec.InferInput<typeof OUTPUTS> };

export type StoredTxOffsets = { outputs: number[]; inputs: number[] };

export class StoredTxCodec extends Codec<Output, Input> {
	public readonly stride = { kind: "variable" } as const;

	/**
	 * Reconstruct the human-readable wire transaction from stored form: substitutes
	 * real prevout txids back in for the U48 pointers and expands pubkey pointers to
	 * full scripts. Unlike {@link toWireBytes} this returns the decoded object (what
	 * the API layer serializes), and only emits a segwit marker when at least one
	 * input actually carries witness data — so legacy txs round-trip to the right txid.
	 */
	// TODO: Move this to somewhere else
	toWire(storedTx: Output, chainStorage: ChainStore): Codec.InferInput<typeof WireTx> {
		const { version, locktime } = storedTx;

		let anyWitness = false;
		for (const input of storedTx.inputs) {
			if (input.witness.kind !== "none") {
				anyWitness = true;
				break;
			}
		}

		const inputs: WireTxInput[] = storedTx.inputs.map((input) => ({
			prevOut: { txId: chainStorage.getPrevOutTxId(input), output: input.prevOut.output },
			scriptSig: input.scriptSig,
			sequence: input.sequence,
		}));

		const outputs: WireTxOutput[] = storedTx.outputs.map((output) => {
			const [scriptPubKey] = chainStorage.stores.pubkey.getKey(output.scriptPubKey);
			const value = BigInt(output.value);
			return { value, scriptPubKey };
		});

		const witness: Uint8Array<ArrayBuffer>[][] = anyWitness ? storedTx.inputs.map((input) => input.witness.raw()) : [];

		return { version, locktime, inputs, outputs, witness };
	}

	public encoder(value: Input, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(value: Input, target: Uint8Array, offset: number): number;
	public encoder(value: Input, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
		if (target === undefined) {
			// Size-compute pass.
			const packBytes = PACK.encode(value);
			const outputBytes = OUTPUTS.encode(value.outputs);
			const inputBytes = INPUTS.encode(value.inputs);

			const totalSize = packBytes.length + outputBytes.length + inputBytes.length;

			const bytes = new Uint8Array(totalSize);
			let pos = 0;
			bytes.set(packBytes, pos);
			pos += packBytes.length;
			bytes.set(outputBytes, pos);
			pos += outputBytes.length;
			bytes.set(inputBytes, pos);
			return bytes;
		}

		offset = offset!;
		const start = offset;
		offset += PACK.encodeInto(value, target, offset);
		offset += OUTPUTS.encodeInto(value.outputs, target, offset);
		offset += INPUTS.encodeInto(value.inputs, target, offset);
		return offset - start;
	}

	public decoder(data: Uint8Array, offset: number): [Output, number] {
		let pos = offset;

		const [{ locktime, version }, packSize] = PACK.decode(data, pos);
		pos += packSize;
		const [outputs, outputSize] = OUTPUTS.decode(data, pos);
		pos += outputSize;
		const [inputs, inputSize] = INPUTS.decode(data, pos);
		pos += inputSize;

		return [{ locktime, version, outputs, inputs }, pos - offset];
	}

	/**
	 * Writes the tx directly into `target` at `offset` (no intermediate
	 * allocations) and returns each vout's and vin's tx-relative byte offset.
	 * Add `txPointer` (the blob offset where the tx starts) to each returned
	 * offset to get the absolute blob pointer for that output/input.
	 *
	 * Invariant: the number of bytes written equals the total encoded size.
	 */
	public encodeWithOffsets(value: Output, target: Uint8Array, offset: number): StoredTxOffsets {
		const start = offset;
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

export type StoredTx = Codec.InferOutput<typeof StoredTx>;
export const StoredTx = new StoredTxCodec();
