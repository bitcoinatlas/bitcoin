import { ArrayCodec, Codec, StructCodec, StructInput, StructOutput, VarInt } from "@nomadshiba/codec";
import type { _ } from "@project/utils";
import { LockTimeAndVersionPack } from "~/stored/StoredLockTimeVersionPack.ts";
import { StoredTxInput } from "~/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/stored/StoredTxOutput.ts";
import { WireTx } from "~/wire/WireTx.ts";
import { WireTxInput } from "~/wire/WireTxInput.ts";
import { WireTxOutput } from "~/wire/WireTxOutput.ts";

type Output = StructOutput<typeof Shape>;
type Input = StructInput<typeof Shape>;
const Shape = {
	lockTimeAndVersionPack: LockTimeAndVersionPack,
	inputs: new ArrayCodec(StoredTxInput, { counter: VarInt }),
	outputs: new ArrayCodec(StoredTxOutput, { counter: VarInt }),
};

export type StoredTxOffsets = { outputs: number[]; inputs: number[] };

export class StoredTxCodec extends StructCodec<typeof Shape> {
	public constructor() {
		super(Shape);
	}

	// TODO: Move this to somewhere else, also we only need bytes here, normally if Router was more flexable
	public toWire(storedTx: Output, chainStorage: _): Codec.InferInput<typeof WireTx> {
		const { version, locktime } = storedTx.lockTimeAndVersionPack;

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

	public encodeWithOffsets(value: Output, target: Uint8Array, offset: number): StoredTxOffsets {
		const start = offset;
		offset += LockTimeAndVersionPack.encodeInto(value.lockTimeAndVersionPack, target, offset);

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
