import { ArrayCodec, Codec, StructCodec, StructInput, StructOutput, VarInt } from "@nomadshiba/codec";
import { LockTimeAndVersionPack } from "~/stored/StoredLockTimeVersionPack.ts";
import { StoredTxInput } from "~/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/stored/StoredTxOutput.ts";

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
