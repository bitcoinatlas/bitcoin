import { ArrayCodec, Codec, Stride, StructCodec, U32LE } from "@nomadshiba/codec";
import { concat } from "@std/bytes";
import { CompactSize } from "~/codec/primitives/CompactSize.ts";
import { LockTime } from "~/codec/LockTime.ts";
import { WireTxInput } from "~/codec/wire/WireTxInput.ts";
import { WireTxOutput } from "~/codec/wire/WireTxOutput.ts";
import { WireSegwitMarker } from "~/codec/wire/WireSegwitMarker.ts";
import { sha256 } from "@noble/hashes/sha2";

// WireTx = version + [segwit_marker] + inputs + outputs + [witness] + locktime
// Note: witness is NOT inside inputs in wire format, it's at tx level
// Segwit marker is 0x00 0x01, present only when witness exists

// Part 1: Everything before witness (including marker detection)
const WireTxPreWitness = new StructCodec({
	version: U32LE,
	hasWitness: WireSegwitMarker,
	inputs: new ArrayCodec(WireTxInput, { counter: CompactSize }),
	outputs: new ArrayCodec(WireTxOutput, { counter: CompactSize }),
});

// Part 2: Everything after witness (just locktime)
const WireTxPostWitness = new StructCodec({
	locktime: LockTime,
});

type T = {
	version: number;
	locktime: LockTime;
	inputs: WireTxInput[];
	outputs: WireTxOutput[];
	witness: Uint8Array[][];
};

class WireTxCodec extends Codec<T> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	encode(tx: T): Uint8Array<ArrayBuffer> {
		const hasWitness = tx.witness.length > 0;

		// Encode pre-witness data
		const preWitness = WireTxPreWitness.encode({
			version: tx.version,
			hasWitness,
			inputs: tx.inputs,
			outputs: tx.outputs,
		});

		const chunks: Uint8Array[] = [preWitness];

		// Witness (if present)
		if (hasWitness) {
			chunks.push(encodeWitness(tx.witness));
		}

		// Locktime
		chunks.push(WireTxPostWitness.encode({
			locktime: tx.locktime,
		}));

		return concat(chunks);
	}

	decode(bytes: Uint8Array): [T, number] {
		// Decode pre-witness data
		const [preWitness, preWitnessBytes] = WireTxPreWitness.decode(bytes);
		let offset = preWitnessBytes;

		// Decode witness (if present)
		let witness: Uint8Array[][] = [];
		if (preWitness.hasWitness) {
			const [w, witnessBytes] = decodeWitness(
				bytes.subarray(offset),
				preWitness.inputs.length,
			);
			witness = w;
			offset += witnessBytes;
		}

		// Decode locktime
		const [postWitness] = WireTxPostWitness.decode(bytes.subarray(offset));
		offset += 4; // locktime is 4 bytes

		return [{
			version: preWitness.version,
			locktime: postWitness.locktime,
			inputs: preWitness.inputs,
			outputs: preWitness.outputs,
			witness,
		}, offset];
	}
}

// Encode witness: array of witness per input
function encodeWitness(witness: Uint8Array[][]): Uint8Array {
	const chunks: Uint8Array[] = [];
	for (const inputWitness of witness) {
		chunks.push(CompactSize.encode(inputWitness.length));
		for (const item of inputWitness) {
			chunks.push(CompactSize.encode(item.length));
			chunks.push(item);
		}
	}
	return concat(chunks);
}

// Decode witness: array of witness per input
function decodeWitness(data: Uint8Array, inputCount: number): [Uint8Array[][], number] {
	let offset = 0;
	const witness: Uint8Array[][] = [];
	for (let i = 0; i < inputCount; i++) {
		const [nItems, nItemsBytes] = CompactSize.decode(data.subarray(offset));
		offset += nItemsBytes;
		const items: Uint8Array[] = [];
		for (let j = 0; j < nItems; j++) {
			const [itemLen, itemLenBytes] = CompactSize.decode(data.subarray(offset));
			offset += itemLenBytes;
			const item = data.subarray(offset, offset + itemLen);
			offset += itemLen;
			items.push(item);
		}
		witness.push(items);
	}
	return [witness, offset];
}

export type WireTx = Codec.InferOutput<typeof WireTx>;
export const WireTx = new WireTxCodec().transform((value, bytes) => {
	// txId must be computed from non-witness serialization (BIP141)
	// even if the bytes include witness data (segwit tx)
	const hasWitness = value.witness.length > 0;
	let hashBytes: Uint8Array;
	if (!hasWitness) {
		hashBytes = bytes;
	} else {
		// Re-encode without witness to get the non-witness serialization
		hashBytes = new WireTxCodec().encode({ ...value, witness: [] });
	}
	return {
		txId: sha256(sha256(hashBytes)),
		...value,
	};
});
