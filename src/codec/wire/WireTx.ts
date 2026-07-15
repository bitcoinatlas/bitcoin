import { ArrayCodec, Codec, Stride, StructCodec, U32LE } from "@nomadshiba/codec";
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

	public encoder(tx: T, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(tx: T, target: Uint8Array, offset: number): number;
	public encoder(tx: T, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
		const hasWitness = tx.witness.length > 0;

		if (target === undefined) {
			const preWitnessBytes = WireTxPreWitness.encode({ version: tx.version, hasWitness, inputs: tx.inputs, outputs: tx.outputs });
			const witnessBytes = hasWitness ? encodeWitness(tx.witness) : null;
			const postWitnessBytes = WireTxPostWitness.encode({ locktime: tx.locktime });

			const total = preWitnessBytes.length +
				(witnessBytes ? witnessBytes.length : 0) +
				postWitnessBytes.length;

			const result = new Uint8Array(total);
			let pos = 0;
			result.set(preWitnessBytes, pos);
			pos += preWitnessBytes.length;
			if (witnessBytes) {
				result.set(witnessBytes, pos);
				pos += witnessBytes.length;
			}
			result.set(postWitnessBytes, pos);
			return result;
		}

		offset = offset!;
		const start = offset;
		offset += WireTxPreWitness.encodeInto({ version: tx.version, hasWitness, inputs: tx.inputs, outputs: tx.outputs }, target, offset);
		if (hasWitness) offset += encodeWitnessInto(tx.witness, target, offset);
		offset += WireTxPostWitness.encodeInto({ locktime: tx.locktime }, target, offset);
		return offset - start;
	}

	public decoder(bytes: Uint8Array, offset: number): [T, number] {
		const [preWitness, preWitnessBytes] = WireTxPreWitness.decode(bytes, offset);
		let currentOffset = offset + preWitnessBytes;

		let witness: Uint8Array[][] = [];
		if (preWitness.hasWitness) {
			const [w, witnessBytes] = decodeWitness(
				bytes,
				currentOffset,
				preWitness.inputs.length,
			);
			witness = w;
			currentOffset += witnessBytes;
		}

		const [postWitness] = WireTxPostWitness.decode(bytes, currentOffset);
		currentOffset += 4;

		return [{
			version: preWitness.version,
			locktime: postWitness.locktime,
			inputs: preWitness.inputs,
			outputs: preWitness.outputs,
			witness,
		}, currentOffset - offset];
	}
}

// Encode witness: array of witness per input
function encodeWitness(witness: Uint8Array[][]): Uint8Array {
	let total = 0;
	for (const inputWitness of witness) {
		total += CompactSize.encode(inputWitness.length).length;
		for (const item of inputWitness) {
			total += CompactSize.encode(item.length).length + item.length;
		}
	}
	const result = new Uint8Array(total);
	encodeWitnessInto(witness, result, 0);
	return result;
}

function encodeWitnessInto(witness: Uint8Array[][], target: Uint8Array, offset: number): number {
	const start = offset;
	for (const inputWitness of witness) {
		offset += CompactSize.encodeInto(inputWitness.length, target, offset);
		for (const item of inputWitness) {
			offset += CompactSize.encodeInto(item.length, target, offset);
			target.set(item, offset);
			offset += item.length;
		}
	}
	return offset - start;
}

// Decode witness: array of witness per input
function decodeWitness(data: Uint8Array, offset: number, inputCount: number): [Uint8Array[][], number] {
	let currentOffset = offset;
	const witness: Uint8Array[][] = [];
	for (let i = 0; i < inputCount; i++) {
		const [nItems, nItemsBytes] = CompactSize.decode(data, currentOffset);
		currentOffset += nItemsBytes;
		const items: Uint8Array[] = [];
		for (let j = 0; j < nItems; j++) {
			const [itemLen, itemLenBytes] = CompactSize.decode(data, currentOffset);
			currentOffset += itemLenBytes;
			const item = data.subarray(currentOffset, currentOffset + itemLen);
			currentOffset += itemLen;
			items.push(item);
		}
		witness.push(items);
	}
	return [witness, currentOffset - offset];
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
