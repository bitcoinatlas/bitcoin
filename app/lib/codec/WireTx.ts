import { ArrayCodec, Codec, StructCodec, U32LE } from "@nomadshiba/codec";
import { compactSize } from "~/lib/codec/primitives.ts";
import { TimeLock, timeLock } from "./TimeLock.ts";
import { WireTxInput } from "./WireTxInput.ts";
import { WireTxOutput } from "./WireTxOutput.ts";

// WireTx = version + [segwit_marker] + inputs + outputs + [witness] + locktime
// Note: witness is NOT inside inputs in wire format, it's at tx level
// Segwit marker is 0x00 0x01, present only when witness exists

export type WireTx = {
	version: number;
	locktime: TimeLock;
	inputs: WireTxInput[];
	output: WireTxOutput[];
	witness: Uint8Array[][];
};

// Segwit marker codec: encodes 0x00 0x01, decodes by peeking
class SegwitMarkerCodec extends Codec<boolean> {
	readonly stride = -1;

	encode(hasWitness: boolean): Uint8Array {
		return hasWitness ? Uint8Array.of(0x00, 0x01) : new Uint8Array(0);
	}

	decode(data: Uint8Array): [boolean, number] {
		if (data.length >= 2 && data[0] === 0x00 && data[1] === 0x01) {
			return [true, 2];
		}
		return [false, 0];
	}
}

const SegwitMarker = new SegwitMarkerCodec();

// Part 1: Everything before witness (including marker detection)
const WireTxPreWitness = new StructCodec({
	version: U32LE,
	hasWitness: SegwitMarker,
	inputs: new ArrayCodec({ codec: WireTxInput, countCodec: compactSize }),
	outputs: new ArrayCodec({ codec: WireTxOutput, countCodec: compactSize }),
});

// Part 2: Everything after witness (just locktime)
const WireTxPostWitness = new StructCodec({
	locktime: timeLock,
});

// Wire format witness: array of witness per input
class WireWitnessCodec extends Codec<Uint8Array[][]> {
	readonly stride = -1;

	encode(witness: Uint8Array[][]): Uint8Array {
		const chunks: Uint8Array[] = [];
		for (const inputWitness of witness) {
			chunks.push(compactSize.encode(inputWitness.length));
			for (const item of inputWitness) {
				chunks.push(compactSize.encode(item.length));
				chunks.push(item);
			}
		}
		return concatBytes(chunks);
	}

	decode(data: Uint8Array, inputCount: number): [Uint8Array[][], number] {
		let offset = 0;
		const witness: Uint8Array[][] = [];
		for (let i = 0; i < inputCount; i++) {
			const [nItems, nItemsBytes] = compactSize.decode(data.subarray(offset));
			offset += nItemsBytes;
			const items: Uint8Array[] = [];
			for (let j = 0; j < nItems; j++) {
				const [itemLen, itemLenBytes] = compactSize.decode(data.subarray(offset));
				offset += itemLenBytes;
				const item = data.subarray(offset, offset + itemLen);
				offset += itemLen;
				items.push(item);
			}
			witness.push(items);
		}
		return [witness, offset];
	}
}

const wireWitness = new WireWitnessCodec();

// Helper to concatenate Uint8Arrays
function concatBytes(chunks: Uint8Array[]): Uint8Array {
	let totalLength = 0;
	for (const chunk of chunks) totalLength += chunk.length;
	const result = new Uint8Array(totalLength);
	let offset = 0;
	for (const chunk of chunks) {
		result.set(chunk, offset);
		offset += chunk.length;
	}
	return result;
}

export class WireTxCodec extends Codec<WireTx> {
	readonly stride = -1;

	encode(tx: WireTx): Uint8Array {
		const hasWitness = tx.witness.length > 0;

		// Encode pre-witness data
		const preWitness = WireTxPreWitness.encode({
			version: tx.version,
			hasWitness,
			inputs: tx.inputs,
			outputs: tx.output,
		});

		const chunks: Uint8Array[] = [preWitness];

		// Witness (if present)
		if (hasWitness) {
			chunks.push(wireWitness.encode(tx.witness));
		}

		// Locktime
		chunks.push(WireTxPostWitness.encode({ locktime: tx.locktime }));

		return concatBytes(chunks);
	}

	decode(bytes: Uint8Array): [WireTx, number] {
		// Decode pre-witness data
		const [preWitness, preWitnessBytes] = WireTxPreWitness.decode(bytes);
		let offset = preWitnessBytes;

		// Decode witness (if present)
		let witness: Uint8Array[][] = [];
		if (preWitness.hasWitness) {
			const [w, witnessBytes] = wireWitness.decode(
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
			output: preWitness.outputs,
			witness,
		}, offset];
	}
}

export const WireTx = new WireTxCodec();
