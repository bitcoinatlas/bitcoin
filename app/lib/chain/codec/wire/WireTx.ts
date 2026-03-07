import { ArrayCodec, Codec, StructCodec, U32LE } from "@nomadshiba/codec";
import { CompactSize } from "~/lib/codec/primitives.ts";
import { TimeLock } from "~/lib/chain/codec/TimeLock.ts";
import { WireTxInput } from "~/lib/chain/codec/wire/WireTxInput.ts";
import { WireTxOutput } from "~/lib/chain/codec/wire/WireTxOutput.ts";
import { WireSegwitMarker } from "~/lib/chain/codec/wire/WireSegwitMarker.ts";

// WireTx = version + [segwit_marker] + inputs + outputs + [witness] + locktime
// Note: witness is NOT inside inputs in wire format, it's at tx level
// Segwit marker is 0x00 0x01, present only when witness exists

export type WireTx = {
	version: number;
	locktime: TimeLock;
	inputs: WireTxInput[];
	outputs: WireTxOutput[];
	witness: Uint8Array[][];
};

// Part 1: Everything before witness (including marker detection)
const WireTxPreWitness = new StructCodec({
	version: U32LE,
	hasWitness: WireSegwitMarker,
	inputs: new ArrayCodec(WireTxInput, { countCodec: CompactSize }),
	outputs: new ArrayCodec(WireTxOutput, { countCodec: CompactSize }),
});

// Part 2: Everything after witness (just locktime)
const WireTxPostWitness = new StructCodec({
	locktime: TimeLock,
});

export class WireTxCodec extends Codec<WireTx> {
	readonly stride = -1;

	encode(tx: WireTx): Uint8Array {
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

		return Uint8Array.from(chunks);
	}

	decode(bytes: Uint8Array): [WireTx, number] {
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

// Uppercase singleton instance (codec convention)
export const WireTx = new WireTxCodec();

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
	return Uint8Array.from(chunks);
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
