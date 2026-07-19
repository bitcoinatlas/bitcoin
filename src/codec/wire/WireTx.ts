import { ArrayCodec, Codec, Stride, StructCodec, U32LE } from "@nomadshiba/codec";
import { LockTime } from "~/codec/LockTime.ts";
import { CompactSize } from "~/codec/primitives/CompactSize.ts";
import { WireSegwitMarker } from "~/codec/wire/WireSegwitMarker.ts";
import { WireTxInput } from "~/codec/wire/WireTxInput.ts";
import { WireTxOutput } from "~/codec/wire/WireTxOutput.ts";
import { sha256d } from "~/libs/hashes/sha256d.ts";

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

type WireTxIn = {
	version: number;
	locktime: LockTime;
	inputs: WireTxInput[];
	outputs: WireTxOutput[];
	witness: Uint8Array[][];
};

export type WireTx = {
	txId: Uint8Array;
	wtxId: Uint8Array;
	version: number;
	locktime: LockTime;
	inputs: WireTxInput[];
	outputs: WireTxOutput[];
	witness: Uint8Array[][];
};

class WireTxCodec extends Codec<WireTx, WireTxIn> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	public encoder(tx: WireTxIn, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(tx: WireTxIn, target: Uint8Array, offset: number): number;
	public encoder(tx: WireTxIn, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
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

	public decoder(bytes: Uint8Array, offset: number): [WireTx, number] {
		const start = offset;
		const [preWitness, preWitnessBytes] = WireTxPreWitness.decode(bytes, offset);
		let cur = offset + preWitnessBytes;

		const markerLen = preWitness.hasWitness ? 2 : 0;
		const bodyStart = offset + 4 + markerLen; // inputs+outputs start (after version + marker)
		const bodyEnd = offset + preWitnessBytes; // inputs+outputs end

		let witness: Uint8Array[][] = [];
		if (preWitness.hasWitness) {
			const [w, wLen] = decodeWitness(bytes, cur, preWitness.inputs.length);
			witness = w;
			cur += wLen;
		}

		const locktimeStart = cur;
		const [postWitness] = WireTxPostWitness.decode(bytes, cur);
		cur += 4; // locktime is always 4 bytes

		// wtxid: hash the full consumed range (marker + witness included)
		const wtxId = sha256d(bytes.subarray(start, cur));

		// txid: legacy serialization = version ++ body ++ locktime
		let txId: Uint8Array;
		if (!preWitness.hasWitness) {
			txId = wtxId; // no marker, no witness → identical & contiguous
		} else {
			const bodyLen = bodyEnd - bodyStart;
			const legacy = new Uint8Array(4 + bodyLen + 4);
			legacy.set(bytes.subarray(offset, offset + 4), 0);
			legacy.set(bytes.subarray(bodyStart, bodyEnd), 4);
			legacy.set(bytes.subarray(locktimeStart, locktimeStart + 4), 4 + bodyLen);
			txId = sha256d(legacy);
		}

		return [{
			version: preWitness.version,
			locktime: postWitness.locktime,
			inputs: preWitness.inputs,
			outputs: preWitness.outputs,
			witness,
			txId,
			wtxId,
		}, cur - start];
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

export const WireTx = new WireTxCodec();
