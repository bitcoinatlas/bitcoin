import { BytesCodec, Codec, Stride, U32, VarInt } from "@nomadshiba/codec";
import { SequenceLock, SequenceLockCodec } from "~/codec/SequenceLock.ts";
import { StoredPrevOutTxId } from "~/codec/stored/StoredPrevOutTxId.ts";
import { StoredWitness } from "~/codec/stored/StoredWitness.ts";
import { COINBASE_VOUT } from "~/constants.ts";

export type PrevOut = {
	txId: StoredPrevOutTxId;
	output: VarInt;
};

export type StoredTxInput = {
	prevOut: PrevOut;
	scriptSig: Uint8Array;
	sequence: SequenceLock;
	witness: Uint8Array[];
};

/**
 * StoredTxInput binary layout
 *
 * -- prevOut txId (ALWAYS FIRST, fixed 6-byte u48 slot) --
 *   StoredPrevOutTxId encoding. Sits at the very start of the input, so an
 *   input's patch offset is simply the input's own offset.
 *
 * -- vout (conditional) --
 *   VarInt, present ONLY when the txId slot decodes to a pointer.
 *   Coinbase stores no vout (implied COINBASE_VOUT).
 *
 * -- 1-byte tag --
 * bits 0-1 : sequence tag    0=0xFFFFFFFF, 1=0xFFFFFFFE, 2=0xFFFFFFFD (RBF),
 *                            3=explicit u32 follows
 * bits 2-7 : spare
 *
 * -- sequence (conditional) --
 *   present ONLY when sequence tag is 3 (explicit): 4-byte u32.
 *   The three common constants are encoded in the tag and store 0 bytes here.
 *
 * -- scriptSig (variable) --
 *   length-prefixed bytes
 *
 * -- witness (variable, LAST) --
 *   StoredWitness encoding
 */

// Use BytesCodec for scriptSig length prefix
const scriptSigCodec = new BytesCodec();

// --- Tag byte layout ---
// bits 0-1: sequence tag   0=0xFFFFFFFF, 1=0xFFFFFFFE, 2=0xFFFFFFFD, 3=explicit u32 follows
// bits 2-7: spare
const SEQ_SHIFT = 0;
const SEQ_MASK = 0b0000_0011;

const SEQ_FINAL = 0; // 0xFFFFFFFF
const SEQ_FE = 1; // 0xFFFFFFFE
const SEQ_FD = 2; // 0xFFFFFFFD (RBF)
const SEQ_EXPLICIT = 3;

const SEQ_VALUE_FINAL = 0xffffffff;
const SEQ_VALUE_FE = 0xfffffffe;
const SEQ_VALUE_FD = 0xfffffffd;

function sequenceTagForU32(seq: number): number {
	switch (seq >>> 0) {
		case SEQ_VALUE_FINAL:
			return SEQ_FINAL;
		case SEQ_VALUE_FE:
			return SEQ_FE;
		case SEQ_VALUE_FD:
			return SEQ_FD;
		default:
			return SEQ_EXPLICIT;
	}
}

function sequenceU32ForTag(tag: number): number | null {
	switch (tag) {
		case SEQ_FINAL:
			return SEQ_VALUE_FINAL;
		case SEQ_FE:
			return SEQ_VALUE_FE;
		case SEQ_FD:
			return SEQ_VALUE_FD;
		default:
			return null; // explicit: read 4 bytes
	}
}

// StoredTxInput codec that decodes to plain TxInput data
export class StoredTxInputCodec extends Codec<StoredTxInput> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	encoder(input: StoredTxInput, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	encoder(input: StoredTxInput, target: Uint8Array, offset: number): number;
	encoder(input: StoredTxInput, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
		const seqU32 = SequenceLockCodec.toU32(input.sequence) >>> 0;
		const seqTag = sequenceTagForU32(seqU32);
		const seqExplicit = seqTag === SEQ_EXPLICIT;

		const scriptSigEncoded = scriptSigCodec.encode(input.scriptSig);
		const witnessEncoded = StoredWitness.encode(input.witness);

		if (target === undefined) {
			// Size-compute pass then single allocation.
			const outputSize = input.prevOut.txId.kind === "pointer" ? VarInt.encode(input.prevOut.output).length : 0;

			const totalLength = StoredPrevOutTxId.stride.size + outputSize + 1 + (seqExplicit ? 4 : 0) +
				scriptSigEncoded.length + witnessEncoded.length;
			const result = new Uint8Array(totalLength);
			this.writeInto(input, result, 0, seqU32, seqTag, seqExplicit, scriptSigEncoded, witnessEncoded);
			return result;
		}

		return this.writeInto(input, target, offset!, seqU32, seqTag, seqExplicit, scriptSigEncoded, witnessEncoded);
	}

	private writeInto(
		input: StoredTxInput,
		target: Uint8Array,
		offset: number,
		seqU32: number,
		seqTag: number,
		seqExplicit: boolean,
		scriptSigEncoded: Uint8Array,
		witnessEncoded: Uint8Array,
	): number {
		const start = offset;

		offset += StoredPrevOutTxId.encodeInto(input.prevOut.txId, target, offset);
		if (input.prevOut.txId.kind === "pointer") {
			offset += VarInt.encodeInto(input.prevOut.output, target, offset);
		}

		const tagByte = (seqTag << SEQ_SHIFT) & SEQ_MASK;
		target[offset++] = tagByte;

		if (seqExplicit) {
			offset += U32.encodeInto(seqU32, target, offset);
		}

		target.set(scriptSigEncoded, offset);
		offset += scriptSigEncoded.length;

		target.set(witnessEncoded, offset);
		offset += witnessEncoded.length;

		return offset - start;
	}

	decoder(data: Uint8Array, offset: number): [StoredTxInput, number] {
		let currentOffset = offset;

		const [txId, txIdBytes] = StoredPrevOutTxId.decode(data, currentOffset);
		currentOffset += txIdBytes;

		let output: number;
		if (txId.kind === "pointer") {
			let outputSize: number;
			[output, outputSize] = VarInt.decode(data, currentOffset);
			currentOffset += outputSize;
		} else {
			output = COINBASE_VOUT;
		}

		const tagByte = data[currentOffset]!;
		currentOffset += 1;

		const seqTag = (tagByte & SEQ_MASK) >>> SEQ_SHIFT;

		let seqU32 = sequenceU32ForTag(seqTag);
		if (seqU32 === null) {
			seqU32 = U32.decode(data, currentOffset)[0] >>> 0;
			currentOffset += 4;
		}

		const [scriptSig, scriptSigBytes] = scriptSigCodec.decode(data, currentOffset);
		currentOffset += scriptSigBytes;

		const [witness, witnessBytes] = StoredWitness.decode(data, currentOffset);
		currentOffset += witnessBytes;

		const input: StoredTxInput = {
			prevOut: { txId, output: output },
			sequence: SequenceLockCodec.fromU32(seqU32),
			scriptSig,
			witness,
		};

		return [input, currentOffset - offset];
	}
}

export const StoredTxInput = new StoredTxInputCodec();
