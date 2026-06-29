import { BytesCodec, Codec, Stride, U32LE, VarInt } from "@nomadshiba/codec";
import { SequenceLock, SequenceLockCodec } from "~/codec/SequenceLock.ts";
import { StoredTxPointer } from "~/codec/stored/StoredTxPointer.ts";
import { StoredWitness } from "~/codec/stored/StoredWitness.ts";
import { COINBASE_VOUT } from "~/constants.ts";

export type PrevOut = {
	txId:
		| { kind: "pointer"; value: StoredTxPointer }
		| { kind: "coinbase"; value?: undefined };
	vout: Codec.InferOutput<typeof VarInt>;
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
 * Field order keeps the single fixed-ish tag byte first, the prevOut payload
 * (fixed per kind) next, the conditional explicit sequence after it, then the
 * two variable-length tails (scriptSig, witness) last. Single forward cursor,
 * no padding.
 *
 * -- 1-byte tag --
 * bit 0    : prevOut kind   0=resolved (pointer), 1=coinbase
 * bits 1-2 : sequence tag    0=0xFFFFFFFF, 1=0xFFFFFFFE, 2=0xFFFFFFFD (RBF),
 *                            3=explicit u32 follows
 * bits 3-7 : spare
 *
 * -- prevOut payload (by kind) --
 *   resolved: 6-byte StoredTxPointer (u48) + VarInt vout
 *   coinbase: none
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
// bit 0:    prevOut kind   0=resolved, 1=coinbase
// bits 1-2: sequence tag   0=0xFFFFFFFF, 1=0xFFFFFFFE, 2=0xFFFFFFFD, 3=explicit u32 follows
// bits 3-7: spare
const PREVOUT_MASK = 0b0000_0001;
const SEQ_SHIFT = 1;
const SEQ_MASK = 0b0000_0110;

const PREVOUT_RESOLVED = 0;
const PREVOUT_COINBASE = 1;

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

	encode(input: StoredTxInput): Uint8Array<ArrayBuffer> {
		// Size-compute pass then single allocation.
		const seqU32 = SequenceLockCodec.toU32(input.sequence) >>> 0;
		const seqTag = sequenceTagForU32(seqU32);
		const seqExplicit = seqTag === SEQ_EXPLICIT;

		const scriptSigEncoded = scriptSigCodec.encode(input.scriptSig);
		const witnessEncoded = StoredWitness.encode(input.witness);

		let prevOutKind: number;
		let prevOutSize: number;

		const { kind } = input.prevOut.txId;
		if (kind === "pointer") {
			prevOutKind = PREVOUT_RESOLVED;
			prevOutSize = StoredTxPointer.stride.size + VarInt.encode(input.prevOut.vout).length;
		} else if (kind === "coinbase") {
			prevOutKind = PREVOUT_COINBASE;
			prevOutSize = 0;
		} else {
			throw new Error(`unknown txid kind, ${kind satisfies never}`);
		}

		const totalLength = 1 + prevOutSize + (seqExplicit ? 4 : 0) +
			scriptSigEncoded.length + witnessEncoded.length;
		const result = new Uint8Array(totalLength);
		this.writeInto(input, result, 0, prevOutKind, seqU32, seqTag, seqExplicit, scriptSigEncoded, witnessEncoded);
		return result;
	}

	public override encodeInto(input: StoredTxInput, target: Uint8Array, offset: number = 0): number {
		const seqU32 = SequenceLockCodec.toU32(input.sequence) >>> 0;
		const seqTag = sequenceTagForU32(seqU32);
		const seqExplicit = seqTag === SEQ_EXPLICIT;

		const scriptSigEncoded = scriptSigCodec.encode(input.scriptSig);
		const witnessEncoded = StoredWitness.encode(input.witness);

		const prevOutKind = input.prevOut.txId.kind === "pointer" ? PREVOUT_RESOLVED : PREVOUT_COINBASE;

		return this.writeInto(input, target, offset, prevOutKind, seqU32, seqTag, seqExplicit, scriptSigEncoded, witnessEncoded);
	}

	private writeInto(
		input: StoredTxInput,
		target: Uint8Array,
		offset: number,
		prevOutKind: number,
		seqU32: number,
		seqTag: number,
		seqExplicit: boolean,
		scriptSigEncoded: Uint8Array,
		witnessEncoded: Uint8Array,
	): number {
		const start = offset;
		const tagByte = (prevOutKind & PREVOUT_MASK) | ((seqTag << SEQ_SHIFT) & SEQ_MASK);
		target[offset++] = tagByte;

		if (input.prevOut.txId.kind === "pointer") {
			offset += StoredTxPointer.encodeInto(input.prevOut.txId.value, target, offset);
			offset += VarInt.encodeInto(input.prevOut.vout, target, offset);
		}
		// coinbase: no payload

		if (seqExplicit) {
			offset += U32LE.encodeInto(seqU32, target, offset);
		}

		target.set(scriptSigEncoded, offset);
		offset += scriptSigEncoded.length;

		target.set(witnessEncoded, offset);
		offset += witnessEncoded.length;

		return offset - start;
	}

	decodeFrom(data: Uint8Array, offset: number): [StoredTxInput, number] {
		let currentOffset = offset;

		const tagByte = data[currentOffset]!;
		currentOffset += 1;

		const prevOutKind = tagByte & PREVOUT_MASK;
		const seqTag = (tagByte & SEQ_MASK) >>> SEQ_SHIFT;

		let txId: PrevOut["txId"];
		let vout: number;

		if (prevOutKind === PREVOUT_RESOLVED) {
			const [pointer] = StoredTxPointer.decodeFrom(data, currentOffset);
			currentOffset += StoredTxPointer.stride.size;
			let voutOffset;
			[vout, voutOffset] = VarInt.decodeFrom(data, currentOffset);
			currentOffset += voutOffset;
			txId = { kind: "pointer", value: pointer };
		} else {
			txId = { kind: "coinbase" };
			vout = COINBASE_VOUT;
		}

		let seqU32 = sequenceU32ForTag(seqTag);
		if (seqU32 === null) {
			seqU32 = U32LE.decodeFrom(data, currentOffset)[0] >>> 0;
			currentOffset += 4;
		}

		const [scriptSig, scriptSigBytes] = scriptSigCodec.decodeFrom(data, currentOffset);
		currentOffset += scriptSigBytes;

		const [witness, witnessBytes] = StoredWitness.decodeFrom(data, currentOffset);
		currentOffset += witnessBytes;

		const input: StoredTxInput = {
			prevOut: { txId, vout },
			scriptSig,
			sequence: SequenceLockCodec.fromU32(seqU32),
			witness,
		};

		return [input, currentOffset - offset];
	}
}

export const StoredTxInput = new StoredTxInputCodec();
