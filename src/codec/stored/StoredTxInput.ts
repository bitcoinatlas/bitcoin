import { BytesCodec, Codec, Stride, U32LE, VarInt } from "@nomadshiba/codec";
import { SequenceLock, SequenceLockCodec } from "~/codec/SequenceLock.ts";
import { StoredPointer } from "~/codec/stored/StoredPointer.ts";
import { StoredWitness } from "~/codec/stored/StoredWitness.ts";
import { COINBASE_VOUT } from "~/constants.ts";

export type PrevOut = {
	txId:
		| { kind: "pointer"; value: number }
		| { kind: "raw"; value: Uint8Array }
		| { kind: "coinbase"; value?: undefined };
	vout: number;
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
 * bits 0-1 : prevOut kind   0=resolved (pointer), 1=raw (txid), 2=coinbase
 * bits 2-3 : sequence tag    0=0xFFFFFFFF, 1=0xFFFFFFFE, 2=0xFFFFFFFD (RBF),
 *                            3=explicit u32 follows
 * bits 4-7 : spare
 *
 * -- prevOut payload (by kind) --
 *   resolved: 6-byte StoredPointer (u48) + VarInt vout
 *   raw:      32-byte txid + VarInt vout
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
// bits 0-1: prevOut kind   0=resolved, 1=raw, 2=coinbase
// bits 2-3: sequence tag    0=0xFFFFFFFF, 1=0xFFFFFFFE, 2=0xFFFFFFFD, 3=explicit u32 follows
// bits 4-7: spare
const PREVOUT_MASK = 0b0000_0011;
const SEQ_SHIFT = 2;
const SEQ_MASK = 0b0000_1100;

const PREVOUT_RESOLVED = 0;
const PREVOUT_RAW = 1;
const PREVOUT_COINBASE = 2;

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

		if (input.prevOut.txId.kind === "pointer") {
			prevOutKind = PREVOUT_RESOLVED;
			prevOutSize = 6 + VarInt.encode(input.prevOut.vout).length;
		} else if (input.prevOut.txId.kind === "raw") {
			prevOutKind = PREVOUT_RAW;
			prevOutSize = 32 + VarInt.encode(input.prevOut.vout).length;
		} else {
			prevOutKind = PREVOUT_COINBASE;
			prevOutSize = 0;
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

		let prevOutKind: number;
		if (input.prevOut.txId.kind === "pointer") prevOutKind = PREVOUT_RESOLVED;
		else if (input.prevOut.txId.kind === "raw") prevOutKind = PREVOUT_RAW;
		else prevOutKind = PREVOUT_COINBASE;

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
			offset += StoredPointer.encodeInto(input.prevOut.txId.value, target, offset);
			offset += VarInt.encodeInto(input.prevOut.vout, target, offset);
		} else if (input.prevOut.txId.kind === "raw") {
			target.set(input.prevOut.txId.value, offset);
			offset += 32;
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

	override size(input: StoredTxInput): number {
		const seqU32 = SequenceLockCodec.toU32(input.sequence) >>> 0;
		const seqExplicit = sequenceTagForU32(seqU32) === SEQ_EXPLICIT;

		let prevOutSize: number;
		if (input.prevOut.txId.kind === "pointer") {
			prevOutSize = StoredPointer.stride.size + VarInt.size(input.prevOut.vout);
		} else if (input.prevOut.txId.kind === "raw") {
			prevOutSize = 32 + VarInt.size(input.prevOut.vout);
		} else {
			prevOutSize = 0;
		}

		return 1 + prevOutSize + (seqExplicit ? 4 : 0) +
			scriptSigCodec.size(input.scriptSig) + StoredWitness.size(input.witness);
	}

	decode(data: Uint8Array): [StoredTxInput, number] {
		let offset = 0;

		const tagByte = data[offset]!;
		offset += 1;

		const prevOutKind = tagByte & PREVOUT_MASK;
		const seqTag = (tagByte & SEQ_MASK) >>> SEQ_SHIFT;

		let txId: PrevOut["txId"];
		let vout: number;

		if (prevOutKind === PREVOUT_RESOLVED) {
			const [pointer] = StoredPointer.decode(data.subarray(offset));
			offset += StoredPointer.stride.size;
			let voutOffset;
			[vout, voutOffset] = VarInt.decode(data.subarray(offset));
			offset += voutOffset;
			txId = { kind: "pointer", value: pointer };
		} else if (prevOutKind === PREVOUT_RAW) {
			txId = { kind: "raw", value: data.subarray(offset, offset + 32) };
			offset += 32;
			let voutOffset;
			[vout, voutOffset] = VarInt.decode(data.subarray(offset));
			offset += voutOffset;
		} else if (prevOutKind === PREVOUT_COINBASE) {
			txId = { kind: "coinbase" };
			vout = COINBASE_VOUT;
		} else {
			throw new Error("unknown prevOut kind");
		}

		// Sequence: either from tag or explicit u32
		let seqU32 = sequenceU32ForTag(seqTag);
		if (seqU32 === null) {
			seqU32 = U32LE.decode(data.subarray(offset))[0] >>> 0;
			offset += 4;
		}

		const [scriptSig, scriptSigBytes] = scriptSigCodec.decode(data.subarray(offset));
		offset += scriptSigBytes;

		const [witness, witnessBytes] = StoredWitness.decode(data.subarray(offset));
		offset += witnessBytes;

		const input: StoredTxInput = {
			prevOut: { txId, vout },
			scriptSig,
			sequence: SequenceLockCodec.fromU32(seqU32),
			witness,
		};

		return [input, offset];
	}
}

export const StoredTxInput = new StoredTxInputCodec();
