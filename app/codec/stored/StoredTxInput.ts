import { BytesCodec, Codec, Stride, U32LE, VarInt } from "@nomadshiba/codec";
import { COINBASE_VOUT } from "~/constants.ts";
import { OutPoint, TxInput } from "~/chain/TxInput.ts";
import { SequenceLockCodec } from "~/codec/SequenceLock.ts";
import { StoredPointer } from "~/codec/stored/StoredPointer.ts";
import { StoredWitness } from "~/codec/stored/StoredWitness.ts";

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
export class StoredTxInputCodec extends Codec<TxInput> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	encode(input: TxInput): Uint8Array<ArrayBuffer> {
		const data = input;

		// Resolve sequence to its raw u32 and decide if it needs an explicit field
		const seqU32 = SequenceLockCodec.toU32(data.sequence) >>> 0;
		const seqTag = sequenceTagForU32(seqU32);
		const seqExplicit = seqTag === SEQ_EXPLICIT;
		const seqBytes = seqExplicit ? 4 : 0;

		let prevOutKind: number;
		let prevOutPayload: Uint8Array;

		if (data.prevOut.txId.kind === "pointer") {
			prevOutKind = PREVOUT_RESOLVED;
			const voutBytes = VarInt.encode(data.prevOut.vout);
			prevOutPayload = new Uint8Array(6 + voutBytes.length);
			prevOutPayload.set(StoredPointer.encode(data.prevOut.txId.value), 0);
			prevOutPayload.set(voutBytes, 6);
		} else if (data.prevOut.txId.kind === "raw") {
			prevOutKind = PREVOUT_RAW;
			const voutBytes = VarInt.encode(data.prevOut.vout);
			prevOutPayload = new Uint8Array(32 + voutBytes.length);
			prevOutPayload.set(data.prevOut.txId.value, 0);
			prevOutPayload.set(voutBytes, 32);
		} else if (data.prevOut.txId.kind === "coinbase") {
			prevOutKind = PREVOUT_COINBASE;
			prevOutPayload = new Uint8Array(0);
		} else {
			throw new Error("unknown prevOut kind");
		}

		const tagByte = (prevOutKind & PREVOUT_MASK) | ((seqTag << SEQ_SHIFT) & SEQ_MASK);

		const scriptSigEncoded = scriptSigCodec.encode(data.scriptSig);
		const witnessEncoded = StoredWitness.encode(data.witness);

		const totalLength = 1 + prevOutPayload.length + seqBytes +
			scriptSigEncoded.length + witnessEncoded.length;
		const result = new Uint8Array(totalLength);
		let offset = 0;

		result[offset] = tagByte;
		offset += 1;

		result.set(prevOutPayload, offset);
		offset += prevOutPayload.length;

		if (seqExplicit) {
			result.set(U32LE.encode(seqU32), offset);
			offset += 4;
		}

		result.set(scriptSigEncoded, offset);
		offset += scriptSigEncoded.length;

		result.set(witnessEncoded, offset);

		return result;
	}

	decode(data: Uint8Array): [TxInput, number] {
		let offset = 0;

		const tagByte = data[offset]!;
		offset += 1;

		const prevOutKind = tagByte & PREVOUT_MASK;
		const seqTag = (tagByte & SEQ_MASK) >>> SEQ_SHIFT;

		let txId: OutPoint["txId"];
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

		const input: TxInput = {
			prevOut: { txId, vout },
			scriptSig,
			sequence: SequenceLockCodec.fromU32(seqU32),
			witness,
		};

		return [input, offset];
	}
}

export const StoredTxInput = new StoredTxInputCodec();
