import { Codec, Stride, VarInt } from "@nomadshiba/codec";
import {
	EXPECTED_SCRIPT_PAYLOAD_LEN,
	normalizeScriptPubKey,
	parseScriptPubKey,
	rawScriptPubKey,
	ScriptPubKey,
	SCRIPTPUBKEY_PATTERN,
} from "~/chain/ScriptPubKey.ts";
import { StoredPointer } from "~/codec/stored/StoredPointer.ts";

export type StoredTxOutput = {
	value: bigint;
	scriptPubKey:
		| { kind: "pointer"; value: number }
		| ScriptPubKey;
};

/**
 * StoredTxOutput binary layout
 *
 * Packed form of the clean shape:
 *   { value: VarInt, scriptPubKey: Enum<{ pointer, raw, p2pkh, p2sh, p2wpkh,
 *     p2wsh, p2tr, opreturn }> }
 *
 * Field order: flag byte, value, then the single variable-length script payload
 * last. Single forward cursor, no padding.
 *
 * -- 1-byte flag --
 * bits 0-2 : scriptTypeId  (3-bit script type, range 0..7)
 * bits 3-7 : spare
 *
 * scriptTypeId mapping:
 *   0 = pointer, 1 = raw, 2 = p2pkh, 3 = p2sh, 4 = p2wpkh, 5 = p2wsh,
 *   6 = p2tr, 7 = opreturn
 *
 * -- value (variable) --
 *   VarInt-encoded satoshis (51-bit max; <= 7 bytes for any valid value)
 *
 * -- script payload (variable, LAST) --
 *   pointer:  6 bytes (StoredPointer u48)
 *   raw:      VarInt-prefixed full script bytes
 *   opreturn: VarInt-prefixed full script bytes
 *   known:    fixed-length hash bytes (20 or 32)
 */

export type StoredScriptPubKey =
	| { kind: "pointer"; value: number }
	| { kind: "raw" | "opreturn" | keyof typeof SCRIPTPUBKEY_PATTERN; value: Uint8Array };

const SCRIPT_KIND_TO_ID: Record<StoredScriptPubKey["kind"], number> = {
	pointer: 0,
	raw: 1,
	p2pkh: 2,
	p2sh: 3,
	p2wpkh: 4,
	p2wsh: 5,
	p2tr: 6,
	opreturn: 7,
};

const SCRIPT_ID_TO_KIND: Record<number, StoredScriptPubKey["kind"]> = {
	0: "pointer",
	1: "raw",
	2: "p2pkh",
	3: "p2sh",
	4: "p2wpkh",
	5: "p2wsh",
	6: "p2tr",
	7: "opreturn",
};

// bits 0-2: scriptTypeId (0..7), bits 3-7: spare.
const SCRIPT_TYPE_MASK = 0x07;

function decodeScriptPubKey(kind: StoredScriptPubKey["kind"], payload: Uint8Array): [StoredScriptPubKey, number] {
	if (kind === "pointer") {
		const [pointer, size] = StoredPointer.decode(payload);
		return [{ kind: "pointer", value: pointer }, size];
	}

	if (kind === "raw" || kind === "opreturn") {
		const [scriptLen, lenSize] = VarInt.decode(payload);
		const scriptBytes = payload.subarray(lenSize, lenSize + scriptLen);
		const decoded = parseScriptPubKey(scriptBytes);
		return [decoded, lenSize + scriptLen];
	}

	const expectedLen = EXPECTED_SCRIPT_PAYLOAD_LEN[kind];
	if (expectedLen === undefined) throw new Error(`Unknown script kind: ${kind}`);

	return [{ kind, value: payload.subarray(0, expectedLen) }, expectedLen];
}

function scriptPayloadSize(data: StoredScriptPubKey): number {
	if (data.kind === "pointer") {
		return StoredPointer.stride.size;
	}
	const normalized = normalizeScriptPubKey(data);
	if (normalized.kind === "raw" || normalized.kind === "opreturn") {
		const script = rawScriptPubKey(normalized);
		return VarInt.size(script.length) + script.length;
	}
	return normalized.value.length;
}

export class StoredTxOutputCodec extends Codec<StoredTxOutput> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	encode(output: StoredTxOutput): Uint8Array<ArrayBuffer> {
		// Single encode path: size the buffer, then fill it via encodeInto.
		// `size()` and `encodeInto()` both normalize, so the standalone encode
		// path normalizes twice — fine here since the hot path uses encodeInto
		// directly (e.g. StoredTx.encodeWithOffsets) and never allocates.
		const out = new Uint8Array(this.size(output));
		this.encodeInto(output, out, 0);
		return out;
	}

	public override encodeInto(output: StoredTxOutput, target: Uint8Array, offset: number = 0): number {
		const { value, scriptPubKey } = output;

		if (value < 0n || value >= (1n << 51n)) {
			throw new Error("Value out of range for 51-bit integer");
		}

		const data = scriptPubKey;
		let cursor = offset;

		// pointer is resolved without normalizing (matches the original early
		// return) and written straight in — no intermediate StoredPointer.encode.
		if (data.kind === "pointer") {
			target[cursor++] = SCRIPT_KIND_TO_ID.pointer & SCRIPT_TYPE_MASK;
			cursor += VarInt.encodeInto(Number(value), target, cursor);
			cursor += StoredPointer.encodeInto(data.value, target, cursor);
			return cursor - offset;
		}

		const normalized = normalizeScriptPubKey(data);
		target[cursor++] = SCRIPT_KIND_TO_ID[normalized.kind] & SCRIPT_TYPE_MASK;
		cursor += VarInt.encodeInto(Number(value), target, cursor);

		if (normalized.kind === "raw" || normalized.kind === "opreturn") {
			// Length prefix + script bytes, both written in place. The only copy
			// left is the script payload itself (its bytes already exist).
			const script = rawScriptPubKey(normalized);
			cursor += VarInt.encodeInto(script.length, target, cursor);
			target.set(script, cursor);
			cursor += script.length;
		} else {
			// known hash type: fixed-length bytes, copied straight in.
			target.set(normalized.value, cursor);
			cursor += normalized.value.length;
		}

		return cursor - offset;
	}

	override size(output: StoredTxOutput): number {
		const { value, scriptPubKey } = output;
		if (value < 0n || value >= (1n << 51n)) {
			throw new Error("Value out of range for 51-bit integer");
		}
		return 1 + VarInt.size(Number(value)) + scriptPayloadSize(scriptPubKey);
	}

	decode(bytes: Uint8Array): [StoredTxOutput, number] {
		if (bytes.length < 1) throw new Error("Invalid data length for StoredTxOutput");

		let offset = 0;
		const flag = bytes[offset]!;
		offset += 1;

		const scriptTypeId = flag & SCRIPT_TYPE_MASK;
		const kind = SCRIPT_ID_TO_KIND[scriptTypeId];
		if (kind === undefined) throw new Error(`Unknown script type ID: ${scriptTypeId}`);

		const [valueNum, valueSize] = VarInt.decode(bytes.subarray(offset));
		offset += valueSize;
		const value = BigInt(valueNum);

		const [scriptPubKey, payloadSize] = decodeScriptPubKey(kind, bytes.subarray(offset));
		offset += payloadSize;

		const output: StoredTxOutput = { value, scriptPubKey };
		return [output, offset];
	}
}

export const StoredTxOutput = new StoredTxOutputCodec();
