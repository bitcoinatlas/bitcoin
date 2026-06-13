import { Codec, Stride, VarInt } from "@nomadshiba/codec";
import { atomic } from "~/chain/chain.ts";
import {
	EXPECTED_SCRIPT_PAYLOAD_LEN,
	normalizeScriptPubKey,
	parseScriptPubKey,
	rawScriptPubKey,
	ScriptPubKey,
	SCRIPTPUBKEY_PATTERN,
} from "~/chain/ScriptPubKey.ts";
import { StoredPointer } from "~/codec/stored/StoredPointer.ts";
import { InferBatches, InferStores } from "~/storage/Atomic.ts";

export type TxOutput = {
	value: bigint;
	scriptPubKey:
		| { kind: "pointer"; value: number }
		| ScriptPubKey;
};

export const TxOutput = {
	async getScriptPubKey(
		output: TxOutput,
		batches?: InferBatches<typeof atomic, "tx"> | InferStores<typeof atomic, "tx">,
	): Promise<ScriptPubKey> {
		if (output.scriptPubKey.kind === "pointer") {
			// TODO: Why?
			const { getTxOutputByPointer } = await import("~/chain/chain.ts");
			const resolved = await getTxOutputByPointer(output.scriptPubKey.value, batches);
			if (resolved.scriptPubKey.kind === "pointer") {
				throw new Error([
					`scriptPubKey resolution failed: pointer ${output.scriptPubKey.value} points to another pointer.`,
					`Expected direct ScriptPubKey at that offset.`,
				].join(" "));
			}
			return resolved.scriptPubKey;
		} else {
			return output.scriptPubKey;
		}
	},
	async getRawScriptPubKey(
		output: TxOutput,
		batches?: InferBatches<typeof atomic, "tx"> | InferStores<typeof atomic, "tx">,
	): Promise<Uint8Array> {
		return rawScriptPubKey(await TxOutput.getScriptPubKey(output, batches));
	},
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

function encodeScriptPubKey(data: StoredScriptPubKey): { kind: StoredScriptPubKey["kind"]; payload: Uint8Array } {
	if (data.kind === "pointer") {
		return { kind: "pointer", payload: StoredPointer.encode(data.value) };
	}

	const normalized = normalizeScriptPubKey(data);

	if (normalized.kind === "raw" || normalized.kind === "opreturn") {
		const script = rawScriptPubKey(normalized);
		const len = VarInt.encode(script.length);
		const payload = new Uint8Array(len.length + script.length);
		payload.set(len, 0);
		payload.set(script, len.length);
		return { kind: normalized.kind, payload };
	}

	return { kind: normalized.kind, payload: normalized.value };
}

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

export class StoredTxOutputCodec extends Codec<TxOutput> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	encode(output: TxOutput): Uint8Array<ArrayBuffer> {
		const { value, scriptPubKey } = output;

		if (value < 0n || value >= (1n << 51n)) {
			throw new Error("Value out of range for 51-bit integer");
		}

		const { kind, payload } = encodeScriptPubKey(scriptPubKey as StoredScriptPubKey);

		const flag = SCRIPT_KIND_TO_ID[kind] & SCRIPT_TYPE_MASK;

		const valueEncoded = VarInt.encode(Number(value));

		const total = 1 + valueEncoded.length + payload.length;

		const out = new Uint8Array(total);
		let offset = 0;

		out[offset] = flag;
		offset += 1;

		out.set(valueEncoded, offset);
		offset += valueEncoded.length;

		out.set(payload, offset);
		return out;
	}

	decode(bytes: Uint8Array): [TxOutput, number] {
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

		const output: TxOutput = { value, scriptPubKey };
		return [output, offset];
	}
}

export const StoredTxOutput = new StoredTxOutputCodec();
