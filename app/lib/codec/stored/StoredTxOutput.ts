import { Codec, Stride, VarInt } from "@nomadshiba/codec";
import {
	EXPECTED_SCRIPT_PAYLOAD_LEN,
	normalizeScriptPubKey,
	parseScriptPubKey,
	rawScriptPubKey,
	ScriptPubKey,
	SCRIPTPUBKEY_PATTERN,
} from "~/lib/chain/ScriptPubKey.ts";
import { StoredPointer } from "~/lib/codec/stored/StoredPointer.ts";

export type TxOutput = {
	value: bigint;
	spentBy: number | null;
	scriptPubKey:
		| { kind: "pointer"; value: number }
		| ScriptPubKey;
};

export const TxOutput = {
	async getScriptPubKey(output: TxOutput): Promise<ScriptPubKey> {
		if (output.scriptPubKey.kind === "pointer") {
			// TODO: Why?
			const { getTxOutputByPointer } = await import("~/chain.ts");
			const resolved = await getTxOutputByPointer(output.scriptPubKey.value);
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
	async getRawScriptPubKey(output: TxOutput): Promise<Uint8Array> {
		return rawScriptPubKey(await TxOutput.getScriptPubKey(output));
	},
};

/**
 * StoredTxOutput binary layout
 *
 * Packed form of the clean shape:
 *   { value: VarInt, scriptPubKey: Enum<{ pointer, raw, p2pkh, p2sh, p2wpkh,
 *     p2wsh, p2tr, opreturn }> }
 * where every spendable variant carries a fixed 6-byte `spentBy` pointer and
 * `opreturn` never does (it is provably unspendable, so no spend state exists).
 *
 * Spend state is the pointer value itself: spentBy == 0 means UNSPENT (null).
 * Blob offset 0 is the genesis coinbase output, which is unspendable, so 0 can
 * never be a real spend target -- it is a free, unambiguous sentinel.
 *
 * The pointer is FIXED-WIDTH and always present for spendable types. This is
 * deliberate: spending an output overwrites the 6 zero-bytes with a real
 * pointer IN PLACE, with no change to record size and no shift of any field
 * after it. A presence flag (omit-when-unspent) would make spending a
 * layout-growing operation that rewrites the record and everything after it,
 * which is incompatible with the append-only blob -- so it is not used.
 *
 * Field order: flag byte, spentBy (spendable only), value, then the single
 * variable-length script payload last. The fixed-width spentBy comes BEFORE the
 * variable-length value so it sits at a CONSTANT offset (record + 1) for every
 * spendable output, independent of how many bytes `value` encodes to. Spending
 * is then a fixed-offset 6-byte poke with no VarInt decode required to locate
 * the pointer being overwritten. Single forward cursor, no padding.
 *
 * -- 1-byte flag --
 * bits 0-2 : scriptTypeId  (3-bit script type, range 0..7)
 * bits 3-7 : spare
 *
 * scriptTypeId mapping:
 *   0 = pointer, 1 = raw, 2 = p2pkh, 3 = p2sh, 4 = p2wpkh, 5 = p2wsh,
 *   6 = p2tr, 7 = opreturn
 *
 * -- spentBy (spendable types only, fixed 6 bytes, at record + 1) --
 *   StoredPointer (u48) blob offset of the spending input, or 0 if unspent.
 *   Never present for opreturn (provably unspendable).
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

// Blob offset 0 is the unspendable genesis coinbase output, so a 0 pointer can
// never be a real spend target -- it is the UNSPENT (null) sentinel.
const SPENT_UNSPENT = 0;

function isSpendable(kind: StoredScriptPubKey["kind"]): boolean {
	return kind !== "opreturn";
}

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
		const { value, spentBy, scriptPubKey } = output;

		if (value < 0n || value >= (1n << 51n)) {
			throw new Error("Value out of range for 51-bit integer");
		}

		const { kind, payload } = encodeScriptPubKey(scriptPubKey as StoredScriptPubKey);
		const spendable = isSpendable(kind);

		const flag = SCRIPT_KIND_TO_ID[kind] & SCRIPT_TYPE_MASK;

		const valueEncoded = VarInt.encode(Number(value));
		// Spendable types always store a fixed 6-byte pointer (0 = unspent).
		// opreturn stores nothing for spend state.
		const spentByEncoded = spendable ? StoredPointer.encode(spentBy ?? SPENT_UNSPENT) : null;

		const total = 1 +
			(spentByEncoded ? spentByEncoded.length : 0) +
			valueEncoded.length +
			payload.length;

		const out = new Uint8Array(total);
		let offset = 0;

		out[offset] = flag;
		offset += 1;

		// Fixed-width spentBy first, so it lands at a constant offset (record + 1)
		// for every spendable output regardless of the VarInt value width.
		if (spentByEncoded) {
			out.set(spentByEncoded, offset);
			offset += spentByEncoded.length;
		}

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

		const spendable = isSpendable(kind);

		// Spendable types carry a fixed 6-byte spentBy pointer (0 = unspent/null)
		// immediately after the flag. opreturn has none.
		let spentBy: number | null = null;
		if (spendable) {
			const [ptr, ptrSize] = StoredPointer.decode(bytes.subarray(offset));
			spentBy = ptr === SPENT_UNSPENT ? null : ptr;
			offset += ptrSize;
		}

		const [valueNum, valueSize] = VarInt.decode(bytes.subarray(offset));
		offset += valueSize;
		const value = BigInt(valueNum);

		const [scriptPubKey, payloadSize] = decodeScriptPubKey(kind, bytes.subarray(offset));
		offset += payloadSize;

		const output: TxOutput = { value, spentBy, scriptPubKey };
		return [output, offset];
	}
}

export const StoredTxOutput = new StoredTxOutputCodec();
