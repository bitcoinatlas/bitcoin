import { Codec, Stride } from "@nomadshiba/codec";
import { TxOutput, TxOutputData } from "~/lib/chain/TxOutput.ts";
import {
	decodePayload,
	encodePayload,
	ID_TO_KIND,
	KIND_TO_ID,
	type StoredScriptPubKeyData,
} from "~/lib/codec/stored/StoredScriptPubKey.ts";

/**
 * StoredTxOutput binary layout
 *
 * ── 7-byte header (56 bits total, little-endian) ──
 * bits  0–50 : value   (51-bit unsigned satoshis)
 * bit   51   : spent   (1-bit flag)
 * bits 52–55 : typeId  (4-bit script type, range 0..15)
 *
 * typeId mapping (see StoredScriptPubKey.ts):
 *   0 = pointer, 1 = raw, 2 = p2pkh, 3 = p2sh, 4 = p2wpkh, 5 = p2wsh, 6 = p2tr
 *
 * ── payload (variable) ──
 * Encoded by encodePayload / decoded by decodePayload in StoredScriptPubKey
 */

export class StoredTxOutputCodec extends Codec<TxOutput> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	encode(output: TxOutput): Uint8Array<ArrayBuffer> {
		const { data } = output;

		if (data.value < 0n || data.value >= (1n << 51n)) {
			throw new Error("Value out of range for 51-bit integer");
		}

		const { kind, payload } = encodePayload(data.scriptPubKey as StoredScriptPubKeyData);

		let bits = BigInt(KIND_TO_ID[kind]);
		if (data.spent) bits |= 1n << 4n;
		const combined = (bits << 51n) | data.value;

		const header = new Uint8Array(7);
		for (let i = 0; i < 7; i++) {
			header[i] = Number((combined >> BigInt(i * 8)) & 0xffn);
		}

		const out = new Uint8Array(7 + payload.length);
		out.set(header, 0);
		out.set(payload, 7);
		return out;
	}

	decode(bytes: Uint8Array): [TxOutput, number] {
		if (bytes.length < 7) throw new Error("Invalid data length for StoredTxOutput");

		let combined = 0n;
		for (let i = 0; i < 7; i++) {
			combined |= BigInt(bytes[i]!) << BigInt(i * 8);
		}

		const value = combined & ((1n << 51n) - 1n);
		const bits = combined >> 51n;
		const spent = (bits & (1n << 4n)) !== 0n;
		const typeId = Number(bits & 0xfn);

		const kind = ID_TO_KIND[typeId];
		if (kind === undefined) throw new Error(`Unknown type ID: ${typeId}`);

		const [scriptPubKey, payloadSize] = decodePayload(kind, bytes.subarray(7));

		const data: TxOutputData = { value, spent, scriptPubKey };
		return [new TxOutput(data), 7 + payloadSize];
	}
}

export const StoredTxOutput = new StoredTxOutputCodec();
