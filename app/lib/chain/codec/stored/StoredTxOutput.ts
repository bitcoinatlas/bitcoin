import { Codec } from "@nomadshiba/codec";
import { ScriptPubKey } from "~/lib/chain/utils/ScriptPubKey.ts";
import { TxOutput, TxOutputData } from "~/lib/chain/TxOutput.ts";
import { StoredPointer } from "~/lib/chain/codec/stored/StoredPointer.ts";
import { CompactSize } from "~/lib/codec/primitives.ts";

/**
 * StoredTxOutput binary layout
 *
 * ── 7-byte header (56 bits total, little-endian) ──
 * bits  0–50 : value   (51-bit unsigned satoshis)
 * bit   51   : spent   (1-bit flag)
 * bits 52–55 : typeId  (4-bit script type, range 0..15)
 *
 * typeId mapping:
 *   0 = pointer (StoredPointer:u48)
 *   1 = raw     (scriptPubKey, arbitrary length with CompactSize prefix)
 *   2 = p2pkh   (20-byte hash160)
 *   3 = p2sh    (20-byte hash160)
 *   4 = p2wpkh  (20-byte hash160)
 *   5 = p2wsh   (32-byte sha256)
 *   6 = p2tr    (32-byte xonly pubkey)
 *   7–15 = reserved
 *
 * ── payload (variable) ──
 * if typeId = 0: 6 bytes [StoredPointer:u48]
 * if typeId = 1: CompactSize + raw scriptPubKey
 * if typeId = 2–6: fixed-length data as listed above
 *
 * userland type is TxOutput with TxOutputData
 */

type Kind = ScriptPubKey["kind"] | "pointer";

const KIND_TO_ID: Record<Kind, number> = {
	pointer: 0,
	raw: 1,
	p2pkh: 2,
	p2sh: 3,
	p2wpkh: 4,
	p2wsh: 5,
	p2tr: 6,
};

const ID_TO_KIND: Record<number, Kind> = {
	[KIND_TO_ID.pointer]: "pointer",
	[KIND_TO_ID.raw]: "raw",
	[KIND_TO_ID.p2pkh]: "p2pkh",
	[KIND_TO_ID.p2sh]: "p2sh",
	[KIND_TO_ID.p2wpkh]: "p2wpkh",
	[KIND_TO_ID.p2wsh]: "p2wsh",
	[KIND_TO_ID.p2tr]: "p2tr",
};

const EXPECTED_HASH_LEN: Record<Exclude<Kind, "raw" | "pointer">, number> = {
	p2pkh: 20,
	p2sh: 20,
	p2wpkh: 20,
	p2wsh: 32,
	p2tr: 32,
};

export class StoredTxOutputCodec extends Codec<TxOutput> {
	readonly stride = -1;

	encode(output: TxOutput): Uint8Array {
		const { data } = output;

		if (data.value < 0n || data.value >= (1n << 51n)) {
			throw new Error("Value out of range for 51-bit integer");
		}

		let payload: Uint8Array;
		let storeKind: Kind;

		if (data.scriptPubKey.kind === "pointer") {
			storeKind = "pointer";
			payload = StoredPointer.encode(data.scriptPubKey.value);
		} else {
			const normalized = ScriptPubKey.normalize(data.scriptPubKey);
			storeKind = normalized.kind;

			if (storeKind === "raw") {
				// Raw scripts: CompactSize prefix + full script bytes
				const script = ScriptPubKey.toRaw(normalized);
				const len = CompactSize.encode(script.length);
				payload = new Uint8Array(len.length + script.length);
				payload.set(len, 0);
				payload.set(script, len.length);
			} else {
				// Known types: just the hash (value)
				payload = normalized.value;
			}
		}

		let bits = BigInt(KIND_TO_ID[storeKind]);
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
		const payload = bytes.subarray(7);

		let bytesRead = 7;
		let scriptPubKey: TxOutputData["scriptPubKey"];

		const kind = ID_TO_KIND[typeId];

		if (kind === "pointer") {
			const [pointer, size] = StoredPointer.decode(payload);
			bytesRead += size;
			scriptPubKey = { kind: "pointer", value: pointer };
		} else if (kind === "raw") {
			// Raw scripts: CompactSize prefix + full script bytes
			const [scriptLen, lenSize] = CompactSize.decode(payload);
			if (payload.length < lenSize + scriptLen) {
				throw new Error(
					`Invalid raw script length: expected ${lenSize + scriptLen} bytes, got ${payload.length}`,
				);
			}
			const scriptBytes = payload.subarray(lenSize, lenSize + scriptLen);
			const decoded = ScriptPubKey.fromRaw(scriptBytes);
			bytesRead += lenSize + scriptLen;
			scriptPubKey = decoded;
		} else if (kind) {
			// Known script type - payload is just the hash
			const expectedLen = EXPECTED_HASH_LEN[kind];
			if (payload.length < expectedLen) {
				throw new Error(`Invalid payload length for ${kind}: expected ${expectedLen}, got ${payload.length}`);
			}
			scriptPubKey = { kind, value: payload.subarray(0, expectedLen) };
			bytesRead += expectedLen;
		} else {
			throw new Error(`Unknown type ID: ${typeId}`);
		}

		const data: TxOutputData = { value, spent, scriptPubKey };
		return [new TxOutput(data), bytesRead];
	}
}

export const StoredTxOutput = new StoredTxOutputCodec();
