import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import type { BoundCodec } from "~/lib/codec/mod.ts";
import { StoredPointer } from "./StoredPointer.ts";

/**
 * StoredTxOutput binary layout
 *
 * ── 7-byte header (56 bits total, little-endian) ──
 * bits  0–50 : value   (51-bit unsigned satoshis)
 * bit   51   : spent   (1-bit flag)
 * bits 52–55 : typeId  (4-bit script type, range 0..15)
 *
 * typeId mapping:
 *   0 = pointer (BlockPointer:u48)
 *   1 = raw     (scriptPubKey, arbitrary length)
 *   2 = p2pkh   (20-byte hash160)
 *   3 = p2sh    (20-byte hash160)
 *   4 = p2wpkh  (20-byte hash160)
 *   5 = p2wsh   (32-byte sha256)
 *   6 = p2tr    (32-byte xonly pubkey)
 *   7–15 = reserved
 *
 * ── payload (variable) ──
 * if typeId = 0: 6 bytes [BlockPointer:u48]
 * if typeId = 1: raw scriptPubKey (arbitrary length)
 * if typeId = 2–6: fixed-length data as listed above
 *
 * userland type is always either:
 *   { value, spent, scriptType: "pointer", pointer }
 *   { value, spent, scriptType: "script", scriptPubKey }
 */

const SCRIPT_TYPE = {
	pointer: 0,
	raw: 1,
	p2pkh: 2,
	p2sh: 3,
	p2wpkh: 4,
	p2wsh: 5,
	p2tr: 6,
} as const;

const SCRIPT_SIZE: Record<number, number> = {
	[SCRIPT_TYPE.p2pkh]: 20,
	[SCRIPT_TYPE.p2sh]: 20,
	[SCRIPT_TYPE.p2wpkh]: 20,
	[SCRIPT_TYPE.p2wsh]: 32,
	[SCRIPT_TYPE.p2tr]: 32,
};

export type StoredTxOutput =
	| { value: bigint; spent: boolean; scriptType: "pointer"; pointer: number }
	| { value: bigint; spent: boolean; scriptType: "raw"; scriptPubKey: Uint8Array };

function detectCompact(script: Uint8Array):
	| { typeId: number; payload: Uint8Array }
	| null {
	// p2pkh
	if (
		script.length === 25 &&
		script[0] === 0x76 && script[1] === 0xa9 &&
		script[2] === 0x14 && script[23] === 0x88 && script[24] === 0xac
	) {
		return { typeId: SCRIPT_TYPE.p2pkh, payload: script.subarray(3, 23) };
	}
	// p2sh
	if (script.length === 23 && script[0] === 0xa9 && script[1] === 0x14 && script[22] === 0x87) {
		return { typeId: SCRIPT_TYPE.p2sh, payload: script.subarray(2, 22) };
	}
	// p2wpkh
	if (script.length === 22 && script[0] === 0x00 && script[1] === 0x14) {
		return { typeId: SCRIPT_TYPE.p2wpkh, payload: script.subarray(2) };
	}
	// p2wsh
	if (script.length === 34 && script[0] === 0x00 && script[1] === 0x20) {
		return { typeId: SCRIPT_TYPE.p2wsh, payload: script.subarray(2) };
	}
	// p2tr
	if (script.length === 34 && script[0] === 0x51 && script[1] === 0x20) {
		return { typeId: SCRIPT_TYPE.p2tr, payload: script.subarray(2) };
	}
	return null;
}

function reconstructScript(typeId: number, payload: Uint8Array): Uint8Array {
	switch (typeId) {
		case SCRIPT_TYPE.p2pkh:
			return Uint8Array.of(0x76, 0xa9, 0x14, ...payload, 0x88, 0xac);
		case SCRIPT_TYPE.p2sh:
			return Uint8Array.of(0xa9, 0x14, ...payload, 0x87);
		case SCRIPT_TYPE.p2wpkh:
			return Uint8Array.of(0x00, 0x14, ...payload);
		case SCRIPT_TYPE.p2wsh:
			return Uint8Array.of(0x00, 0x20, ...payload);
		case SCRIPT_TYPE.p2tr:
			return Uint8Array.of(0x51, 0x20, ...payload);
		default:
			throw new Error(`Cannot reconstruct unknown compact type ${typeId}`);
	}
}

type StoredTxOutputCodec = { stride: number };

const StoredTxOutputCodec = {
	...CodecDefaults<StoredTxOutputCodec>(),
	create(): StoredTxOutputCodec {
		return { stride: -1 };
	},
	encode(_self, obj: StoredTxOutput) {
		if (obj.value < 0n || obj.value >= (1n << 51n)) {
			throw new Error("Value out of range for 51-bit integer");
		}

		let typeId: number;
		let payload: Uint8Array;

		if (obj.scriptType === "pointer") {
			typeId = SCRIPT_TYPE.pointer;
			payload = StoredPointer.encode(obj.pointer);
		} else {
			const detected = detectCompact(obj.scriptPubKey);
			if (detected) {
				typeId = detected.typeId;
				payload = detected.payload;
			} else {
				typeId = SCRIPT_TYPE.raw;
				payload = obj.scriptPubKey;
			}
		}

		let bits = BigInt(typeId);
		if (obj.spent) bits |= 1n << 4n; // spent flag bit
		const combined = (bits << 51n) | obj.value;

		const header = new Uint8Array(7);
		for (let i = 0; i < 7; i++) {
			header[i] = Number((combined >> BigInt(i * 8)) & 0xffn);
		}

		const out = new Uint8Array(7 + payload.length);
		out.set(header, 0);
		out.set(payload, 7);
		return out;
	},
	decode(_self, data: Uint8Array) {
		if (data.length < 7) throw new Error("Invalid data length for StoredTxOutput");

		let combined = 0n;
		for (let i = 0; i < 7; i++) {
			combined |= BigInt(data[i]!) << BigInt(i * 8);
		}

		const value = combined & ((1n << 51n) - 1n);
		const bits = combined >> 51n;
		const spent = (bits & (1n << 4n)) !== 0n;
		const typeId = Number(bits & 0xfn);
		const payload = data.subarray(7);

		let bytesRead = 7;

		if (typeId === SCRIPT_TYPE.pointer) {
			if (payload.length < StoredPointer.stride) throw new Error("Invalid pointer payload length");
			const [pointer] = StoredPointer.decode(payload);
			bytesRead += StoredPointer.stride;
			return [
				{
					value,
					spent,
					scriptType: "pointer" as const,
					pointer,
				},
				bytesRead,
			] as [StoredTxOutput, number];
		}
		if (typeId === SCRIPT_TYPE.raw) {
			return [
				{ value, spent, scriptType: "raw" as const, scriptPubKey: payload },
				data.length,
			] as [StoredTxOutput, number];
		}

		// Fixed-size payloads based on typeId
		const payloadSize = SCRIPT_SIZE[typeId];
		if (payloadSize === undefined) {
			throw new Error(`Unknown type ID: ${typeId}`);
		}

		bytesRead += payloadSize;
		const script = reconstructScript(typeId, payload.subarray(0, payloadSize));
		return [
			{ value, spent, scriptType: "raw" as const, scriptPubKey: script },
			bytesRead,
		] as [StoredTxOutput, number];
	},
} satisfies Impl<StoredTxOutputCodec, Codec<StoredTxOutputCodec, StoredTxOutput>>;

const _schema = StoredTxOutputCodec.create();

export const StoredTxOutput: BoundCodec<StoredTxOutput> = {
	stride: _schema.stride,
	encode(value: StoredTxOutput): Uint8Array {
		return StoredTxOutputCodec.encode(_schema, value);
	},
	decode(data: Uint8Array): [StoredTxOutput, number] {
		return StoredTxOutputCodec.decode(_schema, data);
	},
};
