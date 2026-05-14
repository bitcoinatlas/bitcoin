import { Codec } from "@nomadshiba/codec";
import { detectScriptPubKey, EXPECTED_SCRIPT_PAYLOAD_LEN, normalizeScriptPubKey, rawScriptPubKey, type ScriptPubKey } from "~/lib/chain/ScriptPubKey.ts";
import { StoredPointer } from "~/lib/codec/stored/StoredPointer.ts";
import { CompactSize } from "~/lib/codec/primitives.ts";

/**
 * StoredScriptPubKey — compact binary encoding of ScriptPubKey for on-disk storage.
 *
 * Uses a 4-bit typeId (0=pointer, 1=raw, 2-6=known types) to tag the payload.
 * Payload is:
 *   pointer: 6 bytes (StoredPointer:u48)
 *   raw:     CompactSize-prefixed full script bytes
 *   known:   fixed-length hash bytes (20 or 32)
 */

type Kind = ScriptPubKey["kind"] | "pointer";

export const KIND_TO_ID: Record<Kind, number> = {
	pointer: 0,
	raw: 1,
	p2pkh: 2,
	p2sh: 3,
	p2wpkh: 4,
	p2wsh: 5,
	p2tr: 6,
};

export const ID_TO_KIND: Record<number, Kind> = {
	[KIND_TO_ID.pointer]: "pointer",
	[KIND_TO_ID.raw]: "raw",
	[KIND_TO_ID.p2pkh]: "p2pkh",
	[KIND_TO_ID.p2sh]: "p2sh",
	[KIND_TO_ID.p2wpkh]: "p2wpkh",
	[KIND_TO_ID.p2wsh]: "p2wsh",
	[KIND_TO_ID.p2tr]: "p2tr",
};

export type StoredScriptPubKeyData =
	| { kind: "pointer"; value: number }
	| ScriptPubKey;

export function encodePayload(data: StoredScriptPubKeyData): { kind: Kind; payload: Uint8Array } {
	if (data.kind === "pointer") {
		return { kind: "pointer", payload: StoredPointer.encode(data.value) };
	}

	const normalized = normalizeScriptPubKey(data);

	if (normalized.kind === "raw") {
		const script = rawScriptPubKey(normalized);
		const len = CompactSize.encode(script.length);
		const payload = new Uint8Array(len.length + script.length);
		payload.set(len, 0);
		payload.set(script, len.length);
		return { kind: "raw", payload };
	}

	return { kind: normalized.kind, payload: normalized.value };
}

export function decodePayload(kind: Kind, payload: Uint8Array): [StoredScriptPubKeyData, number] {
	if (kind === "pointer") {
		const [pointer, size] = StoredPointer.decode(payload);
		return [{ kind: "pointer", value: pointer }, size];
	}

	if (kind === "raw") {
		const [scriptLen, lenSize] = CompactSize.decode(payload);
		const scriptBytes = payload.subarray(lenSize, lenSize + scriptLen);
		const decoded = detectScriptPubKey(scriptBytes);
		return [decoded, lenSize + scriptLen];
	}

	const expectedLen = EXPECTED_SCRIPT_PAYLOAD_LEN[kind];
	if (expectedLen === undefined) throw new Error(`Unknown script kind: ${kind}`);

	return [{ kind, value: payload.subarray(0, expectedLen) }, expectedLen];
}

export class StoredScriptPubKeyCodec extends Codec<StoredScriptPubKeyData> {
	readonly stride = -1;

	encode(value: StoredScriptPubKeyData): Uint8Array<ArrayBuffer> {
		const { kind, payload } = encodePayload(value);
		const tag = new Uint8Array([KIND_TO_ID[kind]!]);
		const out = new Uint8Array(1 + payload.length);
		out.set(tag, 0);
		out.set(payload, 1);
		return out;
	}

	decode(bytes: Uint8Array): [StoredScriptPubKeyData, number] {
		const typeId = bytes[0]!;
		const kind = ID_TO_KIND[typeId];
		if (kind === undefined) throw new Error(`Unknown StoredScriptPubKey type ID: ${typeId}`);
		const [data, payloadSize] = decodePayload(kind, bytes.subarray(1));
		return [data, 1 + payloadSize];
	}
}

export const StoredScriptPubKey = new StoredScriptPubKeyCodec();
