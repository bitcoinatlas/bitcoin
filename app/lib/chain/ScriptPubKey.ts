import { Codec, Stride } from "@nomadshiba/codec";

export type ScriptPubKey = { kind: "raw" | keyof typeof SCRIPTPUBKEY_PATTERN; value: Uint8Array };

export const SCRIPTPUBKEY_PATTERN = {
	p2pkh: {
		prefix: [0x76, 0xa9, 0x14] as const,
		suffix: [0x88, 0xac] as const,
		hashLen: 20,
		scriptLen: 25,
	},
	p2sh: {
		prefix: [0xa9, 0x14] as const,
		suffix: [0x87] as const,
		hashLen: 20,
		scriptLen: 23,
	},
	p2wpkh: {
		prefix: [0x00, 0x14] as const,
		suffix: [] as const,
		hashLen: 20,
		scriptLen: 22,
	},
	p2wsh: {
		prefix: [0x00, 0x20] as const,
		suffix: [] as const,
		hashLen: 32,
		scriptLen: 34,
	},
	p2tr: {
		prefix: [0x51, 0x20] as const,
		suffix: [] as const,
		hashLen: 32,
		scriptLen: 34,
	},
} as const;

export const EXPECTED_SCRIPT_PAYLOAD_LEN: Record<Exclude<ScriptPubKey["kind"], "raw">, number> = {
	p2pkh: SCRIPTPUBKEY_PATTERN.p2pkh.hashLen,
	p2sh: SCRIPTPUBKEY_PATTERN.p2sh.hashLen,
	p2wpkh: SCRIPTPUBKEY_PATTERN.p2wpkh.hashLen,
	p2wsh: SCRIPTPUBKEY_PATTERN.p2wsh.hashLen,
	p2tr: SCRIPTPUBKEY_PATTERN.p2tr.hashLen,
};

/** Try to detect the known script type from raw bytes. Falls back to "raw" kind. */
export function detectScriptPubKey(raw: Uint8Array): ScriptPubKey {
	if (raw.length === SCRIPTPUBKEY_PATTERN.p2pkh.scriptLen) {
		let match = true;
		for (let i = 0; i < SCRIPTPUBKEY_PATTERN.p2pkh.prefix.length; i++) {
			if (raw[i] !== SCRIPTPUBKEY_PATTERN.p2pkh.prefix[i]) {
				match = false;
				break;
			}
		}
		if (match) {
			for (let i = 0; i < SCRIPTPUBKEY_PATTERN.p2pkh.suffix.length; i++) {
				if (
					raw[raw.length - SCRIPTPUBKEY_PATTERN.p2pkh.suffix.length + i] !==
						SCRIPTPUBKEY_PATTERN.p2pkh.suffix[i]
				) {
					match = false;
					break;
				}
			}
		}
		if (match) return { kind: "p2pkh", value: raw.subarray(3, 23) };
	}

	if (raw.length === SCRIPTPUBKEY_PATTERN.p2sh.scriptLen) {
		let match = true;
		for (let i = 0; i < SCRIPTPUBKEY_PATTERN.p2sh.prefix.length; i++) {
			if (raw[i] !== SCRIPTPUBKEY_PATTERN.p2sh.prefix[i]) {
				match = false;
				break;
			}
		}
		if (match) {
			for (let i = 0; i < SCRIPTPUBKEY_PATTERN.p2sh.suffix.length; i++) {
				if (
					raw[raw.length - SCRIPTPUBKEY_PATTERN.p2sh.suffix.length + i] !==
						SCRIPTPUBKEY_PATTERN.p2sh.suffix[i]
				) {
					match = false;
					break;
				}
			}
		}
		if (match) return { kind: "p2sh", value: raw.subarray(2, 22) };
	}

	if (raw.length === SCRIPTPUBKEY_PATTERN.p2wpkh.scriptLen) {
		let match = true;
		for (let i = 0; i < SCRIPTPUBKEY_PATTERN.p2wpkh.prefix.length; i++) {
			if (raw[i] !== SCRIPTPUBKEY_PATTERN.p2wpkh.prefix[i]) {
				match = false;
				break;
			}
		}
		if (match) return { kind: "p2wpkh", value: raw.subarray(2) };
	}

	if (raw.length === SCRIPTPUBKEY_PATTERN.p2wsh.scriptLen) {
		let match = true;
		for (let i = 0; i < SCRIPTPUBKEY_PATTERN.p2wsh.prefix.length; i++) {
			if (raw[i] !== SCRIPTPUBKEY_PATTERN.p2wsh.prefix[i]) {
				match = false;
				break;
			}
		}
		if (match) return { kind: "p2wsh", value: raw.subarray(2) };
	}

	if (raw.length === SCRIPTPUBKEY_PATTERN.p2tr.scriptLen) {
		let match = true;
		for (let i = 0; i < SCRIPTPUBKEY_PATTERN.p2tr.prefix.length; i++) {
			if (raw[i] !== SCRIPTPUBKEY_PATTERN.p2tr.prefix[i]) {
				match = false;
				break;
			}
		}
		if (match) return { kind: "p2tr", value: raw.subarray(2) };
	}

	return { kind: "raw", value: raw.slice() };
}

/** Reconstruct raw script bytes from a ScriptPubKey. */
export function rawScriptPubKey(parsed: ScriptPubKey): Uint8Array {
	switch (parsed.kind) {
		case "raw":
			return parsed.value.slice();
		case "p2pkh": {
			const out = new Uint8Array(SCRIPTPUBKEY_PATTERN.p2pkh.scriptLen);
			out[0] = 0x76;
			out[1] = 0xa9;
			out[2] = 0x14;
			out.set(parsed.value, 3);
			out[23] = 0x88;
			out[24] = 0xac;
			return out;
		}
		case "p2sh": {
			const out = new Uint8Array(SCRIPTPUBKEY_PATTERN.p2sh.scriptLen);
			out[0] = 0xa9;
			out[1] = 0x14;
			out.set(parsed.value, 2);
			out[22] = 0x87;
			return out;
		}
		case "p2wpkh": {
			const out = new Uint8Array(SCRIPTPUBKEY_PATTERN.p2wpkh.scriptLen);
			out[0] = 0x00;
			out[1] = 0x14;
			out.set(parsed.value, 2);
			return out;
		}
		case "p2wsh": {
			const out = new Uint8Array(SCRIPTPUBKEY_PATTERN.p2wsh.scriptLen);
			out[0] = 0x00;
			out[1] = 0x20;
			out.set(parsed.value, 2);
			return out;
		}
		case "p2tr": {
			const out = new Uint8Array(SCRIPTPUBKEY_PATTERN.p2tr.scriptLen);
			out[0] = 0x51;
			out[1] = 0x20;
			out.set(parsed.value, 2);
			return out;
		}
	}
}

/** Normalize: if kind is "raw", try to detect the actual type. */
export function normalizeScriptPubKey(parsed: ScriptPubKey): ScriptPubKey {
	if (parsed.kind !== "raw") return parsed;
	return detectScriptPubKey(parsed.value);
}

export class ScriptPubKeyCodec extends Codec<ScriptPubKey> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	encode(value: ScriptPubKey): Uint8Array<ArrayBuffer> {
		return rawScriptPubKey(normalizeScriptPubKey(value)) as Uint8Array<ArrayBuffer>;
	}

	decode(bytes: Uint8Array): [ScriptPubKey, number] {
		const result = detectScriptPubKey(bytes);
		return [result, bytes.length];
	}
}

export const ScriptPubKey = new ScriptPubKeyCodec();
