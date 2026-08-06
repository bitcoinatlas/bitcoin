export type ScriptPubKey = { kind: "raw" | "op_return" | keyof typeof SCRIPTPUBKEY_PATTERN; value: Uint8Array };

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

/** First opcode of an OP_RETURN (provably-unspendable) output script. */
export const OP_RETURN = 0x6a;

export const EXPECTED_SCRIPT_PAYLOAD_LEN: Record<Exclude<ScriptPubKey["kind"], "raw" | "op_return">, number> = {
	p2pkh: SCRIPTPUBKEY_PATTERN.p2pkh.hashLen,
	p2sh: SCRIPTPUBKEY_PATTERN.p2sh.hashLen,
	p2wpkh: SCRIPTPUBKEY_PATTERN.p2wpkh.hashLen,
	p2wsh: SCRIPTPUBKEY_PATTERN.p2wsh.hashLen,
	p2tr: SCRIPTPUBKEY_PATTERN.p2tr.hashLen,
};

/** Try to detect the known script type from raw bytes. Falls back to "raw" kind. */
export function parseScriptPubKey(raw: Uint8Array): ScriptPubKey {
	// OP_RETURN: prefix-only match (first opcode 0x6a). Variable length, no
	// suffix. Provably unspendable, so it never carries spend state. The full
	// script (including the 0x6a and any pushdata opcodes) is stored verbatim
	// to guarantee exact round-trip across non-canonical pushes.
	if (raw.length >= 1 && raw[0] === OP_RETURN) {
		return { kind: "op_return", value: raw.slice() };
	}

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
export function rawScriptPubKey(parsed: ScriptPubKey, target?: Uint8Array, offset = 0): Uint8Array {
	if (parsed.kind === "raw" || parsed.kind === "op_return") {
		// TODO: we store the full with the byte flag opreturn even when parsed, fix that later. not now because syncing
		return parsed.value.slice();
	}

	let out;
	if (target) {
		out = target;
		if ("resize" in out.buffer && out.buffer.resizable) {
			out.buffer.resize(SCRIPTPUBKEY_PATTERN[parsed.kind].scriptLen);
		}
	} else {
		out = new Uint8Array(SCRIPTPUBKEY_PATTERN[parsed.kind].scriptLen);
	}

	switch (parsed.kind) {
		case "p2pkh":
			out[offset + 0] = 0x76;
			out[offset + 1] = 0xa9;
			out[offset + 2] = 0x14;
			out.set(parsed.value, offset + 3);
			out[offset + 23] = 0x88;
			out[offset + 24] = 0xac;
			return out;
		case "p2sh":
			out[offset + 0] = 0xa9;
			out[offset + 1] = 0x14;
			out.set(parsed.value, offset + 2);
			out[offset + 22] = 0x87;
			return out;
		case "p2wpkh":
			out[offset + 0] = 0x00;
			out[offset + 1] = 0x14;
			out.set(parsed.value, 2);
			return out;
		case "p2wsh":
			out[offset + 0] = 0x00;
			out[offset + 1] = 0x20;
			out.set(parsed.value, offset + 2);
			return out;
		case "p2tr":
			out[offset + 0] = 0x51;
			out[offset + 1] = 0x20;
			out.set(parsed.value, offset + 2);
			return out;
	}
}

/** Normalize: if kind is "raw", try to detect the actual type. */
export function normalizeScriptPubKey(parsed: ScriptPubKey): ScriptPubKey {
	if (parsed.kind !== "raw") return parsed;
	return parseScriptPubKey(parsed.value);
}
