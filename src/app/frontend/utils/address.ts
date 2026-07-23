import { sha256d } from "~/libs/hashes/sha256d.ts";
import { parseScriptPubKey, type ScriptPubKey } from "~/chain/ScriptPubKey.ts";
import { encodeBase58, encodeHex } from "@std/encoding";

/** Encode a versioned payload as base58check (adds the 4-byte double-sha256 checksum). */
export function base58check(version: number, payload: Uint8Array): string {
	const data = new Uint8Array(1 + payload.length);
	data[0] = version;
	data.set(payload, 1);
	const checksum = sha256d(data).subarray(0, 4);
	const full = new Uint8Array(data.length + 4);
	full.set(data);
	full.set(checksum, data.length);
	return encodeBase58(full);
}

// ---------------------------------------------------------------------------
// bech32 / bech32m (segwit v0 / v1+)
// ---------------------------------------------------------------------------

const BECH32_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";
const BECH32_CONST = 1;
const BECH32M_CONST = 0x2bc830a3;

function polymod(values: number[]): number {
	const gen = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];
	let chk = 1;
	for (const v of values) {
		const top = chk >>> 25;
		chk = ((chk & 0x1ffffff) << 5) ^ v;
		for (let i = 0; i < 5; i++) {
			if ((top >>> i) & 1) chk ^= gen[i]!;
		}
	}
	return chk >>> 0;
}

function hrpExpand(hrp: string): number[] {
	const out: number[] = [];
	for (let i = 0; i < hrp.length; i++) out.push(hrp.charCodeAt(i) >>> 5);
	out.push(0);
	for (let i = 0; i < hrp.length; i++) out.push(hrp.charCodeAt(i) & 31);
	return out;
}

function convertBits(data: Uint8Array, from: number, to: number, pad: boolean): number[] {
	let acc = 0;
	let bits = 0;
	const out: number[] = [];
	const maxv = (1 << to) - 1;
	for (const value of data) {
		acc = (acc << from) | value;
		bits += from;
		while (bits >= to) {
			bits -= to;
			out.push((acc >>> bits) & maxv);
		}
	}
	if (pad && bits > 0) out.push((acc << (to - bits)) & maxv);
	return out;
}

/** Encode a segwit program (witness version + hash bytes) as a bech32/bech32m address. */
export function segwitAddress(hrp: string, witnessVersion: number, program: Uint8Array): string {
	const data = [witnessVersion, ...convertBits(program, 8, 5, true)];
	const spec = witnessVersion === 0 ? BECH32_CONST : BECH32M_CONST;
	const values = [...hrpExpand(hrp), ...data];
	const mod = polymod([...values, 0, 0, 0, 0, 0, 0]) ^ spec;
	const checksum: number[] = [];
	for (let i = 0; i < 6; i++) checksum.push((mod >>> (5 * (5 - i))) & 31);
	let out = `${hrp}1`;
	for (const d of [...data, ...checksum]) out += BECH32_CHARSET[d]!;
	return out;
}

// ---------------------------------------------------------------------------
// script → address (mainnet)
// ---------------------------------------------------------------------------

const P2PKH_VERSION = 0x00;
const P2SH_VERSION = 0x05;
const HRP = "bc";

export type DecodedScript = {
	script: ScriptPubKey;
	/** Canonical address string, or null for scripts that have no address (OP_RETURN, raw). */
	address: string | null;
	/** Short human label for the output type: "P2WPKH", "OP_RETURN", "Nonstandard", … */
	kind: string;
};

/** Detect the output type and render its canonical mainnet address (when it has one). */
export function decodeScriptPubKey(raw: Uint8Array): DecodedScript {
	const script = parseScriptPubKey(raw);
	switch (script.kind) {
		case "p2pkh":
			return { script, address: base58check(P2PKH_VERSION, script.value), kind: "P2PKH" };
		case "p2sh":
			return { script, address: base58check(P2SH_VERSION, script.value), kind: "P2SH" };
		case "p2wpkh":
			return { script, address: segwitAddress(HRP, 0, script.value), kind: "P2WPKH" };
		case "p2wsh":
			return { script, address: segwitAddress(HRP, 0, script.value), kind: "P2WSH" };
		case "p2tr":
			return { script, address: segwitAddress(HRP, 1, script.value), kind: "P2TR" };
		case "op_return":
			return { script, address: null, kind: "OP_RETURN" };
		case "raw":
			return { script, address: null, kind: "Nonstandard" };
	}
}

// ---------------------------------------------------------------------------
// minimal script disassembler (for raw / OP_RETURN inspection)
// ---------------------------------------------------------------------------

const OPCODES: Record<number, string> = {
	0x00: "OP_0",
	0x4c: "OP_PUSHDATA1",
	0x4d: "OP_PUSHDATA2",
	0x4e: "OP_PUSHDATA4",
	0x4f: "OP_1NEGATE",
	0x51: "OP_1",
	0x52: "OP_2",
	0x53: "OP_3",
	0x60: "OP_16",
	0x61: "OP_NOP",
	0x63: "OP_IF",
	0x64: "OP_NOTIF",
	0x67: "OP_ELSE",
	0x68: "OP_ENDIF",
	0x69: "OP_VERIFY",
	0x6a: "OP_RETURN",
	0x76: "OP_DUP",
	0x87: "OP_EQUAL",
	0x88: "OP_EQUALVERIFY",
	0xa9: "OP_HASH160",
	0xac: "OP_CHECKSIG",
	0xad: "OP_CHECKSIGVERIFY",
	0xae: "OP_CHECKMULTISIG",
	0xba: "OP_CHECKSIGADD",
};

/** Disassemble a script into a compact opcode/pushdata string. Truncates huge pushes. */
export function disassemble(script: Uint8Array, maxPushHex = 64): string {
	const parts: string[] = [];
	let i = 0;
	while (i < script.length) {
		const op = script[i++]!;
		if (op >= 0x01 && op <= 0x4b) {
			const data = script.subarray(i, i + op);
			i += op;
			const hex = encodeHex(data);
			parts.push(hex.length > maxPushHex ? `<${op}b:${hex.slice(0, maxPushHex)}…>` : `<${hex}>`);
		} else if (op === 0x4c || op === 0x4d || op === 0x4e) {
			const n = op === 0x4c ? 1 : op === 0x4d ? 2 : 4;
			let len = 0;
			for (let k = 0; k < n; k++) len |= script[i + k]! << (8 * k);
			i += n;
			const data = script.subarray(i, i + len);
			i += len;
			const hex = encodeHex(data);
			parts.push(`${OPCODES[op]} <${len}b:${hex.slice(0, maxPushHex)}${hex.length > maxPushHex ? "…" : ""}>`);
		} else {
			parts.push(OPCODES[op] ?? `OP_UNKNOWN(0x${op.toString(16)})`);
		}
	}
	return parts.join(" ");
}
