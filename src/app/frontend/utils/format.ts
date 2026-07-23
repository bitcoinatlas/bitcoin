import { encodeHex } from "@std/encoding";
import { HexEncoderStream } from "@std/encoding/unstable-hex-stream";
import type { LockTime } from "~/codec/LockTime.ts";
import type { SequenceLock } from "~/codec/SequenceLock.ts";
import { SECOND } from "~/constants.ts";

export const LOCALE = new Intl.Locale("en-US");

export function formatHash(bytes: Uint8Array): string {
	return encodeHex(bytes.toReversed());
}

/**
 * Format a LockTime for display.
 */
export function formatLocktime(lock: LockTime): string {
	switch (lock.kind) {
		case "none":
			return "none";
		case "block":
			return `block ${lock.height}`;
		case "time":
			return `time ${new Date(lock.timestamp * SECOND).toISOString()}`;
	}
}

/**
 * Format a SequenceLock for display.
 */
export function formatSequence(seq: SequenceLock): string {
	switch (seq.kind) {
		case "final":
			return "final (0xffffffff)";
		case "disable":
			return `disabled (raw: 0x${((seq.unused | 0x80000000) >>> 0).toString(16)})`;
		case "enable":
			if (seq.relativeLock.kind === "block") {
				return `relative lock: ${seq.relativeLock.blocks} blocks`;
			} else {
				return `relative lock: ${seq.relativeLock.seconds}s`;
			}
	}
}

/**
 * Format a satoshi value as BTC with up to 8 decimal places.
 */
export function formatBtc(satoshis: bigint): string {
	const whole = satoshis / 100_000_000n;
	const frac = satoshis % 100_000_000n;
	return `${whole}.${frac.toString().padStart(8, "0")} BTC`;
}

/** Truncate a long id in the middle: 0000abcd… ef123456 */
export function truncateMiddle(str: string, head = 10, tail = 8): string {
	if (str.length <= head + tail + 1) return str;
	return `${str.slice(0, head)}…${str.slice(-tail)}`;
}

/** Group an integer with thin separators: 958399 -> "958,399". */
export function formatNumber(n: number | bigint): string {
	return n.toLocaleString("en-US");
}

/**
 * Format a byte count using binary (1024-base) units — KiB/MiB/GiB, never SI.
 * BitcoinAtlas measures everything in IEC units.
 */
export function formatBytes(bytes: number): string {
	if (bytes < 1024) return `${bytes} B`;
	const units = ["KiB", "MiB", "GiB", "TiB"];
	let value = bytes / 1024;
	let unit = 0;
	while (value >= 1024 && unit < units.length - 1) {
		value /= 1024;
		unit++;
	}
	const digits = value >= 100 ? 0 : value >= 10 ? 1 : 2;
	return `${value.toFixed(digits)} ${units[unit]}`;
}

/** Format a satoshi amount as a plain BTC number (no unit suffix), trimming trailing zeros. */
export function formatBtcValue(satoshis: bigint): string {
	const neg = satoshis < 0n;
	const abs = neg ? -satoshis : satoshis;
	const whole = abs / 100_000_000n;
	const frac = (abs % 100_000_000n).toString().padStart(8, "0").replace(/0+$/, "");
	return `${neg ? "-" : ""}${whole}${frac ? `.${frac}` : ""}`;
}

/** Turn an nBits compact target into a difficulty relative to difficulty-1 (0x1d00ffff). */
export function difficultyFromBits(bits: number): number {
	const exponent = bits >>> 24;
	const mantissa = bits & 0x00ffffff;
	// difficulty_1 = 0xffff * 2^(8*(0x1d-3)); current = mantissa * 2^(8*(exp-3))
	const current = mantissa * Math.pow(2, 8 * (exponent - 3));
	const diff1 = 0xffff * Math.pow(2, 8 * (0x1d - 3));
	return diff1 / current;
}

/** Human-readable large number with metric suffix (for difficulty / hashrate). */
export function formatBig(n: number): string {
	if (!isFinite(n)) return "—";
	const units = ["", "K", "M", "G", "T", "P", "E", "Z"];
	let value = n;
	let unit = 0;
	while (value >= 1000 && unit < units.length - 1) {
		value /= 1000;
		unit++;
	}
	return `${value.toFixed(value >= 100 || unit === 0 ? 0 : 2)}${units[unit]}`;
}

/** Block subsidy in satoshis for a given height (halving every 210_000). */
export function blockSubsidy(height: number): bigint {
	const halvings = Math.floor(height / 210_000);
	if (halvings >= 64) return 0n;
	return 50_00000000n >> BigInt(halvings);
}

/** Absolute UTC timestamp string from a unix-seconds value. */
export function formatUtc(unixSeconds: number): string {
	return new Date(unixSeconds * 1000).toISOString().replace("T", " ").replace(".000Z", " UTC");
}

/** Printable-ASCII fragments pulled out of arbitrary script bytes (coinbase tags, OP_RETURN memos). */
export function extractAscii(bytes: Uint8Array, minRun = 3): string {
	let out = "";
	let run = "";
	for (const b of bytes) {
		if (b >= 0x20 && b <= 0x7e) {
			run += String.fromCharCode(b);
		} else {
			if (run.length >= minRun) out += out ? ` ${run}` : run;
			run = "";
		}
	}
	if (run.length >= minRun) out += out ? ` ${run}` : run;
	return out;
}

const blockHeightFormatter = new Intl.NumberFormat(LOCALE, { style: "decimal" });
export function formatBlockHeight(height: number | bigint | Intl.StringNumericLiteral): string {
	return blockHeightFormatter.format(height);
}
