import { encodeHex } from "@std/encoding";
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

export function formatBytes(bytes: number, base: number, units: string[]): string {
	if (bytes === 0) return `0 ${units[0]}`;
	const i = Math.floor(Math.log(Math.abs(bytes)) / Math.log(base));
	const value = bytes / base ** i;
	return `${value.toFixed(i === 0 ? 0 : 2)} ${units[i]}`;
}

/** Binary — MiB, KiB, etc. (1024-base). */
export function formatBytesBinary(bytes: number): string {
	return formatBytes(bytes, 1024, ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]);
}

/** Decimal — MB, KB, etc. (1000-base). */
export function formatBytesDecimal(bytes: number): string {
	return formatBytes(bytes, 1000, ["B", "KB", "MB", "GB", "TB", "PB"]);
}

/** Format a satoshi amount as a plain BTC number (no unit suffix), trimming trailing zeros. */
export function formatBtcValue(satoshis: bigint): string {
	const neg = satoshis < 0n;
	const abs = neg ? -satoshis : satoshis;
	const whole = abs / 100_000_000n;
	const frac = (abs % 100_000_000n).toString().padStart(8, "0").replace(/0+$/, "");
	return `${neg ? "-" : ""}${whole}${frac ? `.${frac}` : ""}`;
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
