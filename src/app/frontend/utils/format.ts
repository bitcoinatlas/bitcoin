import { encodeHex } from "@std/encoding";
import { U32 } from "@nomadshiba/codec";
import type { LockTime } from "~/codec/LockTime.ts";
import type { SequenceLock } from "~/codec/SequenceLock.ts";
import { DAY, HOUR, MINUTE, MONTH, SECOND, WEEK, YEAR } from "~/constants.ts";
import { BigNumberFormat } from "~/app/frontend/utils/intl/BigNumberFormat.ts";

export const LOCALE = new Intl.Locale("en-US");

export function formatHash(bytes: Uint8Array): string {
	return encodeHex(bytes.toReversed());
}

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

const bytesBinaryFormatter = new BigNumberFormat(LOCALE, { base: 1024, units: ["B", "KiB", "MiB", "GiB", "TiB", "PiB"] });
export function formatBytesBinary(bytes: number): string {
	return bytesBinaryFormatter.format(bytes);
}

const bytesDecimalFormatter = new BigNumberFormat(LOCALE, { base: 1000, units: ["B", "KB", "MB", "GB", "TB", "PB"] });
export function formatBytesDecimal(bytes: number): string {
	return bytesDecimalFormatter.format(bytes);
}

const dateTimeFormatter = new Intl.DateTimeFormat(LOCALE);
export function formatDateTime(value: number | Date | string): string {
	const date = value instanceof Date ? value : new Date(value);
	return dateTimeFormatter.format(date);
}

const relativeTimeFormatter = new Intl.RelativeTimeFormat(LOCALE, { numeric: "auto" });
const RELATIVE_TIME_UNITS = [
	["year", YEAR],
	["month", MONTH],
	["week", WEEK],
	["day", DAY],
	["hour", HOUR],
	["minute", MINUTE],
	["second", SECOND],
] as const;
export function formatRelativeTime(to: Date, from: Date = new Date()): string {
	const diff = to.getTime() - from.getTime();
	for (const [unit, msInUnit] of RELATIVE_TIME_UNITS) {
		const diffInUnits = diff / msInUnit;
		if (Math.abs(diffInUnits) >= 1) {
			return relativeTimeFormatter.format(Math.round(diffInUnits), unit);
		}
	}
	return relativeTimeFormatter.format(0, "second"); // fallback: "now"
}

const blockHeightFormatter = new Intl.NumberFormat(LOCALE, { style: "decimal" });
export function formatBlockHeight(height: number | bigint | Intl.StringNumericLiteral): string {
	return blockHeightFormatter.format(height);
}

export function formatBlockVersion(version: number): string {
	return `0x${encodeHex(U32.encode(version))}`;
}

const difficultyFormatter = new BigNumberFormat(LOCALE, { base: 1000, units: ["", "K", "M", "G", "T", "P", "E", "Z"], separator: "" });
export function formatDifficulty(n: number): string {
	return difficultyFormatter.format(n);
}

const hashrateFormatter = new BigNumberFormat(LOCALE, { base: 1000, units: ["", "K", "M", "G", "T", "P", "E", "Z"], separator: "" });
export function formatHashrate(n: number): string {
	return hashrateFormatter.format(n);
}

// TODO: this should probably moved to shared space, protocol also gonna use this.
export function blockSubsidy(height: number): bigint {
	const halvings = Math.floor(height / 210_000);
	if (halvings >= 64) return 0n;
	return 50_00000000n >> BigInt(halvings);
}

const btcFormatter = new Intl.NumberFormat(LOCALE, { style: "currency", currency: "BTC" });
export function formatBitcoin(sats: bigint) {
	const btc = Number(sats) / 100_000_000;
	return btcFormatter.format(btc);
}
