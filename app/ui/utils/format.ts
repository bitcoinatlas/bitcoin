/**
 * Format a txid / block hash bytes for display.
 * Internally hashes are stored in wire format (little-endian for txid/blockhash).
 * The human-readable convention is reversed (big-endian hex).
 */
export function formatHash(bytes: Uint8Array): string {
	return Array.from(bytes).reverse().map(b => b.toString(16).padStart(2, '0')).join('')
}

import type { TimeLock } from "~/lib/codec/TimeLock.ts";
import type { SequenceLock } from "~/lib/codec/SequenceLock.ts";

/**
 * Format a TimeLock for display.
 */
export function formatLocktime(lock: TimeLock): string {
	switch (lock.kind) {
		case "none": return "none";
		case "block": return `block ${lock.height}`;
		case "time": return `time ${new Date(lock.timestamp * 1000).toISOString()}`;
	}
}

/**
 * Format a SequenceLock for display.
 */
export function formatSequence(seq: SequenceLock): string {
	switch (seq.kind) {
		case "final": return "final (0xffffffff)";
		case "disable": return `disabled (raw: 0x${((seq.unused | 0x80000000) >>> 0).toString(16)})`;
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
	const whole = satoshis / 100_000_000n
	const frac = satoshis % 100_000_000n
	return `${whole}.${frac.toString().padStart(8, '0')} BTC`
}
