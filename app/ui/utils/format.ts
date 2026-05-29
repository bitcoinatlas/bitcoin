/**
 * Format a txid / block hash bytes for display.
 * Internally hashes are stored in wire format (little-endian for txid/blockhash).
 * The human-readable convention is reversed (big-endian hex).
 */
export function formatHash(bytes: Uint8Array): string {
	return Array.from(bytes).reverse().map(b => b.toString(16).padStart(2, '0')).join('')
}

/**
 * Format a satoshi value as BTC with up to 8 decimal places.
 */
export function formatBtc(satoshis: bigint): string {
	const whole = satoshis / 100_000_000n
	const frac = satoshis % 100_000_000n
	return `${whole}.${frac.toString().padStart(8, '0')} BTC`
}
