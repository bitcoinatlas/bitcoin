// Byte array comparison utility

/**
 * Compare two Uint8Arrays lexicographically.
 * Returns -1 if a < b, 0 if a === b, 1 if a > b.
 *
 * Note: For equality checks, use `equal` from `jsr:@std/bytes` instead.
 * For hex conversion, use Deno's unstable API.
 */
export function compare(a: Uint8Array, b: Uint8Array): number {
	const len = Math.min(a.length, b.length);
	for (let i = 0; i < len; i++) {
		if (a[i] !== b[i]) {
			return (a[i]! - b[i]!) < 0 ? -1 : 1;
		}
	}
	return a.length - b.length;
}
