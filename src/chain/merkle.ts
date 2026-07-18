import { sha256 } from "@noble/hashes/sha2";
import { equals } from "@std/bytes";
import type { WireTxs } from "~/codec/wire/WireTxs.ts";

// ---------------------------------------------------------------------------
// Module-level scratch, reused across calls.
//
// Safe because verifySatoshiMerkleRoot is FULLY SYNCHRONOUS: it never awaits,
// so one call cannot interleave with itself on a single isolate, and each Deno
// Worker gets its own module instance (parallel IBD workers do NOT share these).
// The one invariant that keeps this safe: nothing in the hot path may yield.
// If this function ever becomes async, these buffers become a data race.
// ---------------------------------------------------------------------------

const ZERO_ROOT = new Uint8Array(32); // empty-tree root; compared, never rebuilt

// Level-1 holds ceil(n/2) hashes. 16384 covers up to 32768 leaves — ~2x any
// real block (consensus weight limits keep tx count well under this). Grows
// on demand if a block ever exceeds it (near-never after warmup).
const LEVEL1_CAP = 16384;
let scratch = new Uint8Array(LEVEL1_CAP * 32);

const pair = new Uint8Array(64); // hash input: a ‖ b
const tmpInner = new Uint8Array(32); // inner SHA256 output

/**
 * Verifies a block's transaction list against an expected merkle root using
 * Bitcoin's consensus construction (double-SHA256, duplicate-last-on-odd),
 * rejecting CVE-2012-2459 sibling-duplication mutations.
 *
 * Returns `false` for BOTH a mutated tree and a plain root mismatch — a
 * validator rejects the block either way. If you ever need to distinguish
 * malice (mutation, ban-worthy) from innocent corruption (mismatch), switch
 * this to an enum/tagged return.
 *
 * IMPORTANT: `txId` MUST be internal (little-endian) byte order — the form used
 * in consensus, NOT the byte-reversed display order shown by RPC/explorers.
 * Feeding display-order txids produces roots that match nothing.
 */
export function verifySatoshiMerkleRoot(txs: WireTxs, expected: Uint8Array): boolean {
	const n = txs.length;
	if (n === 0) return equals(expected, ZERO_ROOT);
	if (n === 1) return equals(expected, txs.at(0)!.txId); // lone leaf is the root

	// grow scratch only if a block exceeds the cap (effectively never post-warmup)
	const needed = Math.ceil(n / 2) * 32;
	if (needed > scratch.length) scratch = new Uint8Array(needed);

	// level 0 → level 1: read txids from txs, write hashes into scratch
	let count = 0;
	for (let i = 0; i < n; i += 2) {
		const a = txs.at(i)!.txId;
		if (i + 1 < n) {
			const b = txs.at(i + 1)!.txId;
			if (equals(a, b)) return false; // mutated tree — real sibling pair collides
			hashPair(count++, a, b);
		} else {
			hashPair(count++, a, a); // duplicate last (odd width) — NOT a mutation
		}
	}

	// deeper levels: read pairs from scratch, write back into its front.
	// write index w is always <= read index i, so in-place never clobbers a
	// slice we haven't consumed yet.
	while (count > 1) {
		let w = 0;
		for (let i = 0; i < count; i += 2) {
			const aOff = i * 32;
			if (i + 1 < count) {
				const bOff = (i + 1) * 32;
				if (equalsAt(aOff, bOff)) return false; // mutation at an interior level
				hashPairAt(w++, aOff, bOff);
			} else {
				hashPairAt(w++, aOff, aOff); // duplicate last — NOT a mutation
			}
		}
		count = w;
	}

	return equals(scratch.subarray(0, 32), expected);
}

// ---------------------------------------------------------------------------
// Hashing helpers.
//
// `sha256.create()` per call allocates only small, non-escaping, nursery-
// collected objects. The allocation that mattered — the fresh output arrays a plain
// sha256() would return and that escape into `scratch` — is eliminated by
// digesting straight into the target buffer. Not worth chasing the hasher
// objects via noble's `_cloneInto`: it's unstable internal API, and merkle
// hashing is not on the IBD hot path (disk I/O is). Revisit only if a real
// profile says otherwise — and pin the noble version if you do.
// ---------------------------------------------------------------------------

// hash two external 32-byte inputs → scratch[dst]
function hashPair(dst: number, a: Uint8Array, b: Uint8Array): void {
	pair.set(a, 0);
	pair.set(b, 32);
	sha256d(scratch.subarray(dst * 32, dst * 32 + 32));
}

// hash two 32-byte slices already in scratch (by byte offset) → scratch[dst]
function hashPairAt(dst: number, aOff: number, bOff: number): void {
	pair.set(scratch.subarray(aOff, aOff + 32), 0);
	pair.set(scratch.subarray(bOff, bOff + 32), 32);
	sha256d(scratch.subarray(dst * 32, dst * 32 + 32));
}

// double-SHA256 of `pair` (64B) → `out` (32B). `pair` is fully consumed by the
// inner hash before `out` is written, so out may alias into scratch safely.
function sha256d(out: Uint8Array): void {
	sha256.create().update(pair).digestInto(tmpInner);
	sha256.create().update(tmpInner).digestInto(out);
}

// compare two 32-byte slices within scratch by offset, no view allocation
function equalsAt(aOff: number, bOff: number): boolean {
	for (let i = 0; i < 32; i++) {
		if (scratch[aOff + i] !== scratch[bOff + i]) return false;
	}
	return true;
}
