/**
 * BIP30 — no block may contain a transaction whose txid duplicates an existing
 * transaction with unspent outputs.
 *
 * Early coinbases had freely-chosen scriptSigs, so two different blocks could
 * (and did) mint coinbases with IDENTICAL txids (CVE-2012-1909). BIP30 bans
 * that. Once BIP34 (see ./bip34.ts) put the height in every coinbase, duplicate
 * coinbases became impossible and BIP30 is automatically satisfied.
 *
 * But BIP34 activated AFTER two historical duplicate-coinbase pairs already
 * existed on mainnet, so those two DUPLICATING blocks are hard-coded exceptions
 * where the new coinbase is allowed to OVERWRITE the earlier identical one (the
 * earlier one's outputs were already spent, so nothing is lost).
 *
 * Mirrors Bitcoin Core's `IsBIP30Repeat` (validation.cpp): the two duplicating
 * blocks (91842 overwrites 91722, 91880 overwrites 91812).
 */

import { decodeHex } from "@std/encoding";
import { equals } from "@std/bytes";

/**
 * The two mainnet blocks ALLOWED to carry a coinbase whose txid duplicates an
 * earlier coinbase. Keyed by the duplicating block's height → its block hash in
 * WIRE (little-endian) order — the hashes are written here in RPC display order
 * and reversed once at load, so lookups compare directly against the wire-order
 * hash stored in the header domain.
 */
export const BIP30_EXCEPTION_BLOCKS: ReadonlyMap<number, Uint8Array> = new Map([
	[91842, decodeHex("00000000000a4d0a398161ffc163c503763b1f4360639393e0e4c8e300e0caec").reverse()],
	[91880, decodeHex("00000000000743f190a18c5577a3c2d2a1f610ae9601ac046a38084ccb7cd721").reverse()],
]);

/**
 * Is `height` one of the two BIP30-exempt duplicating blocks, and does its hash
 * match the expected mainnet block? `blockHashWire` is the 32-byte
 * double-SHA256 of the header in WIRE (little-endian) order — the same bytes
 * stored in the header/blockhash domain. Matching the hash (not just the height)
 * stops us overwriting a txid on some other/forked chain.
 */
export function isBip30Exception(height: number, blockHashWire: Uint8Array): boolean {
	const expected = BIP30_EXCEPTION_BLOCKS.get(height);
	if (expected === undefined) return false;
	return equals(blockHashWire, expected);
}
