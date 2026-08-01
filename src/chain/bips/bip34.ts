import { equals } from "@std/bytes";
import { ScriptNum } from "~/codec/primitives/ScriptNum.ts";

/**
 * BIP34 — "Block v2, Height in Coinbase".
 *
 * From its activation height, every block's coinbase scriptSig must BEGIN with
 * the block height, serialized as a Script number push. This makes every
 * coinbase txid unique from then on (two blocks at different heights can no
 * longer share a coinbase), which is what lets BIP30's duplicate-txid check be
 * assumed satisfied afterwards (see ./bip30.ts).
 *
 * Mirrors Bitcoin Core: consensus.BIP34Height (kernel/chainparams.cpp) and the
 * "bad-cb-height" check in ContextualCheckBlock (validation.cpp).
 */

/** Mainnet BIP34 (DEPLOYMENT_HEIGHTINCB) activation height. */
export const BIP34_HEIGHT = 227931;

/**
 * The expected coinbase scriptSig prefix at `height`: `CScript() << nHeight` —
 * a minimal little-endian `CScriptNum` of the height, prefixed by its byte
 * length as a direct-push opcode. The real coinbase scriptSig must start with
 * exactly these bytes.
 */
export function bip34ExpectedCoinbasePrefix(height: number): Uint8Array {
	const num = ScriptNum.encode(height);
	// Heights are always > 0 and well under OP_PUSHDATA1 (0x4c) territory for any
	// realistic chain, so a plain direct-push opcode (== byte length) is the
	// minimal encoding Core uses. Guard anyway.
	if (num.length >= 0x4c) throw new Error(`bip34: height ${height} scriptnum too large for direct push`);
	const out = new Uint8Array(1 + num.length);
	out[0] = num.length;
	out.set(num, 1);
	return out;
}

/**
 * Verify a coinbase scriptSig satisfies BIP34 at `height`. No-op below
 * activation. Throws on mismatch (a consensus failure), mirroring Core's
 * "bad-cb-height".
 */
export function checkBip34CoinbaseHeight(height: number, coinbaseScriptSig: Uint8Array): void {
	if (height < BIP34_HEIGHT) return;
	const expect = bip34ExpectedCoinbasePrefix(height);
	if (
		coinbaseScriptSig.length < expect.length ||
		!equals(coinbaseScriptSig.subarray(0, expect.length), expect)
	) {
		throw new Error(`bip34: block height mismatch in coinbase at height ${height} (bad-cb-height)`);
	}
}
