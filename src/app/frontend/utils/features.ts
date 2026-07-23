import type { WireTx } from "~/codec/wire/WireTx.ts";
import { SequenceLockCodec } from "~/codec/SequenceLock.ts";
import { parseScriptPubKey } from "~/chain/ScriptPubKey.ts";
import { COINBASE_TXID, COINBASE_VOUT } from "~/constants.ts";

export type FeatureTag =
	| "coinbase"
	| "segwit"
	| "taproot"
	| "rbf"
	| "data"
	| "locktime"
	| "inscription"
	| "multisig";

const RBF_THRESHOLD = 0xfffffffe;

function isCoinbaseInput(tx: WireTx): boolean {
	const first = tx.inputs[0];
	if (!first) return false;
	if (first.prevOut.output !== COINBASE_VOUT) return false;
	const id = first.prevOut.txId;
	if (id.length !== COINBASE_TXID.length) return false;
	for (let i = 0; i < id.length; i++) {
		if (id[i] !== 0) return false;
	}
	return true;
}

/** Heuristic ordinals/inscription detector: a taproot script-path reveal envelope. */
function looksLikeInscription(tx: WireTx): boolean {
	for (const stack of tx.witness) {
		for (const item of stack) {
			if (item.length < 8) continue;
			let sawIf = false;
			// scan for OP_IF (0x63) then an "ord" ascii marker shortly after — the
			// standard `OP_FALSE OP_IF "ord" … OP_ENDIF` envelope.
			for (let i = 0; i < item.length - 3; i++) {
				if (item[i] === 0x63) sawIf = true;
				if (sawIf && item[i] === 0x6f && item[i + 1] === 0x72 && item[i + 2] === 0x64) return true;
			}
		}
	}
	return false;
}

/** Bare (non-P2SH) multisig output detector: pushes ending in OP_CHECKMULTISIG. */
function hasBareMultisig(tx: WireTx): boolean {
	for (const out of tx.outputs) {
		const s = out.scriptPubKey;
		if (s.length >= 4 && s[s.length - 1] === 0xae && s[0]! >= 0x51 && s[0]! <= 0x60) return true;
	}
	return false;
}

/** Detect the structural feature tags carried by a single transaction. */
export function detectTxFeatures(tx: WireTx, index: number): FeatureTag[] {
	const tags: FeatureTag[] = [];

	const coinbase = index === 0 || isCoinbaseInput(tx);
	if (coinbase) tags.push("coinbase");

	if (tx.witness.some((stack) => stack.some((item) => item.length > 0))) tags.push("segwit");

	let taproot = false;
	let data = false;
	for (const out of tx.outputs) {
		const kind = parseScriptPubKey(out.scriptPubKey).kind;
		if (kind === "p2tr") taproot = true;
		if (kind === "op_return") data = true;
	}
	if (taproot) tags.push("taproot");
	if (data) tags.push("data");

	if (!coinbase) {
		for (const input of tx.inputs) {
			if ((SequenceLockCodec.toU32(input.sequence) >>> 0) < RBF_THRESHOLD) {
				tags.push("rbf");
				break;
			}
		}
	}

	if (tx.locktime.kind !== "none") tags.push("locktime");
	if (looksLikeInscription(tx)) tags.push("inscription");
	if (hasBareMultisig(tx)) tags.push("multisig");

	return tags;
}

/** Union of feature tags across a whole block's transactions, in a stable display order. */
export function aggregateBlockFeatures(txs: WireTx[]): FeatureTag[] {
	const order: FeatureTag[] = ["segwit", "taproot", "rbf", "data", "inscription", "multisig", "locktime"];
	const present = new Set<FeatureTag>();
	txs.forEach((tx, i) => detectTxFeatures(tx, i).forEach((t) => present.add(t)));
	return order.filter((t) => present.has(t));
}

/** Total satoshis paid to the outputs of a transaction. */
export function totalOut(tx: WireTx): bigint {
	let sum = 0n;
	for (const out of tx.outputs) sum += out.value;
	return sum;
}
