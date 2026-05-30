import { SequenceLock } from "~/lib/codec/SequenceLock.ts";
import { COINBASE_TXID } from "~/constants.ts";

export type OutPoint = {
	txId:
		| { kind: "pointer"; value: number }
		| { kind: "raw"; value: Uint8Array }
		| { kind: "coinbase"; value?: undefined };
	vout: number;
};

export type TxInput = {
	prevOut: OutPoint;
	scriptSig: Uint8Array;
	sequence: SequenceLock;
	witness: Uint8Array[];
};

export async function getPrevOutTxId(input: TxInput): Promise<Uint8Array> {
	const txId = input.prevOut.txId;
	const { kind, value } = txId;
	if (kind === "raw") {
		return value;
	}

	if (kind === "pointer") {
		const { getTxByPointer } = await import("~/chain.ts");
		return await getTxByPointer(value).then((tx) => tx.data.txId);
	}

	if (kind === "coinbase") {
		return COINBASE_TXID;
	}

	throw new Error(`getPrevOutTxId doesn't handle txId kind: ${kind satisfies never}`);
}
