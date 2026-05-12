import { getTxByPointer } from "~/chain.ts";
import { SequenceLock } from "~/lib/codec/SequenceLock.ts";

export type OutPoint = {
	txId:
		| { kind: "pointer"; value: number }
		| { kind: "raw"; value: Uint8Array };
	vout: number;
};

export type TxInputData = {
	prevOut: OutPoint;
	scriptSig: Uint8Array;
	sequence: SequenceLock;
	witness: Uint8Array[];
};

export class TxInput {
	public data: TxInputData;

	constructor(data: TxInputData) {
		this.data = data;
	}

	public async getPrevOutTxId(): Promise<Uint8Array> {
		const txId = this.data.prevOut.txId;
		const { kind, value } = txId;
		if (kind === "raw") {
			return value;
		}

		if (kind === "pointer") {
			return await getTxByPointer(value).then((tx) => tx.data.txId);
		}

		throw new Error(`getPrevOutTxId doesn't handle txId kind: ${kind satisfies never}`);
	}
}
