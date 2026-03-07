import { WireTxInput } from "../codec/WireTxInput.ts";
import { SequenceLock } from "./utils/SequenceLock.ts";

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
		if (this.data.prevOut.txId.kind === "raw") {
			return this.data.prevOut.txId.value;
		}
		// In a real implementation, you would fetch the txId from storage using the pointer value.
		// For this example, we'll just throw an error to indicate that this is not implemented.
		throw new Error("Pointer txId not implemented");
	}
}
