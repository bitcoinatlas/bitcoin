import { TimeLock } from "./utils/TimeLock.ts";
import { TxInput } from "./TxInput.ts";
import { TxOutput } from "./TxOutput.ts";

export type TxData = {
	version: number;
	locktime: TimeLock;
	witness: boolean;
	inputs: TxInput[];
	output: TxOutput[];
};

export class Tx {
	public data: TxData;

	constructor(data: TxData) {
		this.data = data;
	}
}
