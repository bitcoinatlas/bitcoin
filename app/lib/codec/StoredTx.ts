import { ArrayCodec, Codec, u32LE } from "@nomadshiba/codec";
import { bytes32, compactSize } from "~/lib/codec/primitives.ts";
import { StoredTxInput, storedTxInput } from "~/lib/codec/StoredTxInput.ts";
import { StoredTxOutput, storedTxOutput } from "~/lib/codec/StoredTxOutput.ts";

export type { StoredTxInput } from "~/lib/codec/StoredTxInput.ts";
export type { StoredTxOutput } from "~/lib/codec/StoredTxOutput.ts";

export type StoredTx = {
	// This is the only place where we store the full txId,
	// if we dont store it anywhere else, in order to find the txId,
	// we have to hash every transaction until the coinbase transactions of the utxo we are spending.
	txId: Uint8Array;
	version: number;
	lockTime: number;
	vout: StoredTxOutput[];
	vin: StoredTxInput[];
};

// Re-export from primitives for convenience
export { storedTxInput } from "~/lib/codec/StoredTxInput.ts";
export { storedTxOutput } from "~/lib/codec/StoredTxOutput.ts";

// Arrays using compactSize for count
const voutArray = new ArrayCodec(storedTxOutput, { countCodec: compactSize });
const vinArray = new ArrayCodec(storedTxInput, { countCodec: compactSize });

export class StoredTxCodec extends Codec<StoredTx> {
	readonly stride = -1;

	encode(value: StoredTx): Uint8Array {
		const chunks: Uint8Array[] = [];

		chunks.push(bytes32.encode(value.txId));
		chunks.push(u32LE.encode(value.version));
		chunks.push(u32LE.encode(value.lockTime));
		chunks.push(voutArray.encode(value.vout));
		chunks.push(vinArray.encode(value.vin));

		// Calculate total size and concatenate
		let totalLength = 0;
		for (const chunk of chunks) {
			totalLength += chunk.length;
		}
		const result = new Uint8Array(totalLength);
		let offset = 0;
		for (const chunk of chunks) {
			result.set(chunk, offset);
			offset += chunk.length;
		}
		return result;
	}

	decode(data: Uint8Array): [StoredTx, number] {
		let offset = 0;

		const [txId] = bytes32.decode(data.subarray(offset));
		offset += 32;

		const [version] = u32LE.decode(data.subarray(offset));
		offset += 4;

		const [lockTime] = u32LE.decode(data.subarray(offset));
		offset += 4;

		const [vout, voutBytes] = voutArray.decode(data.subarray(offset));
		offset += voutBytes;

		const [vin, vinBytes] = vinArray.decode(data.subarray(offset));
		offset += vinBytes;

		return [
			{
				txId,
				version,
				lockTime,
				vout,
				vin,
			},
			offset,
		];
	}
}

export const storedTx = new StoredTxCodec();
