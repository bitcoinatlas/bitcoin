import { ArrayCodec, BytesCodec, Codec, U32LE } from "@nomadshiba/codec";
import { Bytes32, CompactSize } from "~/lib/codec/primitives.ts";
import { StoredTxOutput } from "~/lib/codec/StoredTxOutput.ts";
import type { TxOutput } from "~/lib/chain/TxOutput.ts";

// Per block optimizations like coinbase transaction, doesn't save that much space,
// But its easy to implement so why not. Why store 0s randomly in the middle of the chunk?
export type StoredCoinbaseTx = {
	txId: Uint8Array;
	version: number;
	lockTime: number;
	sequence: number;
	coinbase: Uint8Array;
	vout: TxOutput[];
};

// Re-export for convenience
export { StoredTxOutput } from "~/lib/codec/StoredTxOutput.ts";

// Bytes codecs
const scriptBytes = new BytesCodec();

// Array using CompactSize for count
const voutArray = new ArrayCodec(StoredTxOutput, { countCodec: CompactSize });

export class StoredCoinbaseTxCodec extends Codec<StoredCoinbaseTx> {
	readonly stride = -1;

	encode(value: StoredCoinbaseTx): Uint8Array {
		const chunks: Uint8Array[] = [];

		chunks.push(Bytes32.encode(value.txId));
		chunks.push(U32LE.encode(value.version));
		chunks.push(U32LE.encode(value.lockTime));
		chunks.push(U32LE.encode(value.sequence));
		chunks.push(scriptBytes.encode(value.coinbase));
		chunks.push(voutArray.encode(value.vout));

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

	decode(data: Uint8Array): [StoredCoinbaseTx, number] {
		let offset = 0;

		const [txId] = Bytes32.decode(data.subarray(offset));
		offset += 32;

		const [version] = U32LE.decode(data.subarray(offset));
		offset += 4;

		const [lockTime] = U32LE.decode(data.subarray(offset));
		offset += 4;

		const [sequence] = U32LE.decode(data.subarray(offset));
		offset += 4;

		const [coinbase, coinbaseBytes] = scriptBytes.decode(data.subarray(offset));
		offset += coinbaseBytes;

		const [vout, voutBytes] = voutArray.decode(data.subarray(offset));
		offset += voutBytes;

		return [
			{
				txId,
				version,
				lockTime,
				sequence,
				coinbase,
				vout,
			},
			offset,
		];
	}
}

export const StoredCoinbaseTx = new StoredCoinbaseTxCodec();
