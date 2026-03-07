import { ArrayCodec, Codec } from "@nomadshiba/codec";
import { CompactSize, U24LE } from "~/lib/codec/primitives.ts";
import { StoredCoinbaseTx } from "~/lib/chain/codec/stored/StoredCoinbaseTx.ts";
import { StoredTx } from "~/lib/chain/codec/stored/StoredTx.ts";

export type { StoredCoinbaseTx } from "./StoredCoinbaseTx.ts";

export type StoredBlock = {
	coinbaseTx: StoredCoinbaseTx;
	transactions: StoredTx[];
};

// Array of transactions using CompactSize
const transactionsArray = new ArrayCodec(StoredTx, { countCodec: CompactSize });

export class StoredBlockCodec extends Codec<StoredBlock> {
	readonly stride = -1;

	encode(value: StoredBlock): Uint8Array {
		const chunks: Uint8Array[] = [];

		// Encode transaction count (U24LE for consistency with original)
		chunks.push(U24LE.encode(value.transactions.length));

		// Encode coinbase transaction
		chunks.push(StoredCoinbaseTx.encode(value.coinbaseTx));

		// Encode remaining transactions
		chunks.push(transactionsArray.encode(value.transactions));

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

	decode(data: Uint8Array): [StoredBlock, number] {
		let offset = 0;

		// Decode transaction count
		const [txCount] = U24LE.decode(data.subarray(offset));
		offset += 3;

		// Decode coinbase transaction
		const [coinbaseTx, coinbaseBytes] = StoredCoinbaseTx.decode(data.subarray(offset));
		offset += coinbaseBytes;

		// Decode remaining transactions
		const transactions: StoredTx[] = [];
		for (let i = 0; i < txCount; i++) {
			const [tx, txBytes] = StoredTx.decode(data.subarray(offset));
			transactions.push(tx);
			offset += txBytes;
		}

		return [
			{
				coinbaseTx,
				transactions,
			},
			offset,
		];
	}
}

export const storedBlock = new StoredBlockCodec();
