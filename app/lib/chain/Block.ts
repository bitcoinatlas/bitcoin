import { sha256 } from "@noble/hashes/sha2";
import { WireBlock } from "./codec/wire/WireBlock.ts";
import { WireTx } from "./codec/wire/WireTx.ts";
import type { StoredBlock } from "./codec/stored/StoredBlock.ts";
import { Tx } from "./Tx.ts";
import { WireBlockHeader } from "./codec/wire/WireBlockHeader.ts";

export class Block {
	public readonly header: WireBlockHeader;
	public readonly headerHash: Uint8Array;
	public readonly txs: Tx[];

	constructor(header: WireBlockHeader, txs: Tx[] = []) {
		this.header = header;
		this.txs = txs;
		// There are not many headers, so probably this wont be too slow, but in case it becomes an issue, do the same thing you did with the transactions.
		this.headerHash = sha256(sha256(WireBlock.shape.header.encode(header)));
	}

	async toWire(): Promise<WireBlock> {
		const wireTxs: WireTx[] = [];
		for (const tx of this.txs) {
			wireTxs.push(await tx.toWire());
		}
		return {
			header: this.header,
			txs: wireTxs,
		};
	}

	static async fromWire(wireBlock: WireBlock): Promise<Block> {
		const txs: Tx[] = [];
		for (const wireTx of wireBlock.txs) {
			txs.push(await Tx.fromWire(wireTx));
		}
		return new Block(wireBlock.header, txs);
	}

	async toStore(): Promise<StoredBlock> {
		if (this.txs.length === 0) {
			throw new Error("Cannot store block with no transactions");
		}

		const coinbaseStored = await this.txs[0]!.toStore();
		const otherTxs: Awaited<ReturnType<Tx["toStore"]>>[] = [];

		for (let i = 1; i < this.txs.length; i++) {
			otherTxs.push(await this.txs[i]!.toStore());
		}

		return {
			coinbaseTx: {
				txId: coinbaseStored.txId,
				version: coinbaseStored.version,
				lockTime: coinbaseStored.lockTime,
				coinbase: coinbaseStored.vin[0]!,
				outputs: coinbaseStored.vout,
			},
			transactions: otherTxs,
		};
	}

	static async fromStore(storedBlock: StoredBlock, header: WireBlockHeader): Promise<Block> {
		const txs: Tx[] = [];

		// Reconstruct coinbase tx
		const coinbaseTx = new Tx({
			version: storedBlock.coinbaseTx.version,
			locktime: storedBlock.coinbaseTx.lockTime,
			witness: storedBlock.coinbaseTx.coinbase.data.witness.length > 0,
			inputs: [storedBlock.coinbaseTx.coinbase],
			outputs: storedBlock.coinbaseTx.outputs,
		});
		txs.push(coinbaseTx);

		// Add other transactions
		for (const storedTx of storedBlock.transactions) {
			txs.push(await Tx.fromStore(storedTx));
		}

		return new Block(header, txs);
	}
}
