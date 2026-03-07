import { Tx } from "~/lib/chain/Tx.ts";

export type BlockHeader = {
	version: number;
	prevHash: Uint8Array;
	merkleRoot: Uint8Array;
	timestamp: number;
	bits: number;
	nonce: number;
};

export class Block {
	public header: BlockHeader;
	#txs?: Tx[];

	constructor(header: BlockHeader, txs?: Tx[]) {
		this.header = header;
		this.#txs = txs;
	}

	public async getTxs(): Promise<Tx[]> {
		if (this.#txs) return this.#txs;
		throw new Error("Not Implemented");
	}
}
