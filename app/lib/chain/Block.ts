import { sha256 } from "@noble/hashes/sha2";
import { WireBlock } from "./codec/wire/WireBlock.ts";
import { WireTx } from "./codec/wire/WireTx.ts";

export class Block {
	public readonly header: WireBlock["header"];
	public readonly headerHash: Uint8Array;
	public async txs(): Promise<WireTx[]> {
		return [];
	}

	constructor(header: WireBlock["header"]) {
		this.header = header;
		// There are not many headers, so probably this wont be too slow, but in case it becomes an issue, do the same thing you did with the transactions.
		this.headerHash = sha256(sha256(WireBlock.shape.header.encode(header)));
	}
}
