import { WireBlockHeader } from "~/lib/codec/wire/WireBlockHeader.ts";
import { StoredPointer } from "~/lib/codec/stored/StoredPointer.ts";

export type PeerChainNodeParams = {
	header: WireBlockHeader;
	cumulativeWork: bigint;
	pointer: StoredPointer | null;
};

export class PeerChainNode {
	#brand: any;

	header: WireBlockHeader;
	cumulativeWork: bigint;
	pointer: StoredPointer | null;

	constructor(params: PeerChainNodeParams) {
		this.header = params.header;
		this.cumulativeWork = params.cumulativeWork;
		this.pointer = params.pointer;
	}
}
