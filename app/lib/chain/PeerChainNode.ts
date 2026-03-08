import { WireBlockHeader } from "./codec/wire/WireBlockHeader.ts";

export type PeerChainNodeParams = {
	header: WireBlockHeader;
	cumulativeWork: bigint;
	pointer: number | null;
};

export class PeerChainNode {
	#brand: any;

	header: WireBlockHeader;
	cumulativeWork: bigint;
	pointer: number | null;

	constructor(params: PeerChainNodeParams) {
		this.header = params.header;
		this.cumulativeWork = params.cumulativeWork;
		this.pointer = params.pointer;
	}
}
