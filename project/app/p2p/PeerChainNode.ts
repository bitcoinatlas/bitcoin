import { WireBlockHeader } from "@project/codecs";

export type PeerChainNode = {
	header: WireBlockHeader;
	cumulativeWork: bigint;
};
