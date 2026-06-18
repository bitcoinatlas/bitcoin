import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";

export type PeerChainNode = {
	header: WireBlockHeader;
	cumulativeWork: bigint;
};
