import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { StoredPointer } from "~/codec/stored/StoredPointer.ts";

export type PeerChainNode = {
	header: WireBlockHeader;
	cumulativeWork: bigint;
	pointer: StoredPointer | null;
};
