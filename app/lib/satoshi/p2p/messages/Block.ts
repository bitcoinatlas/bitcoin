import { Block } from "~/lib/satoshi/primitives/Block.ts";
import { PeerMessage } from "~/lib/satoshi/p2p/PeerMessage.ts";

export type BlockMessage = {
	block: Block;
};

export const BlockMessage = PeerMessage.create("block", Block);
