import { WireBlock } from "~/lib/codec/wire/WireBlock.ts";
import { type PeerMessage } from "~/lib/peer/Peer.ts";

export const BlockMessage: PeerMessage<typeof WireBlock> = {
	command: "block",
	codec: WireBlock,
};
