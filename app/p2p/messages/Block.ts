import { WireBlock } from "~/codec/wire/WireBlock.ts";
import { type PeerMessage } from "~/p2p/Peer.ts";

export const BlockMessage: PeerMessage<typeof WireBlock> = {
	command: "block",
	codec: WireBlock,
};
