import { SendCmpctMessage } from "~/lib/satoshi/p2p/messages/SendCmpct.ts";
import { PeerMessageHandler } from "../PeerMessageHandler.ts";

export const SendCmpctHandler: PeerMessageHandler<SendCmpctMessage> = {
	message: SendCmpctMessage,
	handle({ peer, data }) {
		peer.log(`📦 Received sendcmpct → announce=${data.announce}, version=${data.version}`);
	},
};
