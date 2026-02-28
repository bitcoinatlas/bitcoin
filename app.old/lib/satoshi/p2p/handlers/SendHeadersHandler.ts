import { SendHeadersMessage } from "~/lib/satoshi/p2p/messages/SendHeaders.ts";
import { PeerMessageHandler } from "../PeerMessageHandler.ts";

export const SendHeadersHandler: PeerMessageHandler<SendHeadersMessage> = {
	message: SendHeadersMessage,
	handle({ peer }) {
		peer.log(`🪪 Peer prefers headers over inv`);
		// TODO: Handle
	},
};
