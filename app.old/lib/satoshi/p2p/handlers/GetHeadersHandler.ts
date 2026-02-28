import { GetHeadersMessage } from "~/lib/satoshi/p2p/messages/GetHeaders.ts";
import { PeerMessageHandler } from "../PeerMessageHandler.ts";

export const GetHeadersHandler: PeerMessageHandler<GetHeadersMessage> = {
	message: GetHeadersMessage,
	handle({ peer, data }) {
		peer.log(`📚 Received getheaders (locator count: ${data.locators.length})`);
	},
};
