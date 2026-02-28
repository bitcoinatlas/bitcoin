import { humanize } from "~/lib/logging/human.ts";
import { InvMessage } from "~/lib/satoshi/p2p/messages/Inv.ts";
import { PeerMessageHandler } from "../PeerMessageHandler.ts";

export const InvHandler: PeerMessageHandler<InvMessage> = {
	message: InvMessage,
	handle({ peer, data }) {
		for (const item of data.inventory) {
			peer.log(`📩 Inv: ${item.type} ${humanize(item.hash)}`);
		}
	},
};
