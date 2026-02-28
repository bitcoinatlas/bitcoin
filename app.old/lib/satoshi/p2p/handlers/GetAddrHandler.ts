import { AddrMessage } from "~/lib/satoshi/p2p/messages/Addr.ts";
import { GetAddrMessage } from "~/lib/satoshi/p2p/messages/GetAddr.ts";
import { Peer } from "../Peer.ts";
import { PeerMessageHandler } from "../PeerMessageHandler.ts";

export const GetAddrHandler: PeerMessageHandler<GetAddrMessage> = {
	message: GetAddrMessage,
	async handle({ peer, ctx }) {
		peer.log(`📭 Received getaddr request`);

		// Get known peers from PeerManager
		const knownPeers = ctx.peerManager.getKnownPeers();

		// Limit to 1000 addresses per Bitcoin protocol
		const maxAddresses = 1000;
		const addresses = Array.from(knownPeers)
			.slice(0, maxAddresses)
			.map((address) => ({
				timestamp: Math.floor(Date.now() / 1000),
				services: 1n, // NODE_NETWORK
				host: address.host,
				port: address.port,
			}));

		if (addresses.length > 0) {
			await peer.send(AddrMessage, { addresses });
			peer.log(`📬 Sent ${addresses.length} addresses`);
		}
	},
};

export async function getAddr(peer: Peer): Promise<AddrMessage> {
	peer.log("📭 Requesting peer addresses...");
	const addrPromise = peer.expect(AddrMessage);
	await peer.send(GetAddrMessage, {});
	const addrs = await addrPromise;
	peer.log(`📬 Received ${addrs.addresses.length} addresses`);
	return addrs;
}
