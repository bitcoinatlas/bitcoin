import { VerackMessage } from "~/lib/satoshi/p2p/messages/Verack.ts";
import { VersionMessage } from "~/lib/satoshi/p2p/messages/Version.ts";
import { Peer } from "~/lib/satoshi/p2p/Peer.ts";
import { PeerMessageHandler } from "../PeerMessageHandler.ts";

export const VersionHandler: PeerMessageHandler<VersionMessage> = {
	message: VersionMessage,
	async handle({ peer, data }) {
		peer.log(`🤝 Received version: v${data.version}, ua=${data.userAgent}`);
		await peer.send(VerackMessage, {});
	},
};

export async function handshake(peer: Peer, version: VersionMessage): Promise<void> {
	await peer.send(VersionMessage, version);
	peer.log(`📗 Sent version`);
	await peer.expect(VerackMessage, () => true);
	peer.log(`✅ Handshake complete`);
}
