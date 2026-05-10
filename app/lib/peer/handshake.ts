import { type Peer } from "~/lib/peer/Peer.ts";
import { VersionMessage, type VersionPayload } from "~/lib/peer/messages/Version.ts";
import { VerackMessage } from "~/lib/peer/messages/Verack.ts";

const USER_AGENT = "/NomadShiba:0.0.1/";
const PROTOCOL_VERSION = 70015;
const SERVICES = 0n; // NODE_NONE — we are a client only

/**
 * Initiate outbound handshake:
 * 1. Send our version
 * 2. Wait for verack from remote
 * Also installs a one-shot handler to reply verack if the remote sends version first.
 */
export async function handshake(peer: Peer): Promise<void> {
	// Handle inbound version — reply with verack
	const unlisten = peer.onMessage((msg) => {
		if (msg.command !== "version") return;
		unlisten();
		peer.send(VerackMessage, null).catch(() => {});
	});

	const localIp = "0.0.0.0";
	const localPort = 0;

	const versionPayload: VersionPayload = {
		version: PROTOCOL_VERSION,
		services: SERVICES,
		timestamp: BigInt(Math.floor(Date.now() / 1000)),
		recvServices: SERVICES,
		recvIP: peer.host,
		recvPort: peer.port,
		transServices: SERVICES,
		transIP: localIp,
		transPort: localPort,
		nonce: BigInt(Math.floor(Math.random() * Number.MAX_SAFE_INTEGER)),
		userAgent: USER_AGENT,
		startHeight: 0,
		relay: false,
	};

	await peer.send(VersionMessage, versionPayload);
	console.log(`[handshake] sent version → ${peer.host}:${peer.port}`);

	await peer.expect(VerackMessage);
	console.log(`[handshake] complete ✓ ${peer.host}:${peer.port}`);
}
