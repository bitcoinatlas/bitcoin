import { equals } from "@std/bytes";
import { verifyProofOfWork, workFromHeader } from "~/chain/utils/pow.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { GetHeadersMessage } from "~/p2p/messages/GetHeaders.ts";
import { HeadersMessage } from "~/p2p/messages/Headers.ts";
import { Peer } from "~/p2p/Peer.ts";
import { PeerChain } from "~/p2p/PeerChain.ts";
import { PeerChainNode } from "~/p2p/PeerChainNode.ts";

const PROTOCOL_VERSION = 70015;

export function buildLocators(chain: PeerChain): Uint8Array[] {
	const locators: Uint8Array[] = [];
	let step = 1;
	let index = chain.height();

	while (index >= 0) {
		locators.push(chain.at(index)!.header.hash());
		if (locators.length >= 10) step <<= 1;
		index -= step;
	}

	// Always include genesis
	const genesis = chain.at(0)!.header.hash();
	if (!equals(locators.at(-1)!, genesis)) {
		locators.push(genesis);
	}

	return locators;
}

export async function getHeaders(peer: Peer, locators: Uint8Array[]): Promise<WireBlockHeader[]> {
	const responsePromise = peer.expect(HeadersMessage, 10_000);
	await peer.send(GetHeadersMessage, {
		version: PROTOCOL_VERSION,
		locators,
		stopHash: new Uint8Array(32),
	});
	const { headers } = await responsePromise;
	return headers;
}
