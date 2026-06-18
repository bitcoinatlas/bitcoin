import { equals } from "@std/bytes";
import { PeerChain } from "~/chain/PeerChain.ts";
import { PeerChainNode } from "~/chain/PeerChainNode.ts";
import { verifyProofOfWork, workFromHeader } from "~/chain/utils/pow.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { GetHeadersMessage } from "~/p2p/messages/GetHeaders.ts";
import { HeadersMessage } from "~/p2p/messages/Headers.ts";
import { Peer } from "~/p2p/Peer.ts";

const PROTOCOL_VERSION = 70015;
const CHUNK_SIZE = 200_000;

export async function syncHeader(peer: Peer, chain: PeerChain) {
	const targetHeight = chain.height() + CHUNK_SIZE;
	const locators = buildLocators(chain);

	while (true) {
		const headers = await getHeaders(peer, locators);
		if (headers.length === 0) return true; // peer at tip
		if (chain.height() >= targetHeight) return false; // chunk done

		const verifiedCount = verifyAndPushHeaders(chain, headers);
		if (verifiedCount === 0) return true; // nothing new, treat as tip

		// Next request: single locator = current tip
		locators.length = 0;
		locators.push(chain.tip()!.header.hash());

		console.log(`[sync] height=${chain.height()} work=${chain.tip()!.cumulativeWork}`);
	}
}

function buildLocators(chain: PeerChain): Uint8Array[] {
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

async function getHeaders(peer: Peer, locators: Uint8Array[]): Promise<WireBlockHeader[]> {
	const responsePromise = peer.expect(HeadersMessage, 10_000);
	await peer.send(GetHeadersMessage, {
		version: PROTOCOL_VERSION,
		locators,
		stopHash: new Uint8Array(32),
	});
	const { headers } = await responsePromise;
	return headers;
}

export function verifyAndPushHeaders(chain: PeerChain, headers: WireBlockHeader[]): number {
	const tip = chain.tip();
	if (!tip) return 0;

	let count = 0;
	let prevHash = tip.header.hash();
	let cumulativeWork = tip.cumulativeWork;

	for (const header of headers) {
		if (!equals(header.prevHash, prevHash)) break; // chain onto current tip
		if (!verifyProofOfWork(header)) break;

		cumulativeWork += workFromHeader(header);
		const node: PeerChainNode = { header, cumulativeWork };
		chain.push(node);

		prevHash = header.hash();
		count++;
	}

	return count;
}
