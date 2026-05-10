import { appendBlockHeader, localChain } from "~/chain.ts";
import { WireBlockHeader } from "~/lib/chain/codec/wire/WireBlockHeader.ts";
import { verifyProofOfWork, workFromHeader } from "~/lib/chain/utils/pow.ts";
import { type Peer } from "~/lib/peer/Peer.ts";
import { GetHeadersMessage } from "~/lib/peer/messages/GetHeaders.ts";
import { HeadersMessage } from "~/lib/peer/messages/Headers.ts";
import { PeerChainNode } from "~/lib/chain/PeerChainNode.ts";
import { peers } from "~/peers.ts";

const PROTOCOL_VERSION = 70015;
const MAX_HEADERS_PER_REQUEST = 2000;
const CHUNK_SIZE = 210_000;

// Tracks which peers are already at chain tip — skip re-requesting them.
const peersAtTip = new WeakSet<Peer>();

/** Call once per tick. Syncs headers from all connected peers not yet at tip. */
export async function syncHeadersFromPeers(): Promise<void> {
	for (const peer of peers()) {
		if (!peer.connected) continue;
		if (peersAtTip.has(peer)) continue;

		try {
			const reachedTip = await syncHeadersFromPeer(peer);
			if (reachedTip) {
				peersAtTip.add(peer);
				console.log(`[sync] peer ${peer.host} at tip, height=${localChain.height()}`);
			}
		} catch (e) {
			console.error(`[sync] ${peer.host} failed:`, e);
		}
	}
}

/**
 * Sync headers from a single peer up to CHUNK_SIZE headers ahead.
 * Returns true if the peer has no more headers (reached its tip).
 */
async function syncHeadersFromPeer(peer: Peer): Promise<boolean> {
	const targetHeight = localChain.height() + CHUNK_SIZE;

	// Build block locator — exponentially spaced hashes back to genesis.
	const locators = buildLocators();

	while (true) {
		if (!peer.connected) throw new Error("peer disconnected during sync");

		const responsePromise = peer.expect(HeadersMessage, 10_000);
		await peer.send(GetHeadersMessage, {
			version: PROTOCOL_VERSION,
			locators,
			stopHash: new Uint8Array(32),
		});

		const { headers } = await responsePromise;

		if (headers.length === 0) return true; // peer at tip

		if (localChain.height() >= targetHeight) return false; // chunk done

		const accepted = applyHeaders(peer, headers);
		if (accepted === 0) return true; // nothing new, treat as tip

		// Next request: single locator = current tip
		locators.length = 0;
		locators.push(localChain.tip()!.header.hash);

		console.log(`[sync] height=${localChain.height()} work=${localChain.tip()!.cumulativeWork}`);

		if (headers.length < MAX_HEADERS_PER_REQUEST) return true; // peer sent partial batch → tip
	}
}

/**
 * Validate and append headers to localChain.
 * Returns number of headers successfully appended.
 */
function applyHeaders(peer: Peer, headers: WireBlockHeader[]): number {
	const tip = localChain.tip();
	if (!tip) return 0;

	let count = 0;
	let prevHash = tip.header.hash;
	let cumulativeWork = tip.cumulativeWork;

	for (const header of headers) {
		// Must chain onto current tip
		if (!bytesEqual(header.prevHash, prevHash)) {
			console.warn(`[sync] ${peer.host}: chain broken at height ${localChain.height() + 1}, stopping`);
			break;
		}

		if (!verifyProofOfWork(header)) {
			console.warn(`[sync] ${peer.host}: invalid PoW at height ${localChain.height() + 1}`);
			break;
		}

		cumulativeWork += workFromHeader(header);
		const node = new PeerChainNode({ header, cumulativeWork, pointer: null });
		localChain.push(node);

		prevHash = header.hash;
		count++;
	}

	if (count > 0) {
		appendBlockHeader(headers.slice(0, count));
	}

	return count;
}

/** Exponentially-spaced locator hashes from tip back to genesis. */
function buildLocators(): Uint8Array[] {
	const locators: Uint8Array[] = [];
	let step = 1;
	let index = localChain.height();

	while (index >= 0) {
		locators.push(localChain.at(index)!.header.hash);
		if (locators.length >= 10) step <<= 1;
		index -= step;
	}

	// Always include genesis
	const genesis = localChain.at(0)!.header.hash;
	if (!bytesEqual(locators.at(-1)!, genesis)) {
		locators.push(genesis);
	}

	return locators;
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) return false;
	}
	return true;
}
