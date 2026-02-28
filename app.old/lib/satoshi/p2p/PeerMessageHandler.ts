import { Peer } from "./Peer.ts";
import { PeerManager } from "./PeerManager.ts";

export type PeerMessageHandler<T> = {
	message: Peer.Message<T>;
	handle(event: PeerMessageHandlerEvent<T>): Promise<void> | void;
};

export type PeerMessageHandlerEvent<T> = {
	peer: Peer;
	data: T;
	ctx: {
		peerManager: PeerManager;
	};
};
