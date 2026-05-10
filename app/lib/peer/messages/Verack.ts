import { Codec } from "@nomadshiba/codec";
import { type PeerMessage } from "~/lib/peer/Peer.ts";

class VerackCodec extends Codec<null> {
	readonly stride = 0;

	encode(_data: null): Uint8Array<ArrayBuffer> {
		return new Uint8Array(0) as Uint8Array<ArrayBuffer>;
	}

	decode(_bytes: Uint8Array): [null, number] {
		return [null, 0];
	}
}

export const VerackMessage: PeerMessage<VerackCodec> = {
	command: "verack",
	codec: new VerackCodec(),
};
