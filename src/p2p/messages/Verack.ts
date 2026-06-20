import { Codec, Stride } from "@nomadshiba/codec";
import { type PeerMessage } from "~/p2p/Peer.ts";

class VerackCodec extends Codec<null> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 0 };

	public encode(_data: null): Uint8Array<ArrayBuffer> {
		return new Uint8Array(0);
	}

	public override encodeInto(_data: null, _target: Uint8Array, _offset: number = 0): number {
		return 0;
	}

	public decode(_bytes: Uint8Array): [null, number] {
		return [null, 0];
	}
}

export const VerackMessage: PeerMessage<VerackCodec> = {
	command: "verack",
	codec: new VerackCodec(),
};
