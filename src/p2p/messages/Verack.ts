import { Codec, Stride } from "@nomadshiba/codec";
import { type PeerMessage } from "~/p2p/Peer.ts";

class VerackCodec extends Codec<null> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 0 };

	public encoder(_data: null, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(_data: null, target: Uint8Array, offset: number): number;
	public encoder(_data: null, target?: Uint8Array, _offset?: number): Uint8Array<ArrayBuffer> | number {
		if (target === undefined) return new Uint8Array(0);
		return 0;
	}

	public decoder(_bytes: Uint8Array, _offset: number): [null, number] {
		return [null, 0];
	}
}

export const VerackMessage: PeerMessage<VerackCodec> = {
	command: "verack",
	codec: new VerackCodec(),
};
