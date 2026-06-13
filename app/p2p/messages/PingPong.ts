import { Codec, Stride } from "@nomadshiba/codec";
import { type PeerMessage } from "~/p2p/Peer.ts";
import { Uint8ArrayView } from "~/utils/Uint8ArrayView.ts";

class PingPongCodec extends Codec<bigint> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 8 };

	encode(nonce: bigint): Uint8Array<ArrayBuffer> {
		const buf = new Uint8Array(8);
		new Uint8ArrayView(buf).setBigUint64(0, nonce, true);
		return buf;
	}

	decode(bytes: Uint8Array): [bigint, number] {
		return [new Uint8ArrayView(bytes).getBigUint64(0, true), 8];
	}
}

const codec = new PingPongCodec();

export const PingMessage: PeerMessage<PingPongCodec> = { command: "ping", codec };
export const PongMessage: PeerMessage<PingPongCodec> = { command: "pong", codec };
