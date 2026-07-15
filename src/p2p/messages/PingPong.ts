import { Codec, Stride } from "@nomadshiba/codec";
import { type PeerMessage } from "~/p2p/Peer.ts";
import { Uint8ArrayView } from "~/libs/collections/Uint8ArrayView.ts";

class PingPongCodec extends Codec<bigint> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 8 };

	public encoder(nonce: bigint, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(nonce: bigint, target: Uint8Array, offset: number): number;
	public encoder(nonce: bigint, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
		if (target === undefined) {
			const buf = new Uint8Array(8);
			new Uint8ArrayView(buf).setBigUint64(0, nonce, true);
			return buf;
		}
		new DataView(target.buffer, target.byteOffset + offset!).setBigUint64(0, nonce, true);
		return 8;
	}

	public decoder(bytes: Uint8Array, offset: number): [bigint, number] {
		return [new Uint8ArrayView(bytes, offset).getBigUint64(0, true), 8];
	}
}

const codec = new PingPongCodec();

export const PingMessage: PeerMessage<PingPongCodec> = { command: "ping", codec };
export const PongMessage: PeerMessage<PingPongCodec> = { command: "pong", codec };
