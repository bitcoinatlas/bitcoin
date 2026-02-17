import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { BytesView } from "~/lib/BytesView.ts";
import { PeerMessage } from "~/lib/satoshi/p2p/PeerMessage.ts";

export type PongMessage = {
	nonce: bigint;
};

type PongMessageCodec = { stride: number };

const PongMessageCodec = {
	...CodecDefaults<PongMessageCodec>(),
	create(): PongMessageCodec {
		return { stride: 8 };
	},
	encode(_self, data: PongMessage) {
		const bytes = new Uint8Array(8);
		const view = new BytesView(bytes);
		view.setBigUint64(0, data.nonce, true);
		return bytes;
	},
	decode(_self, bytes: Uint8Array) {
		const view = new BytesView(bytes);
		return [{ nonce: view.getBigUint64(0, true) }, 8] as [PongMessage, number];
	},
} satisfies Impl<PongMessageCodec, Codec<PongMessageCodec, PongMessage>>;

const _codec = PongMessageCodec.create();
export const PongMessage = PeerMessage.create("pong", {
	stride: _codec.stride,
	encode: (v: PongMessage) => PongMessageCodec.encode(_codec, v),
	decode: (d: Uint8Array) => PongMessageCodec.decode(_codec, d),
});
