import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { BytesView } from "~/lib/BytesView.ts";
import { PeerMessage } from "~/lib/satoshi/p2p/PeerMessage.ts";

export type PingMessage = {
	nonce: bigint;
};

type PingMessageCodec = { stride: number };

const PingMessageCodec = {
	...CodecDefaults<PingMessageCodec>(),
	create(): PingMessageCodec {
		return { stride: 8 };
	},
	encode(_self, data: PingMessage) {
		const bytes = new Uint8Array(8);
		const view = new BytesView(bytes);
		view.setBigUint64(0, data.nonce, true);
		return bytes;
	},
	decode(_self, bytes: Uint8Array) {
		const view = new BytesView(bytes);
		return [{ nonce: view.getBigUint64(0, true) }, 8] as [PingMessage, number];
	},
} satisfies Impl<PingMessageCodec, Codec<PingMessageCodec, PingMessage>>;

const _codec = PingMessageCodec.create();
export const PingMessage = PeerMessage.create("ping", {
	stride: _codec.stride,
	encode: (v: PingMessage) => PingMessageCodec.encode(_codec, v),
	decode: (d: Uint8Array) => PingMessageCodec.decode(_codec, d),
});
