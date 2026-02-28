import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { PeerMessage } from "~/lib/satoshi/p2p/PeerMessage.ts";

export type SendHeadersMessage = null;

type SendHeadersMessageCodec = { stride: number };

const SendHeadersMessageCodec = {
	...CodecDefaults<SendHeadersMessageCodec>(),
	create(): SendHeadersMessageCodec {
		return { stride: 0 };
	},
	encode(_self, _: SendHeadersMessage) {
		return new Uint8Array(0);
	},
	decode(_self, _bytes: Uint8Array) {
		return [null, 0] as [SendHeadersMessage, number];
	},
} satisfies Impl<SendHeadersMessageCodec, Codec<SendHeadersMessageCodec, SendHeadersMessage>>;

const _codec = SendHeadersMessageCodec.create();
export const SendHeadersMessage = PeerMessage.create("sendheaders", {
	stride: _codec.stride,
	encode: (v: SendHeadersMessage) => SendHeadersMessageCodec.encode(_codec, v),
	decode: (d: Uint8Array) => SendHeadersMessageCodec.decode(_codec, d),
});
