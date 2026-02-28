import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { PeerMessage } from "~/lib/satoshi/p2p/PeerMessage.ts";

export type VerackMessage = null;

type VerackMessageCodec = { stride: number };

const VerackMessageCodec = {
	...CodecDefaults<VerackMessageCodec>(),
	create(): VerackMessageCodec {
		return { stride: 0 };
	},
	encode(_self, _: VerackMessage) {
		return new Uint8Array(0);
	},
	decode(_self, _bytes: Uint8Array) {
		return [null, 0] as [VerackMessage, number];
	},
} satisfies Impl<VerackMessageCodec, Codec<VerackMessageCodec, VerackMessage>>;

const _codec = VerackMessageCodec.create();
export const VerackMessage = PeerMessage.create("verack", {
	stride: _codec.stride,
	encode: (v: VerackMessage) => VerackMessageCodec.encode(_codec, v),
	decode: (d: Uint8Array) => VerackMessageCodec.decode(_codec, d),
});
