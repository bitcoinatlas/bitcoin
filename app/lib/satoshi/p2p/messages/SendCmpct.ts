import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { BytesView } from "~/lib/BytesView.ts";
import { PeerMessage } from "~/lib/satoshi/p2p/PeerMessage.ts";

export type SendCmpctMessage = {
	announce: boolean;
	version: bigint;
};

type SendCmpctMessageCodec = { stride: number };

const SendCmpctMessageCodec = {
	...CodecDefaults<SendCmpctMessageCodec>(),
	create(): SendCmpctMessageCodec {
		return { stride: 9 };
	},
	encode(_self, data: SendCmpctMessage) {
		const bytes = new Uint8Array(9);
		const view = new BytesView(bytes);
		view.setUint8(0, data.announce ? 1 : 0);
		view.setBigUint64(1, data.version, true);
		return bytes;
	},
	decode(_self, bytes: Uint8Array) {
		const view = new BytesView(bytes);
		return [
			{
				announce: view.getUint8(0) === 1,
				version: view.getBigUint64(1, true),
			},
			9,
		] as [SendCmpctMessage, number];
	},
} satisfies Impl<SendCmpctMessageCodec, Codec<SendCmpctMessageCodec, SendCmpctMessage>>;

const _codec = SendCmpctMessageCodec.create();
export const SendCmpctMessage = PeerMessage.create("sendcmpct", {
	stride: _codec.stride,
	encode: (v: SendCmpctMessage) => SendCmpctMessageCodec.encode(_codec, v),
	decode: (d: Uint8Array) => SendCmpctMessageCodec.decode(_codec, d),
});
