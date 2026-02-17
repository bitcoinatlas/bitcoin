import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { PeerMessage } from "~/lib/satoshi/p2p/PeerMessage.ts";

export type GetAddrMessage = Record<string, never>; // Empty message

type GetAddrMessageCodec = { stride: number };

const GetAddrMessageCodec = {
	...CodecDefaults<GetAddrMessageCodec>(),
	create(): GetAddrMessageCodec {
		return { stride: 0 };
	},
	encode(_self, _data: GetAddrMessage) {
		return new Uint8Array(0);
	},
	decode(_self, _bytes: Uint8Array) {
		return [{}, 0] as [GetAddrMessage, number];
	},
} satisfies Impl<GetAddrMessageCodec, Codec<GetAddrMessageCodec, GetAddrMessage>>;

const _codec = GetAddrMessageCodec.create();
export const GetAddrMessage = PeerMessage.create("getaddr", {
	stride: _codec.stride,
	encode: (v: GetAddrMessage) => GetAddrMessageCodec.encode(_codec, v),
	decode: (d: Uint8Array) => GetAddrMessageCodec.decode(_codec, d),
});
