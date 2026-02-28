import { BytesView } from "~/lib/BytesView.ts";
import { Stride, StrideFixed } from "~/lib/codec/traits.ts";
import { PeerMessage } from "~/lib/satoshi/p2p/PeerMessage.ts";
import type { Impl } from "~/traits.ts";

export type PingMessageValue = {
	nonce: bigint;
};

export type PingMessage = { stride: StrideFixed };

export const PingMessage = {
	command() {
		return "ping";
	},
	create(): PingMessage {
		return { stride: Stride.fixed(8) };
	},
	stride(self) {
		return self.stride;
	},
	encode(self, data, destination = new Uint8Array(self.stride.size)) {
		const view = new BytesView(destination);
		view.setBigUint64(0, data.nonce, true);
		return destination;
	},
	decode(self, bytes) {
		const view = new BytesView(bytes);
		return [{ nonce: view.getBigUint64(0, true) }, self.stride.size];
	},
} satisfies Impl<PingMessage, PeerMessage<PingMessage, PingMessageValue>>;
