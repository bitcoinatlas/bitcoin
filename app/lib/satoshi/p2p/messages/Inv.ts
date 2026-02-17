import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { BytesView } from "~/lib/BytesView.ts";
import { PeerMessage } from "~/lib/satoshi/p2p/PeerMessage.ts";

const typeKeyToByte = {
	TX: 1,
	BLOCK: 2,
};

const typeByteToKey = new Map(
	Object.entries(typeKeyToByte).map(([key, value]) => [value, key as keyof typeof typeKeyToByte] as const),
);

export type InvVector = {
	type: keyof typeof typeKeyToByte; // 1 = tx, 2 = block, etc.
	hash: Uint8Array; // 32 bytes
};

export type InvMessage = {
	inventory: InvVector[];
};

type InvMessageCodec = { stride: number };

const InvMessageCodec = {
	...CodecDefaults<InvMessageCodec>(),
	create(): InvMessageCodec {
		return { stride: -1 };
	},
	encode(_self, data: InvMessage) {
		const count = data.inventory.length;
		if (count >= 0xfd) throw new Error("Too many inventory items");

		const bytes = new Uint8Array(1 + count * 36); // 1 varint + 36 per entry
		let offset = 0;

		bytes[offset++] = count;

		for (const item of data.inventory) {
			const view = new BytesView(bytes, offset, 36);
			view.setUint32(0, typeKeyToByte[item.type], true); // little-endian
			bytes.set(item.hash, offset + 4);
			offset += 36;
		}

		return bytes;
	},
	decode(_self, bytes: Uint8Array) {
		let offset = 0;
		const count = bytes[offset++]!;
		const inventory = [];

		for (let i = 0; i < count; i++) {
			const view = new BytesView(bytes, offset, 36);
			const type = typeByteToKey.get(view.getUint32(0, true));
			if (!type) {
				throw new Error(`Unknown inventory type byte: ${view.getUint32(0, true)}`);
			}
			const hash = bytes.subarray(offset + 4, offset + 36);
			inventory.push({ type, hash });
			offset += 36;
		}

		return [{ inventory }, offset] as [InvMessage, number];
	},
} satisfies Impl<InvMessageCodec, Codec<InvMessageCodec, InvMessage>>;

const _codec = InvMessageCodec.create();
export const InvMessage = PeerMessage.create("inv", {
	stride: _codec.stride,
	encode: (v: InvMessage) => InvMessageCodec.encode(_codec, v),
	decode: (d: Uint8Array) => InvMessageCodec.decode(_codec, d),
});
