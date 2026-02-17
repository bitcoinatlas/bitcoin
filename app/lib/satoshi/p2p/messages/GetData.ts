import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { BytesView } from "~/lib/BytesView.ts";
import { CompactSize } from "~/lib/CompactSize.ts";
import { PeerMessage } from "~/lib/satoshi/p2p/PeerMessage.ts";

// inventory type codes from protocol
const typeKeyToByte = {
	CMPCT_BLOCK: 0x4,

	SATOSHI_TX: 0x1,
	SATOSHI_BLOCK: 0x2,
	SATOSHI_FILTERED_BLOCK: 0x3,

	WITNESS_TX: 0x40000001,
	WITNESS_BLOCK: 0x40000002,
	WITNESS_FILTERED_BLOCK: 0x40000003,
};

const typeByteToKey = new Map(
	Object.entries(typeKeyToByte).map(
		([key, value]) => [value, key as keyof typeof typeKeyToByte] as const,
	),
);

export type GetDataMessage = {
	inventory: {
		type: keyof typeof typeKeyToByte;
		hash: Uint8Array; // 32-byte block/tx hash (LE)
	}[];
};

type GetDataMessageCodec = { stride: number };

const GetDataMessageCodec = {
	...CodecDefaults<GetDataMessageCodec>(),
	create(): GetDataMessageCodec {
		return { stride: -1 };
	},
	encode(_self, data: GetDataMessage) {
		const count = data.inventory.length;
		const countBytes = CompactSize.encode(count);

		const bytes = new Uint8Array(countBytes.length + count * 36);
		bytes.set(countBytes, 0);

		let offset = countBytes.length;
		for (const item of data.inventory) {
			const view = new BytesView(bytes, offset, 36);
			view.setUint32(0, typeKeyToByte[item.type], true);
			bytes.set(item.hash, offset + 4);
			offset += 36;
		}

		return bytes;
	},
	decode(_self, bytes: Uint8Array) {
		const [count, countSize] = CompactSize.decode(bytes, 0);
		let offset = countSize;

		const inventory: GetDataMessage["inventory"] = [];
		for (let i = 0; i < count; i++) {
			const entry = new BytesView(bytes, offset, 36);
			const typeCode = entry.getUint32(0, true);
			const type = typeByteToKey.get(typeCode);
			if (!type) throw new Error(`Unknown inventory type: ${typeCode}`);

			const hash = bytes.subarray(offset + 4, offset + 36);
			inventory.push({ type, hash });

			offset += 36;
		}
		return [{ inventory }, offset] as [GetDataMessage, number];
	},
} satisfies Impl<GetDataMessageCodec, Codec<GetDataMessageCodec, GetDataMessage>>;

const _codec = GetDataMessageCodec.create();
export const GetDataMessage = PeerMessage.create("getdata", {
	stride: _codec.stride,
	encode: (v: GetDataMessage) => GetDataMessageCodec.encode(_codec, v),
	decode: (d: Uint8Array) => GetDataMessageCodec.decode(_codec, d),
});
