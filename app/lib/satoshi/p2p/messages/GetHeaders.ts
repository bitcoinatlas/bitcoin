import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { BytesView } from "~/lib/BytesView.ts";
import { PeerMessage } from "~/lib/satoshi/p2p/PeerMessage.ts";

export type GetHeadersMessage = {
	version: number;
	locators: Uint8Array[]; // block locator hashes
	stopHash: Uint8Array;
};

type GetHeadersMessageCodec = { stride: number };

const GetHeadersMessageCodec = {
	...CodecDefaults<GetHeadersMessageCodec>(),
	create(): GetHeadersMessageCodec {
		return { stride: -1 };
	},
	encode(_self, data: GetHeadersMessage) {
		const count = data.locators.length;
		const bytes = new Uint8Array(4 + 1 + 32 * count + 32);
		const view = new BytesView(bytes);

		let offset = 0;

		view.setUint32(offset, data.version, true);
		offset += 32 / 8;

		// CompactSize count (assuming < 0xfd)
		if (count >= 0xfd) {
			throw new Error("Too many block locator hashes; CompactSize > 0xfc not supported here.");
		}

		view.setUint8(offset++, count);

		for (const hash of data.locators) {
			if (hash.byteLength !== 32) throw new Error("Invalid hash length in locator");
			bytes.set(hash, offset);
			offset += hash.byteLength;
		}

		if (data.stopHash.byteLength !== 32) {
			throw new Error("Invalid stopHash length");
		}
		bytes.set(data.stopHash, offset);
		offset += data.stopHash.byteLength;

		return bytes.subarray(0, offset);
	},
	decode(_self, bytes: Uint8Array) {
		const view = new BytesView(bytes);

		let offset = 0;

		const version = view.getUint32(offset, true);
		offset += 32 / 8;

		const count = view.getUint8(offset++);
		const hashes: Uint8Array[] = [];

		for (let i = 0; i < count; i++) {
			hashes.push(bytes.subarray(offset, offset + 32));
			offset += 32;
		}

		const stopHash = bytes.subarray(offset, offset + 32);
		offset += 32;

		return [{ version, locators: hashes, stopHash }, offset] as [GetHeadersMessage, number];
	},
} satisfies Impl<GetHeadersMessageCodec, Codec<GetHeadersMessageCodec, GetHeadersMessage>>;

const _codec = GetHeadersMessageCodec.create();
export const GetHeadersMessage = PeerMessage.create("getheaders", {
	stride: _codec.stride,
	encode: (v: GetHeadersMessage) => GetHeadersMessageCodec.encode(_codec, v),
	decode: (d: Uint8Array) => GetHeadersMessageCodec.decode(_codec, d),
});
