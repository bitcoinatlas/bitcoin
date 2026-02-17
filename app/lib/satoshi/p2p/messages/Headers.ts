import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { BlockHeader } from "~/lib/satoshi/primitives/BlockHeader.ts";
import { PeerMessage } from "~/lib/satoshi/p2p/PeerMessage.ts";
import { CompactSize } from "~/lib/CompactSize.ts";

export type HeadersMessage = {
	headers: BlockHeader[];
};

type HeadersMessageCodec = { stride: number };

const HeadersMessageCodec = {
	...CodecDefaults<HeadersMessageCodec>(),
	create(): HeadersMessageCodec {
		return { stride: -1 };
	},
	encode(_self, data: HeadersMessage) {
		const count = data.headers.length;
		if (count > 2000) {
			throw new Error("Too many headers (max 2000)");
		}

		const chunks: Uint8Array[] = [];
		chunks.push(CompactSize.encode(count));

		for (const header of data.headers) {
			const headerBytes = BlockHeader.encode(header);
			if (headerBytes.byteLength !== 80) {
				throw new Error("Invalid header size");
			}
			chunks.push(headerBytes);

			// tx count — always 0x00 in headers message
			chunks.push(new Uint8Array([0x00]));
		}

		// flatten chunks into a single buffer
		const totalLength = chunks.reduce((sum, c) => sum + c.length, 0);
		const out = new Uint8Array(totalLength);
		let offset = 0;
		for (const c of chunks) {
			out.set(c, offset);
			offset += c.length;
		}
		return out;
	},
	decode(_self, bytes: Uint8Array) {
		let offset = 0;

		const [count, bytesRead] = CompactSize.decode(bytes, offset);
		offset += bytesRead;
		if (count > 2000) {
			throw new Error("Too many headers (max 2000)");
		}

		const headers: BlockHeader[] = [];
		for (let i = 0; i < count; i++) {
			if (offset + BlockHeader.stride > bytes.length) {
				throw new Error("Incomplete header data");
			}
			const [header, headerBytes] = BlockHeader.decode(bytes.subarray(offset));
			offset += headerBytes;

			const txCount = bytes[offset++];
			if (txCount !== 0x00) {
				throw new Error("Invalid tx count in headers message");
			}

			headers.push(header);
		}

		return [{ headers }, offset] as [HeadersMessage, number];
	},
} satisfies Impl<HeadersMessageCodec, Codec<HeadersMessageCodec, HeadersMessage>>;

const _codec = HeadersMessageCodec.create();
export const HeadersMessage = PeerMessage.create("headers", {
	stride: _codec.stride,
	encode: (v: HeadersMessage) => HeadersMessageCodec.encode(_codec, v),
	decode: (d: Uint8Array) => HeadersMessageCodec.decode(_codec, d),
});
