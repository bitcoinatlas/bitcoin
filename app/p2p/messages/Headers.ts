import { Codec, Stride } from "@nomadshiba/codec";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { CompactSize } from "~/codec/primitives/CompactSize.ts";
import { type PeerMessage } from "~/p2p/Peer.ts";

export type HeadersPayload = {
	headers: WireBlockHeader[];
};

const HEADER_STRIDE = WireBlockHeader.stride.size;

class HeadersCodec extends Codec<HeadersPayload> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	encode(data: HeadersPayload): Uint8Array<ArrayBuffer> {
		const count = data.headers.length;
		const countBytes = CompactSize.encode(count);
		const out = new Uint8Array(countBytes.length + count * (HEADER_STRIDE + 1));
		out.set(countBytes, 0);
		let off = countBytes.length;
		for (const header of data.headers) {
			out.set(WireBlockHeader.encode(header), off);
			off += HEADER_STRIDE;
			out[off++] = 0x00; // tx count always 0 in headers msg
		}
		return out;
	}

	decode(bytes: Uint8Array): [HeadersPayload, number] {
		const [count, csLen] = CompactSize.decode(bytes);
		let off = csLen;
		if (count > 2000) throw new Error("too many headers");
		const headers: WireBlockHeader[] = [];
		for (let i = 0; i < count; i++) {
			const [header, stride] = WireBlockHeader.decode(bytes.subarray(off));
			off += stride;
			off += 1; // skip tx count byte
			headers.push(header);
		}
		return [{ headers }, off];
	}
}

export const HeadersMessage: PeerMessage<HeadersCodec> = {
	command: "headers",
	codec: new HeadersCodec(),
};
