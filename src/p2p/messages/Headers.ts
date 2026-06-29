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

	public encode(data: HeadersPayload): Uint8Array<ArrayBuffer> {
		const count = data.headers.length;
		const out = new Uint8Array(CompactSize.encode(count).length + count * (HEADER_STRIDE + 1));
		this.encodeInto(data, out);
		return out;
	}

	public override encodeInto(data: HeadersPayload, target: Uint8Array, offset: number = 0): number {
		const start = offset;
		offset += CompactSize.encodeInto(data.headers.length, target, offset);
		for (const header of data.headers) {
			offset += WireBlockHeader.encodeInto(header, target, offset);
			target[offset++] = 0x00; // tx count always 0 in headers msg
		}
		return offset - start;
	}

	public decodeFrom(bytes: Uint8Array, offset: number): [HeadersPayload, number] {
		const [count, csLen] = CompactSize.decodeFrom(bytes, offset);
		let off = offset + csLen;
		if (count > 2000) throw new Error("too many headers");
		const headers: WireBlockHeader[] = [];
		for (let i = 0; i < count; i++) {
			const [header, stride] = WireBlockHeader.decodeFrom(bytes, off);
			off += stride;
			off += 1; // skip tx count byte
			headers.push(header);
		}
		return [{ headers }, off - offset];
	}
}

export const HeadersMessage: PeerMessage<HeadersCodec> = {
	command: "headers",
	codec: new HeadersCodec(),
};
