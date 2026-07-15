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

	public encoder(data: HeadersPayload, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(data: HeadersPayload, target: Uint8Array, offset: number): number;
	public encoder(data: HeadersPayload, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
		if (target === undefined) {
			const count = data.headers.length;
			const out = new Uint8Array(CompactSize.encode(count).length + count * (HEADER_STRIDE + 1));
			this.encoder(data, out, 0);
			return out;
		}

		offset = offset!;
		const start = offset;
		offset += CompactSize.encodeInto(data.headers.length, target, offset);
		for (const header of data.headers) {
			offset += WireBlockHeader.encodeInto(header, target, offset);
			target[offset++] = 0x00; // tx count always 0 in headers msg
		}
		return offset - start;
	}

	public decoder(bytes: Uint8Array, offset: number): [HeadersPayload, number] {
		const [count, csLen] = CompactSize.decode(bytes, offset);
		let off = offset + csLen;
		if (count > 2000) throw new Error("too many headers");
		const headers: WireBlockHeader[] = [];
		for (let i = 0; i < count; i++) {
			const [header, stride] = WireBlockHeader.decode(bytes, off);
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
