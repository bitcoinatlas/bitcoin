import { WireBlockHeader } from "~/lib/chain/codec/wire/WireBlockHeader.ts";
import { type PeerMessage } from "~/lib/peer/Peer.ts";

export type HeadersPayload = {
	headers: WireBlockHeader[];
};

function encodeCompactSize(n: number): Uint8Array {
	if (n < 0xfd) return new Uint8Array([n]);
	const out = new Uint8Array(3);
	out[0] = 0xfd;
	new DataView(out.buffer).setUint16(1, n, true);
	return out;
}

function decodeCompactSize(bytes: Uint8Array, off: number): [number, number] {
	const first = bytes[off]!;
	if (first < 0xfd) return [first, 1];
	const view = new DataView(bytes.buffer, bytes.byteOffset);
	if (first === 0xfd) return [view.getUint16(off + 1, true), 3];
	if (first === 0xfe) return [view.getUint32(off + 1, true), 5];
	throw new Error("compact size > 32-bit not supported");
}

const HEADER_STRIDE = WireBlockHeader.inner.stride; // 80 bytes

function encode(data: HeadersPayload): Uint8Array<ArrayBuffer> {
	const count = data.headers.length;
	const countBytes = encodeCompactSize(count);
	const out = new Uint8Array(countBytes.length + count * (HEADER_STRIDE + 1));
	out.set(countBytes, 0);
	let off = countBytes.length;
	for (const header of data.headers) {
		out.set(WireBlockHeader.inner.encode(header), off);
		off += HEADER_STRIDE;
		out[off++] = 0x00; // tx count always 0 in headers msg
	}
	return out;
}

function decode(bytes: Uint8Array): [HeadersPayload, number] {
	let off = 0;
	const [count, csLen] = decodeCompactSize(bytes, off);
	off += csLen;
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

export const HeadersMessage: PeerMessage<HeadersPayload> = {
	command: "headers",
	codec: { encode, decode },
};
