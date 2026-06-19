import { Codec, Stride } from "@nomadshiba/codec";
import { type PeerMessage } from "~/p2p/Peer.ts";
import { Uint8ArrayView } from "~/libs/collections/Uint8ArrayView.ts";

export type VersionPayload = {
	version: number;
	services: bigint;
	timestamp: bigint;
	recvServices: bigint;
	recvIP: string;
	recvPort: number;
	transServices: bigint;
	transIP: string;
	transPort: number;
	nonce: bigint;
	userAgent: string;
	startHeight: number;
	relay: boolean;
};

function encodeIP(ip: string): Uint8Array {
	const out = new Uint8Array(16);
	if (/^\d{1,3}(\.\d{1,3}){3}$/.test(ip)) {
		const parts = ip.split(".").map(Number);
		out.set([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff], 0);
		out.set(parts, 12);
		return out;
	}
	const halves = ip.split("::");
	let head = halves[0] ? halves[0].split(":") : [];
	const tail = halves[1] ? halves[1].split(":") : [];
	if (halves.length === 2) {
		const fill = 8 - head.length - tail.length;
		head = [...head, ...Array(fill).fill("0"), ...tail];
	}
	for (let i = 0; i < 8; i++) {
		const v = parseInt(head[i] ?? "0", 16);
		out[i * 2] = v >> 8;
		out[i * 2 + 1] = v & 0xff;
	}
	return out;
}

function decodeIP(bytes: Uint8Array): string {
	const isV4 = bytes.slice(0, 10).every((b) => b === 0) && bytes[10] === 0xff && bytes[11] === 0xff;
	if (isV4) return `${bytes[12]}.${bytes[13]}.${bytes[14]}.${bytes[15]}`;
	const parts: string[] = [];
	for (let i = 0; i < 16; i += 2) parts.push(((bytes[i]! << 8) | bytes[i + 1]!).toString(16));
	return parts.join(":").replace(/(^|:)0(:0)+(:|$)/, "::");
}

class VersionCodec extends Codec<VersionPayload> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	encode(data: VersionPayload): Uint8Array<ArrayBuffer> {
		const ua = new TextEncoder().encode(data.userAgent);
		const out = new Uint8Array(4 + 8 + 8 + 8 + 16 + 2 + 8 + 16 + 2 + 8 + 1 + ua.length + 4 + 1);
		const view = new Uint8ArrayView(out);
		let off = 0;

		view.setInt32(off, data.version, true);
		off += 4;
		view.setBigUint64(off, data.services, true);
		off += 8;
		view.setBigUint64(off, data.timestamp, true);
		off += 8;
		view.setBigUint64(off, data.recvServices, true);
		off += 8;
		out.set(encodeIP(data.recvIP), off);
		off += 16;
		view.setUint16(off, data.recvPort, false);
		off += 2;
		view.setBigUint64(off, data.transServices, true);
		off += 8;
		out.set(encodeIP(data.transIP), off);
		off += 16;
		view.setUint16(off, data.transPort, false);
		off += 2;
		view.setBigUint64(off, data.nonce, true);
		off += 8;
		out[off++] = ua.length;
		out.set(ua, off);
		off += ua.length;
		view.setInt32(off, data.startHeight, true);
		off += 4;
		out[off++] = data.relay ? 1 : 0;

		return out;
	}

	decode(bytes: Uint8Array): [VersionPayload, number] {
		const view = new Uint8ArrayView(bytes);
		let off = 0;

		const version = view.getInt32(off, true);
		off += 4;
		const services = view.getBigUint64(off, true);
		off += 8;
		const timestamp = view.getBigUint64(off, true);
		off += 8;
		const recvServices = view.getBigUint64(off, true);
		off += 8;
		const recvIP = decodeIP(bytes.subarray(off, off + 16));
		off += 16;
		const recvPort = view.getUint16(off, false);
		off += 2;
		const transServices = view.getBigUint64(off, true);
		off += 8;
		const transIP = decodeIP(bytes.subarray(off, off + 16));
		off += 16;
		const transPort = view.getUint16(off, false);
		off += 2;
		const nonce = view.getBigUint64(off, true);
		off += 8;
		const uaLen = bytes[off++]!;
		const userAgent = new TextDecoder().decode(bytes.subarray(off, off + uaLen));
		off += uaLen;
		const startHeight = view.getInt32(off, true);
		off += 4;
		const relay = !!bytes[off++];

		return [{
			version,
			services,
			timestamp,
			recvServices,
			recvIP,
			recvPort,
			transServices,
			transIP,
			transPort,
			nonce,
			userAgent,
			startHeight,
			relay,
		}, off];
	}
}

export const VersionMessage: PeerMessage<VersionCodec> = {
	command: "version",
	codec: new VersionCodec(),
};
