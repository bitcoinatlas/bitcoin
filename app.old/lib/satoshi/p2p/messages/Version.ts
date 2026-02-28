import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { BytesView } from "~/lib/BytesView.ts";
import { PeerMessage } from "~/lib/satoshi/p2p/PeerMessage.ts";

export type Version = {
	version: number;
	services: bigint;
	timestamp: bigint;
	recvServices: bigint;
	transServices: bigint;
	nonce: bigint;
	userAgent: string;
	startHeight: number;
	relay: boolean;
};

export type VersionMessage = Version & {
	recvIP: string;
	recvPort: number;
	transIP: string;
	transPort: number;
};

type VersionMessageCodec = { stride: number };

const VersionMessageCodec = {
	...CodecDefaults<VersionMessageCodec>(),
	create(): VersionMessageCodec {
		return { stride: -1 };
	},
	encode(_self, data: VersionMessage) {
		const userAgentBytes = new TextEncoder().encode(data.userAgent);
		const userAgentLength = userAgentBytes.length;

		const bytes = new Uint8Array(200); // more breathing room
		const view = new BytesView(bytes);

		let offset = 0;

		view.setInt32(offset, data.version, true);
		offset += 4;

		view.setBigUint64(offset, data.services, true);
		offset += 8;

		view.setBigUint64(offset, data.timestamp, true);
		offset += 8;

		view.setBigUint64(offset, data.recvServices, true);
		offset += 8;

		const recvIPBytes = encodeIP(data.recvIP);
		bytes.set(recvIPBytes, offset);
		offset += 16;

		view.setUint16(offset, data.recvPort, false);
		offset += 2;

		view.setBigUint64(offset, data.transServices, true);
		offset += 8;

		const transIPBytes = encodeIP(data.transIP);
		bytes.set(transIPBytes, offset);
		offset += 16;

		view.setUint16(offset, data.transPort, false);
		offset += 2;

		view.setBigUint64(offset, data.nonce, true);
		offset += 8;

		bytes[offset++] = userAgentLength;
		bytes.set(userAgentBytes, offset);
		offset += userAgentLength;

		view.setInt32(offset, data.startHeight, true);
		offset += 4;

		bytes[offset++] = data.relay ? 1 : 0;

		return bytes.subarray(0, offset);
	},
	decode(_self, bytes: Uint8Array) {
		const view = new BytesView(bytes);
		let offset = 0;

		const version = view.getInt32(offset, true);
		offset += 4;

		const services = view.getBigUint64(offset, true);
		offset += 8;

		const timestamp = view.getBigUint64(offset, true);
		offset += 8;

		const recvServices = view.getBigUint64(offset, true);
		offset += 8;

		const recvIPBytes = bytes.subarray(offset, offset + 16);
		const recvIP = decodeIP(recvIPBytes);
		offset += 16;

		const recvPort = view.getUint16(offset, false);
		offset += 2;

		const transServices = view.getBigUint64(offset, true);
		offset += 8;

		const transIPBytes = bytes.subarray(offset, offset + 16);
		const transIP = decodeIP(transIPBytes);
		offset += 16;

		const transPort = view.getUint16(offset, false);
		offset += 2;

		const nonce = view.getBigUint64(offset, true);
		offset += 8;

		const userAgentLength = bytes[offset++]!;
		const userAgent = new TextDecoder().decode(bytes.subarray(offset, offset + userAgentLength));
		offset += userAgentLength;

		const startHeight = view.getInt32(offset, true);
		offset += 4;

		const relay = !!bytes[offset++];

		return [
			{
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
			},
			offset,
		] as [VersionMessage, number];
	},
} satisfies Impl<VersionMessageCodec, Codec<VersionMessageCodec, VersionMessage>>;

const _codec = VersionMessageCodec.create();
export const VersionMessage = PeerMessage.create("version", {
	stride: _codec.stride,
	encode: (v: VersionMessage) => VersionMessageCodec.encode(_codec, v),
	decode: (d: Uint8Array) => VersionMessageCodec.decode(_codec, d),
});

function encodeIP(ip: string): Uint8Array {
	const bytes = new Uint8Array(16);

	// Check IPv4 (a.b.c.d)
	if (/^\d{1,3}(\.\d{1,3}){3}$/.test(ip)) {
		const parts = ip.split(".").map(Number);
		if (parts.some((p) => p < 0 || p > 255)) {
			throw new Error("Invalid IPv4 address");
		}
		// ::ffff:a.b.c.d
		bytes.set([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff], 0);
		bytes.set(parts, 12);
		return bytes;
	}

	// Otherwise treat as IPv6
	// Expand compressed form (::)
	const parts = ip.split("::");
	let head = parts[0] ? parts[0].split(":") : [];
	const tail = parts[1] ? parts[1].split(":") : [];
	if (parts.length === 1) {
		// no compression
	} else if (parts.length === 2) {
		const missing = 8 - (head.length + tail.length);
		if (missing < 0) throw new Error("Invalid IPv6 address");
		head = [...head, ...Array(missing).fill("0"), ...tail];
	} else {
		throw new Error("Invalid IPv6 address");
	}

	if (head.length !== 8) throw new Error("Invalid IPv6 address");

	head.forEach((h, i) => {
		const val = parseInt(h, 16);
		if (isNaN(val) || val < 0 || val > 0xffff) {
			throw new Error("Invalid IPv6 part: " + h);
		}
		bytes[i * 2] = val >> 8;
		bytes[i * 2 + 1] = val & 0xff;
	});

	return bytes;
}

function decodeIP(bytes: Uint8Array): string {
	// IPv4-mapped IPv6 ::ffff:a.b.c.d
	const prefix = bytes.subarray(0, 12);
	const v4 = bytes.subarray(12);
	const isV4Mapped = prefix.every((b, i) => i < 10 ? b === 0 : b === 0xff);
	if (isV4Mapped) {
		return `${v4[0]}.${v4[1]}.${v4[2]}.${v4[3]}`;
	}

	// IPv6
	const parts: string[] = [];
	for (let i = 0; i < 16; i += 2) {
		parts.push(((bytes[i]! << 8) | bytes[i + 1]!).toString(16));
	}
	// Basic zero-compression (not perfect but good enough)
	return parts.join(":").replace(/(^|:)0(:0)+(:|$)/, "::");
}
