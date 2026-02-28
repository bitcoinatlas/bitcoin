import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { BytesView } from "~/lib/BytesView.ts";
import { CompactSize } from "~/lib/CompactSize.ts";
import { PeerMessage } from "~/lib/satoshi/p2p/PeerMessage.ts";

export type AddrItem = {
	timestamp: number; // Unix timestamp
	services: bigint;
	host: string;
	port: number;
};

export type AddrMessage = {
	addresses: AddrItem[];
};

type AddrMessageCodec = { stride: number };

const AddrMessageCodec = {
	create(): AddrMessageCodec {
		return { stride: -1 };
	},
	encode(_self, data: AddrMessage) {
		const count = data.addresses.length;
		const countBytes = CompactSize.encode(count);
		const bytes = new Uint8Array(countBytes.length + count * 30);

		let offset = 0;
		bytes.set(countBytes, offset);
		offset += countBytes.length;

		for (const addr of data.addresses) {
			const view = new BytesView(bytes);

			// timestamp (4 bytes)
			view.setUint32(offset, addr.timestamp, true);
			offset += 4;

			// services (8 bytes)
			view.setBigUint64(offset, addr.services, true);
			offset += 8;

			// ip (16 bytes)
			const ipBytes = encodeIP(addr.host);
			bytes.set(ipBytes, offset);
			offset += 16;

			// port (2 bytes, big-endian)
			view.setUint16(offset, addr.port, false);
			offset += 2;
		}

		return bytes.subarray(0, offset);
	},
	decode(_self, bytes: Uint8Array) {
		const [count, countSize] = CompactSize.decode(bytes, 0);
		let offset = countSize;

		const addresses: AddrItem[] = [];
		const view = new BytesView(bytes);

		for (let i = 0; i < count; i++) {
			const timestamp = view.getUint32(offset, true) * 1000;
			offset += 4;

			const services = view.getBigUint64(offset, true);
			offset += 8;

			const ipBytes = bytes.subarray(offset, offset + 16);
			const ip = decodeIP(ipBytes);
			offset += 16;

			const port = view.getUint16(offset, false);
			offset += 2;

			addresses.push({ timestamp, services, host: ip, port });
		}

		return [{ addresses }, offset] as [AddrMessage, number];
	},
} satisfies Impl<AddrMessageCodec, Codec<AddrMessageCodec, AddrMessage>>;

const _codec = AddrMessageCodec.create();
export const AddrMessage = PeerMessage.create("addr", {
	stride: _codec.stride,
	encode: (v: AddrMessage) => AddrMessageCodec.encode(_codec, v),
	decode: (d: Uint8Array) => AddrMessageCodec.decode(_codec, d),
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

	// IPv6 - strip brackets if present
	let ipv6 = ip;
	if (ipv6.startsWith("[") && ipv6.endsWith("]")) {
		ipv6 = ipv6.slice(1, -1);
	}

	// Parse IPv6
	const parts = ipv6.split("::");
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

	// Find longest sequence of zeros for compression
	let longestStart = -1;
	let longestLen = 0;
	let currentStart = -1;
	let currentLen = 0;

	for (let i = 0; i < parts.length; i++) {
		if (parts[i] === "0") {
			if (currentStart === -1) {
				currentStart = i;
				currentLen = 1;
			} else {
				currentLen++;
			}
			// Update longest if current is longer
			if (currentLen > longestLen) {
				longestStart = currentStart;
				longestLen = currentLen;
			}
		} else {
			currentStart = -1;
			currentLen = 0;
		}
	}

	// Build IPv6 address
	let ipv6: string;
	if (longestLen >= 2) {
		const before = parts.slice(0, longestStart).join(":");
		const after = parts.slice(longestStart + longestLen).join(":");
		if (before && after) {
			ipv6 = `${before}::${after}`;
		} else if (before) {
			ipv6 = `${before}::`;
		} else if (after) {
			ipv6 = `::${after}`;
		} else {
			ipv6 = "::";
		}
	} else {
		ipv6 = parts.join(":");
	}

	// Wrap IPv6 in brackets
	return `[${ipv6}]`;
}
