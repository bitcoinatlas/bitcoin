import { Codec } from "@nomadshiba/codec";
import { type PeerMessage } from "~/lib/peer/Peer.ts";

export type GetHeadersPayload = {
	version: number;
	locators: Uint8Array[];
	stopHash: Uint8Array;
};

class GetHeadersCodec extends Codec<GetHeadersPayload> {
	readonly stride = -1;

	encode(data: GetHeadersPayload): Uint8Array<ArrayBuffer> {
		const count = data.locators.length;
		if (count >= 0xfd) throw new Error("too many locators");
		const out = new Uint8Array(4 + 1 + 32 * count + 32);
		const view = new DataView(out.buffer);
		view.setUint32(0, data.version, true);
		out[4] = count;
		let off = 5;
		for (const hash of data.locators) {
			out.set(hash, off);
			off += 32;
		}
		out.set(data.stopHash, off);
		return out;
	}

	decode(bytes: Uint8Array): [GetHeadersPayload, number] {
		const view = new DataView(bytes.buffer, bytes.byteOffset);
		const version = view.getUint32(0, true);
		const count = bytes[4]!;
		let off = 5;
		const locators: Uint8Array[] = [];
		for (let i = 0; i < count; i++) {
			locators.push(bytes.slice(off, off + 32));
			off += 32;
		}
		const stopHash = bytes.slice(off, off + 32);
		off += 32;
		return [{ version, locators, stopHash }, off];
	}
}

export const GetHeadersMessage: PeerMessage<GetHeadersCodec> = {
	command: "getheaders",
	codec: new GetHeadersCodec(),
};
