import { Codec, Stride } from "@nomadshiba/codec";
import { type PeerMessage } from "~/p2p/Peer.ts";
import { Uint8ArrayView } from "~/libs/collections/Uint8ArrayView.ts";

export type GetHeadersPayload = {
	version: number;
	locators: Uint8Array[];
	stopHash: Uint8Array;
};

class GetHeadersCodec extends Codec<GetHeadersPayload> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	public encode(data: GetHeadersPayload): Uint8Array<ArrayBuffer> {
		const out = new Uint8Array(4 + 1 + 32 * data.locators.length + 32);
		this.encodeInto(data, out);
		return out;
	}

	public override encodeInto(data: GetHeadersPayload, target: Uint8Array, offset: number = 0): number {
		const count = data.locators.length;
		if (count >= 0xfd) throw new Error("too many locators");
		const view = new Uint8ArrayView(target);
		view.setUint32(offset, data.version, true);
		target[offset + 4] = count;
		let off = offset + 5;
		for (const hash of data.locators) {
			target.set(hash, off);
			off += 32;
		}
		target.set(data.stopHash, off);
		return 4 + 1 + 32 * count + 32;
	}

	public override size(data: GetHeadersPayload): number {
		return 4 + 1 + 32 * data.locators.length + 32;
	}

	public decode(bytes: Uint8Array): [GetHeadersPayload, number] {
		const view = new Uint8ArrayView(bytes);
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
