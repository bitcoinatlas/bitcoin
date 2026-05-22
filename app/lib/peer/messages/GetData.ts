import { Codec, Stride } from "@nomadshiba/codec";
import { CompactSize } from "~/lib/codec/primitives.ts";
import { type PeerMessage } from "~/lib/peer/Peer.ts";

export const MSG_BLOCK = 2;

export type InvVector = {
	type: number; // MSG_TX=1, MSG_BLOCK=2
	hash: Uint8Array; // 32 bytes
};

export type GetDataPayload = {
	inventory: InvVector[];
};

class GetDataCodec extends Codec<GetDataPayload> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	encode(data: GetDataPayload): Uint8Array<ArrayBuffer> {
		const count = data.inventory.length;
		const countBytes = CompactSize.encode(count);
		const out = new Uint8Array(countBytes.length + count * 36);
		out.set(countBytes, 0);
		let off = countBytes.length;
		for (const inv of data.inventory) {
			out[off] = inv.type & 0xff;
			out[off + 1] = (inv.type >>> 8) & 0xff;
			out[off + 2] = (inv.type >>> 16) & 0xff;
			out[off + 3] = (inv.type >>> 24) & 0xff;
			out.set(inv.hash, off + 4);
			off += 36;
		}
		return out;
	}

	decode(bytes: Uint8Array): [GetDataPayload, number] {
		const [count, csLen] = CompactSize.decode(bytes);
		let off = csLen;
		const inventory: InvVector[] = [];
		for (let i = 0; i < count; i++) {
			const type = (bytes[off]! | (bytes[off + 1]! << 8) | (bytes[off + 2]! << 16) | (bytes[off + 3]! << 24)) >>>
				0;
			const hash = bytes.slice(off + 4, off + 36);
			inventory.push({ type, hash });
			off += 36;
		}
		return [{ inventory }, off];
	}
}

export const GetDataMessage: PeerMessage<GetDataCodec> = {
	command: "getdata",
	codec: new GetDataCodec(),
};
