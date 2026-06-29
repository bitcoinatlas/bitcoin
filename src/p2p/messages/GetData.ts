import { Codec, Stride } from "@nomadshiba/codec";
import { CompactSize } from "~/codec/primitives/CompactSize.ts";
import { type PeerMessage } from "~/p2p/Peer.ts";

export const MSG_BLOCK = 2;
/** Request block including segwit witness data. Requires peer version ≥ 70013 and NODE_WITNESS service. */
export const MSG_WITNESS_BLOCK = 0x40000002;
/** NODE_WITNESS service flag — peer supports segwit (BIP 144). */
export const NODE_WITNESS = 0x8n;

export type InvVector = {
	type: number; // MSG_TX=1, MSG_BLOCK=2
	hash: Uint8Array; // 32 bytes
};

export type GetDataPayload = {
	inventory: InvVector[];
};

class GetDataCodec extends Codec<GetDataPayload> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	public encode(data: GetDataPayload): Uint8Array<ArrayBuffer> {
		const count = data.inventory.length;
		const out = new Uint8Array(CompactSize.encode(count).length + count * 36);
		this.encodeInto(data, out);
		return out;
	}

	public override encodeInto(data: GetDataPayload, target: Uint8Array, offset: number = 0): number {
		const start = offset;
		offset += CompactSize.encodeInto(data.inventory.length, target, offset);
		for (const inv of data.inventory) {
			target[offset] = inv.type & 0xff;
			target[offset + 1] = (inv.type >>> 8) & 0xff;
			target[offset + 2] = (inv.type >>> 16) & 0xff;
			target[offset + 3] = (inv.type >>> 24) & 0xff;
			target.set(inv.hash, offset + 4);
			offset += 36;
		}
		return offset - start;
	}

	public decodeFrom(bytes: Uint8Array, offset: number): [GetDataPayload, number] {
		const [count, csLen] = CompactSize.decodeFrom(bytes, offset);
		let off = offset + csLen;
		const inventory: InvVector[] = [];
		for (let i = 0; i < count; i++) {
			const type = (bytes[off]! | (bytes[off + 1]! << 8) | (bytes[off + 2]! << 16) | (bytes[off + 3]! << 24)) >>>
				0;
			const hash = bytes.slice(off + 4, off + 36);
			inventory.push({ type, hash });
			off += 36;
		}
		return [{ inventory }, off - offset];
	}
}

export const GetDataMessage: PeerMessage<GetDataCodec> = {
	command: "getdata",
	codec: new GetDataCodec(),
};
