import { Codec, Stride } from "@nomadshiba/codec";
import { MAX_BLOCK_WEIGHT } from "~/constants.ts";
import { Uint8ArrayView } from "~/libs/collections/Uint8ArrayView.ts";

export class CompactSizeCodec extends Codec<number> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	public encode(n: number): Uint8Array<ArrayBuffer> {
		if (n < 0xfd) return Uint8Array.of(n);
		if (n <= 0xffff) {
			const buffer = new Uint8Array(3);
			buffer[0] = 0xfd;
			new Uint8ArrayView(buffer).setUint16(1, n, true);
			return buffer;
		}
		if (n <= 0xffffffff) {
			const buffer = new Uint8Array(5);
			buffer[0] = 0xfe;
			new Uint8ArrayView(buffer).setUint32(1, n, true);
			return buffer;
		}
		const buffer = new Uint8Array(9);
		buffer[0] = 0xff;
		new Uint8ArrayView(buffer).setBigUint64(1, BigInt(n), true);
		return buffer;
	}

	public override encodeInto(n: number, target: Uint8Array, offset: number = 0): number {
		if (n < 0xfd) {
			target[offset] = n;
			return 1;
		}
		const view = new DataView(target.buffer, target.byteOffset + offset);
		if (n <= 0xffff) {
			target[offset] = 0xfd;
			view.setUint16(1, n, true);
			return 3;
		}
		if (n <= 0xffffffff) {
			target[offset] = 0xfe;
			view.setUint32(1, n, true);
			return 5;
		}
		target[offset] = 0xff;
		view.setBigUint64(1, BigInt(n), true);
		return 9;
	}

	public decodeFrom(data: Uint8Array, offset: number): [number, number] {
		const view = new Uint8ArrayView(data, offset);
		const first = view.getUint8(0);
		if (first < 0xfd) return [first, 1];
		if (first === 0xfd) {
			const val = view.getUint16(1, true);
			if (val < 0xfd) throw new Error("non-canonical CompactSize");
			return [val, 3];
		}
		if (first === 0xfe) {
			const val = view.getUint32(1, true);
			if (val < 0x10000) throw new Error("non-canonical CompactSize");
			return [val, 5];
		}
		const val = view.getBigUint64(1, true);
		if (val < 0x100000000n) throw new Error("non-canonical CompactSize");
		if (val > BigInt(MAX_BLOCK_WEIGHT)) throw new Error("CompactSize too large");
		return [Number(val), 9];
	}
}

export const CompactSize = new CompactSizeCodec();
