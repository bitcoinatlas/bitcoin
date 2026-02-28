import { BytesView } from "~/lib/BytesView.ts";
import { MAX_BLOCK_WEIGHT } from "~/lib/constants.ts";

/*
    Since max size we are going to handle is 4 million, we can use number type safely here.
*/

export namespace CompactSize {
	export function encode(n: number): Uint8Array {
		if (n < 0xfdn) return Uint8Array.of(Number(n));
		if (n <= 0xffffn) {
			const buf = new Uint8Array(3);
			buf[0] = 0xfd;
			new BytesView(buf).setUint16(1, Number(n), true);
			return buf;
		}
		if (n <= 0xffffffffn) {
			const buf = new Uint8Array(5);
			buf[0] = 0xfe;
			new BytesView(buf).setUint32(1, Number(n), true);
			return buf;
		}
		const buf = new Uint8Array(9);
		buf[0] = 0xff;
		new BytesView(buf).setBigUint64(1, BigInt(n), true);
		return buf;
	}

	export function decode(bytes: Uint8Array, offset: number): [value: number, offset: number] {
		const first = bytes[offset]!;
		if (first < 0xfd) return [first, offset + 1];
		if (first === 0xfd) {
			const val = new BytesView(bytes, offset + 1, 2).getUint16(0, true);
			if (val < 0xfd) throw new Error("non-canonical CompactSize");
			return [val, offset + 3];
		}
		if (first === 0xfe) {
			const val = new BytesView(bytes, offset + 1, 4).getUint32(0, true);
			if (val < 0x10000) throw new Error("non-canonical CompactSize");
			return [val, offset + 5];
		}
		const val = new BytesView(bytes, offset + 1, 8).getBigUint64(0, true);
		if (val < 0x100000000n) throw new Error("non-canonical CompactSize");
		if (val > BigInt(MAX_BLOCK_WEIGHT)) throw new Error("CompactSize too large");
		return [Number(val), offset + 9];
	}
}
