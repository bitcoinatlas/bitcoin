import { Codec } from "@nomadshiba/codec";
import { MAX_BLOCK_WEIGHT } from "~/constants.ts";
import { Uint8ArrayView } from "~/lib/Uint8ArrayView.ts";

export class CompactSizeCodec extends Codec<number> {
	readonly stride = -1;

	encode(n: number): Uint8Array {
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

	decode(data: Uint8Array): [number, number] {
		const view = new Uint8ArrayView(data);
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

export class U24LECodec extends Codec<number> {
	readonly stride = 3;

	encode(value: number): Uint8Array {
		if (value < 0 || value > 0xffffff || !Number.isInteger(value)) {
			throw new RangeError("Value out of range for U24LE");
		}
		const arr = new Uint8Array(3);
		arr[0] = value & 0xff;
		arr[1] = (value >>> 8) & 0xff;
		arr[2] = (value >>> 16) & 0xff;
		return arr;
	}

	decode(data: Uint8Array): [number, number] {
		if (data.length < 3) throw new Error("Not enough bytes for U24LE");
		const value = data[0]! | (data[1]! << 8) | (data[2]! << 16);
		return [value, 3];
	}
}

export const U24LE = new U24LECodec();

const MAX_U48 = 2 ** 48 - 1;

export class U48LECodec extends Codec<number> {
	readonly stride = 6;

	encode(value: number): Uint8Array {
		if (value < 0 || value > MAX_U48 || !Number.isInteger(value)) {
			throw new RangeError("Value out of range for U48LE");
		}
		const arr = new Uint8Array(6);
		arr[0] = value & 0xff;
		arr[1] = (value >>> 8) & 0xff;
		arr[2] = (value >>> 16) & 0xff;
		arr[3] = (value >>> 24) & 0xff;
		const hi = Math.floor(value / 0x100000000);
		arr[4] = hi & 0xff;
		arr[5] = (hi >>> 8) & 0xff;
		return arr;
	}

	decode(data: Uint8Array): [number, number] {
		if (data.length < 6) throw new Error("Not enough bytes for U48LE");
		const lo = (data[0]! | (data[1]! << 8) | (data[2]! << 16) | (data[3]! << 24)) >>> 0;
		const hi = data[4]! | (data[5]! << 8);
		return [hi * 0x100000000 + lo, 6];
	}
}

export const U48LE = new U48LECodec();

export class U56LECodec extends Codec<bigint> {
	readonly stride = 7;

	encode(value: bigint): Uint8Array {
		if (value < 0n || value > 0x00ffffffffffffffn) {
			throw new RangeError("Value out of range for U56LE");
		}
		const arr = new Uint8Array(7);
		for (let i = 0; i < 7; i++) {
			arr[i] = Number((value >> BigInt(i * 8)) & 0xffn);
		}
		return arr;
	}

	decode(data: Uint8Array): [bigint, number] {
		if (data.length < 7) throw new Error("Not enough bytes for U56LE");
		let value = 0n;
		for (let i = 0; i < 7; i++) {
			value |= BigInt(data[i]!) << BigInt(i * 8);
		}
		return [value, 7];
	}
}

export const U56LE = new U56LECodec();

export class Bytes32Codec extends Codec<Uint8Array> {
	readonly stride = 32;

	encode(value: Uint8Array): Uint8Array {
		if (value.length !== 32) {
			throw new RangeError(`Expected 32 bytes, got ${value.length}`);
		}
		return value.slice();
	}

	decode(data: Uint8Array): [Uint8Array, number] {
		if (data.length < 32) throw new Error("Not enough bytes for Bytes32");
		return [data.subarray(0, 32), 32];
	}
}

export const Bytes32 = new Bytes32Codec();
