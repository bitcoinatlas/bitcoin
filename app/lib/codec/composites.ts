import type { Dyn, Impl } from "~/traits.ts";
import { dyn } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { decodeVarInt, encodeVarInt } from "~/lib/codec/varint.ts";
import { Stride } from "./mod.ts";

// ── Tuple ──
// Fixed-length tuple of potentially different types.
// Concatenates each element (no wrapper prefix).

export type TupleCodec<T extends readonly unknown[] = readonly unknown[]> = {
	stride: Stride;
	codecs: { [K in keyof T]: Dyn<Codec<any, T[K]>> };
};

export const TupleCodec = {
	create<T extends readonly unknown[]>(
		codecs: { [K in keyof T]: Dyn<Codec<any, T[K]>> },
	): TupleCodec<T> {
		let totalStride = 0;
		let isVariable = false;
		for (const codec of codecs) {
			const s = codec.stride();
			if (s.type === "variable") {
				isVariable = true;
				break;
			}
			totalStride += s.size;
		}
		return {
			stride: isVariable ? Stride.variable() : Stride.fixed(totalStride),
			codecs,
		};
	},
	stride(self) {
		return self.stride;
	},
	encode<T extends readonly unknown[]>(self: TupleCodec<T>, value: [...T]): Uint8Array {
		const parts: Uint8Array[] = [];
		for (let i = 0; i < self.codecs.length; i++) {
			parts.push(self.codecs[i]!.encode(value[i]));
		}
		const totalLength = parts.reduce((sum, p) => sum + p.length, 0);
		const result = new Uint8Array(totalLength);
		let offset = 0;
		for (const part of parts) {
			result.set(part, offset);
			offset += part.length;
		}
		return result;
	},
	decode<T extends readonly unknown[]>(self: TupleCodec<T>, data: Uint8Array): [T, number] {
		const result: unknown[] = [];
		let offset = 0;
		for (let i = 0; i < self.codecs.length; i++) {
			const [value, size] = self.codecs[i]!.decode(data.subarray(offset));
			result[i] = value;
			offset += size;
		}
		return [result as unknown as T, offset];
	},
} satisfies Impl<TupleCodec, Codec<TupleCodec>>;

// ── Struct ──
// Named fields, stored as a tuple in DEFINITION ORDER.

export type StructCodec<T extends Record<string, unknown> = Record<string, unknown>> = {
	stride: Stride;
	keys: (keyof T & string)[];
	codecs: { [K in keyof T]: Dyn<Codec<any, T[K]>> };
};

export const StructCodec = {
	create<T extends Record<string, unknown>>(
		shape: { [K in keyof T]: Dyn<Codec<any, T[K]>> },
	): StructCodec<T> {
		const keys = Object.keys(shape) as (keyof T & string)[];
		let totalStride = 0;
		let isVariable = false;
		for (const key of keys) {
			const s = shape[key]!.stride();
			if (s.type === "variable") {
				isVariable = true;
				break;
			}
			totalStride += s.size;
		}
		return {
			stride: isVariable ? Stride.variable() : Stride.fixed(totalStride),
			keys,
			codecs: shape,
		};
	},
	stride(self) {
		return self.stride;
	},
	encode<T extends Record<string, unknown>>(self: StructCodec<T>, value: T): Uint8Array {
		const parts: Uint8Array[] = [];
		for (const key of self.keys) {
			parts.push(self.codecs[key]!.encode(value[key]));
		}
		const totalLength = parts.reduce((sum, p) => sum + p.length, 0);
		const result = new Uint8Array(totalLength);
		let offset = 0;
		for (const part of parts) {
			result.set(part, offset);
			offset += part.length;
		}
		return result;
	},
	decode<T extends Record<string, unknown>>(self: StructCodec<T>, data: Uint8Array): [T, number] {
		const result: Record<string, unknown> = {};
		let offset = 0;
		for (const key of self.keys) {
			const [value, size] = self.codecs[key]!.decode(data.subarray(offset));
			result[key] = value;
			offset += size;
		}
		return [result as T, offset];
	},
} satisfies Impl<StructCodec, Codec<StructCodec>>;

// ── Vector ──
// Variable-length array. Varint count prefix + concatenated elements.

export type VectorCodec<T = unknown> = {
	stride: Stride;
	codec: Dyn<Codec<any, T>>;
};

export const VectorCodec = {
	create<T>(codec: Dyn<Codec<any, T>>): VectorCodec<T> {
		return { stride: Stride.variable(), codec };
	},
	stride(self) {
		return self.stride;
	},
	encode<T>(self: VectorCodec<T>, value: T[]): Uint8Array {
		const parts: Uint8Array[] = [];
		for (const item of value) {
			parts.push(self.codec.encode(item));
		}
		const elementsLength = parts.reduce((sum, p) => sum + p.length, 0);
		const elementsData = new Uint8Array(elementsLength);
		let offset = 0;
		for (const part of parts) {
			elementsData.set(part, offset);
			offset += part.length;
		}
		const countPrefix = encodeVarInt(value.length);
		const result = new Uint8Array(countPrefix.length + elementsData.length);
		result.set(countPrefix, 0);
		result.set(elementsData, countPrefix.length);
		return result;
	},
	decode<T>(self: VectorCodec<T>, data: Uint8Array): [T[], number] {
		const [count, bytesRead] = decodeVarInt(data);
		const result: T[] = [];
		let offset = bytesRead;
		for (let i = 0; i < count; i++) {
			const [value, size] = self.codec.decode(data.subarray(offset));
			result.push(value);
			offset += size;
		}
		return [result, offset];
	},
} satisfies Impl<VectorCodec, Codec<VectorCodec>>;

// ── Option ──
// 0x00 for null, 0x01 + payload for present.

export type OptionCodec<T = unknown> = {
	stride: Stride;
	codec: Dyn<Codec<any, T>>;
};

export const OptionCodec = {
	create<T>(codec: Dyn<Codec<any, T>>): OptionCodec<T> {
		return { stride: Stride.variable(), codec };
	},
	stride(self) {
		return self.stride;
	},
	encode<T>(self: OptionCodec<T>, value: T | null): Uint8Array {
		if (value === null) {
			return new Uint8Array([0]);
		}
		const encoded = self.codec.encode(value);
		const result = new Uint8Array(1 + encoded.length);
		result[0] = 1;
		result.set(encoded, 1);
		return result;
	},
	decode<T>(self: OptionCodec<T>, data: Uint8Array): [T | null, number] {
		if (data[0] === 0) {
			return [null, 1];
		}
		const [value, size] = self.codec.decode(data.subarray(1));
		return [value, 1 + size];
	},
} satisfies Impl<OptionCodec, Codec<OptionCodec>>;

// ── Enum ──
// 1-byte variant index (sorted by name) + payload.

export type EnumValue<T extends Record<string, unknown> = Record<string, unknown>> = {
	[K in keyof T & string]: { kind: K; value: T[K] };
}[keyof T & string];

export type EnumCodec<T extends Record<string, unknown> = Record<string, unknown>> = {
	stride: Stride;
	keys: (keyof T & string)[];
	variants: { [K in keyof T]: Dyn<Codec<any, T[K]>> };
};

export const EnumCodec = {
	create<T extends Record<string, unknown>>(
		variants: { [K in keyof T]: Dyn<Codec<any, T[K]>> },
	): EnumCodec<T> {
		const keys = (Object.keys(variants) as (keyof T & string)[]).sort();
		let commonStride: number | null = null;
		let isFixed = true;
		for (const key of keys) {
			const s = variants[key]!.stride();
			if (s.type === "variable") {
				isFixed = false;
				break;
			}
			if (commonStride === null) {
				commonStride = s.size;
			} else if (commonStride !== s.size) {
				isFixed = false;
				break;
			}
		}
		const stride = isFixed && commonStride !== null ? Stride.fixed(1 + commonStride) : Stride.variable();
		return { stride, keys, variants };
	},
	stride(self) {
		return self.stride;
	},
	encode<T extends Record<string, unknown>>(self: EnumCodec<T>, value: EnumValue<T>): Uint8Array {
		const index = self.keys.indexOf(value.kind);
		if (index === -1) {
			throw new Error(`Invalid enum variant: ${value.kind}`);
		}
		const codec = self.variants[value.kind]!;
		const encodedValue = codec.encode(value.value);
		const result = new Uint8Array(1 + encodedValue.length);
		result[0] = index;
		result.set(encodedValue, 1);
		return result;
	},
	decode<T extends Record<string, unknown>>(self: EnumCodec<T>, data: Uint8Array): [EnumValue<T>, number] {
		const index = data[0]!;
		if (index >= self.keys.length) {
			throw new Error(`Invalid enum index: ${index}`);
		}
		const key = self.keys[index]!;
		const codec = self.variants[key]!;
		const [value, size] = codec.decode(data.subarray(1));
		return [{ kind: key, value } as EnumValue<T>, 1 + size];
	},
} satisfies Impl<EnumCodec, Codec<EnumCodec>>;

// ── Mapping ──
// Encoded as a Vector of Tuple [key, value].

export type MappingCodec<K = unknown, V = unknown> = {
	stride: Stride;
	entries: VectorCodec<[K, V]>;
};

export const MappingCodec = {
	create<K, V>(keyCodec: Dyn<Codec<any, K>>, valueCodec: Dyn<Codec<any, V>>): MappingCodec<K, V> {
		const tupleCodec = TupleCodec.create<[K, V]>([keyCodec, valueCodec] as any);
		const tupleDyn = dyn(TupleCodec, tupleCodec) as Dyn<Codec<any, [K, V]>>;
		const entries = VectorCodec.create(tupleDyn);
		return { stride: Stride.variable(), entries };
	},
	stride(self) {
		return self.stride;
	},
	encode<K, V>(self: MappingCodec<K, V>, value: Map<K, V>): Uint8Array {
		const entries = Array.from(value.entries()) as [K, V][];
		return VectorCodec.encode(self.entries, entries);
	},
	decode<K, V>(self: MappingCodec<K, V>, data: Uint8Array): [Map<K, V>, number] {
		const [entries, size] = VectorCodec.decode(self.entries, data);
		return [new Map(entries), size];
	},
} satisfies Impl<MappingCodec, Codec<MappingCodec>>;
