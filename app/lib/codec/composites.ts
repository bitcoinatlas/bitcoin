import type { Dyn, Impl } from "~/traits.ts";
import { dyn } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { decodeVarInt, encodeVarInt } from "~/lib/codec/varint.ts";

// ── Tuple ──
// Fixed-length tuple of potentially different types.
// Concatenates each element (no wrapper prefix).

export type TupleSchema<T extends readonly unknown[] = readonly unknown[]> = {
	stride: number;
	codecs: { [K in keyof T]: Dyn<Codec<any, T[K]>> };
};

export const TupleCodec = {
	...CodecDefaults<TupleSchema>(),
	create<T extends readonly unknown[]>(
		codecs: { [K in keyof T]: Dyn<Codec<any, T[K]>> },
	): TupleSchema<T> {
		let totalStride = 0;
		let isVariable = false;
		for (const codec of codecs) {
			const s = codec.stride();
			if (s < 0) {
				isVariable = true;
				break;
			}
			totalStride += s;
		}
		return {
			stride: isVariable ? -1 : totalStride,
			codecs,
		};
	},
	encode<T extends readonly unknown[]>(self: TupleSchema<T>, value: [...T]): Uint8Array {
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
	decode<T extends readonly unknown[]>(self: TupleSchema<T>, data: Uint8Array): [T, number] {
		const result: unknown[] = [];
		let offset = 0;
		for (let i = 0; i < self.codecs.length; i++) {
			const [value, size] = self.codecs[i]!.decode(data.subarray(offset));
			result[i] = value;
			offset += size;
		}
		return [result as unknown as T, offset];
	},
} satisfies Impl<TupleSchema, Codec<TupleSchema, readonly unknown[]>>;

// ── Struct ──
// Named fields, stored as a tuple in DEFINITION ORDER.

export type StructSchema<T extends Record<string, unknown> = Record<string, unknown>> = {
	stride: number;
	keys: (keyof T & string)[];
	codecs: { [K in keyof T]: Dyn<Codec<any, T[K]>> };
};

export const StructCodec = {
	...CodecDefaults<StructSchema>(),
	create<T extends Record<string, unknown>>(
		shape: { [K in keyof T]: Dyn<Codec<any, T[K]>> },
	): StructSchema<T> {
		const keys = Object.keys(shape) as (keyof T & string)[];
		let totalStride = 0;
		let isVariable = false;
		for (const key of keys) {
			const s = shape[key]!.stride();
			if (s < 0) {
				isVariable = true;
				break;
			}
			totalStride += s;
		}
		return {
			stride: isVariable ? -1 : totalStride,
			keys,
			codecs: shape,
		};
	},
	encode<T extends Record<string, unknown>>(self: StructSchema<T>, value: T): Uint8Array {
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
	decode<T extends Record<string, unknown>>(self: StructSchema<T>, data: Uint8Array): [T, number] {
		const result: Record<string, unknown> = {};
		let offset = 0;
		for (const key of self.keys) {
			const [value, size] = self.codecs[key]!.decode(data.subarray(offset));
			result[key] = value;
			offset += size;
		}
		return [result as T, offset];
	},
} satisfies Impl<StructSchema, Codec<StructSchema, Record<string, unknown>>>;

// ── Vector ──
// Variable-length array. Varint count prefix + concatenated elements.

export type VectorSchema<T = unknown> = {
	stride: number;
	codec: Dyn<Codec<any, T>>;
};

export const VectorCodec = {
	...CodecDefaults<VectorSchema>(),
	create<T>(codec: Dyn<Codec<any, T>>): VectorSchema<T> {
		return { stride: -1, codec };
	},
	encode<T>(self: VectorSchema<T>, value: T[]): Uint8Array {
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
	decode<T>(self: VectorSchema<T>, data: Uint8Array): [T[], number] {
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
} satisfies Impl<VectorSchema, Codec<VectorSchema, unknown[]>>;

// ── Option ──
// 0x00 for null, 0x01 + payload for present.

export type OptionSchema<T = unknown> = {
	stride: number;
	codec: Dyn<Codec<any, T>>;
};

export const OptionCodec = {
	...CodecDefaults<OptionSchema>(),
	create<T>(codec: Dyn<Codec<any, T>>): OptionSchema<T> {
		return { stride: -1, codec };
	},
	encode<T>(self: OptionSchema<T>, value: T | null): Uint8Array {
		if (value === null) {
			return new Uint8Array([0]);
		}
		const encoded = self.codec.encode(value);
		const result = new Uint8Array(1 + encoded.length);
		result[0] = 1;
		result.set(encoded, 1);
		return result;
	},
	decode<T>(self: OptionSchema<T>, data: Uint8Array): [T | null, number] {
		if (data[0] === 0) {
			return [null, 1];
		}
		const [value, size] = self.codec.decode(data.subarray(1));
		return [value, 1 + size];
	},
} satisfies Impl<OptionSchema, Codec<OptionSchema, unknown | null>>;

// ── Enum ──
// 1-byte variant index (sorted by name) + payload.

export type EnumValue<T extends Record<string, unknown> = Record<string, unknown>> = {
	[K in keyof T & string]: { kind: K; value: T[K] };
}[keyof T & string];

export type EnumSchema<T extends Record<string, unknown> = Record<string, unknown>> = {
	stride: number;
	keys: (keyof T & string)[];
	variants: { [K in keyof T]: Dyn<Codec<any, T[K]>> };
};

export const EnumCodec = {
	...CodecDefaults<EnumSchema>(),
	create<T extends Record<string, unknown>>(
		variants: { [K in keyof T]: Dyn<Codec<any, T[K]>> },
	): EnumSchema<T> {
		const keys = (Object.keys(variants) as (keyof T & string)[]).sort();
		return { stride: -1, keys, variants };
	},
	encode<T extends Record<string, unknown>>(self: EnumSchema<T>, value: EnumValue<T>): Uint8Array {
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
	decode<T extends Record<string, unknown>>(self: EnumSchema<T>, data: Uint8Array): [EnumValue<T>, number] {
		const index = data[0]!;
		if (index >= self.keys.length) {
			throw new Error(`Invalid enum index: ${index}`);
		}
		const key = self.keys[index]!;
		const codec = self.variants[key]!;
		const [value, size] = codec.decode(data.subarray(1));
		return [{ kind: key, value } as EnumValue<T>, 1 + size];
	},
} satisfies Impl<EnumSchema, Codec<EnumSchema, EnumValue>>;

// ── Mapping ──
// Encoded as a Vector of Tuple [key, value].

export type MappingSchema<K = unknown, V = unknown> = {
	stride: number;
	entriesSchema: VectorSchema<[K, V]>;
};

export const MappingCodec = {
	...CodecDefaults<MappingSchema>(),
	create<K, V>(keyCodec: Dyn<Codec<any, K>>, valueCodec: Dyn<Codec<any, V>>): MappingSchema<K, V> {
		const tupleSchema = TupleCodec.create<[K, V]>([keyCodec, valueCodec] as any);
		const tupleDyn = dyn(TupleCodec, tupleSchema) as Dyn<Codec<any, [K, V]>>;
		const entriesSchema = VectorCodec.create(tupleDyn);
		return { stride: -1, entriesSchema };
	},
	encode<K, V>(self: MappingSchema<K, V>, value: Map<K, V>): Uint8Array {
		const entries = Array.from(value.entries()) as [K, V][];
		return VectorCodec.encode(self.entriesSchema, entries);
	},
	decode<K, V>(self: MappingSchema<K, V>, data: Uint8Array): [Map<K, V>, number] {
		const [entries, size] = VectorCodec.decode(self.entriesSchema, data);
		return [new Map(entries), size];
	},
} satisfies Impl<MappingSchema, Codec<MappingSchema, Map<unknown, unknown>>>;
