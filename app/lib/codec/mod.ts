// ── Codec Module ──
// Zero-dependency binary serialization, Rust-style.
// Structs are schemas (carry stride). Values are plain T.

// Core
export {
	type Codec,
	CodecDefaults,
	type HasStride,
	HasStrideDefaults,
	type InferCodecValue,
} from "~/lib/codec/traits.ts";
export { decodeVarInt, encodeVarInt } from "~/lib/codec/varint.ts";

// Primitives
export { Bool, F32, F64, I16, I32, I64, I8, U16, U32, U64, U8 } from "~/lib/codec/primitives.ts";

// Bytes & Strings
export { Bytes, Str } from "~/lib/codec/bytes.ts";

// Composites
export {
	EnumCodec,
	type EnumSchema,
	type EnumValue,
	MappingCodec,
	type MappingSchema,
	OptionCodec,
	type OptionSchema,
	StructCodec,
	type StructSchema,
	TupleCodec,
	type TupleSchema,
	VectorCodec,
	type VectorSchema,
} from "~/lib/codec/composites.ts";

// Bitcoin-specific
export { Bytes32, U24, U48, U56 } from "~/lib/codec/bitcoin.ts";
