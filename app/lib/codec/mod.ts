// ── Codec Module ──
// Zero-dependency binary serialization, Rust-style.
// Structs are schemas (carry stride). Values are plain T.

// Core
export { type Codec, Stride } from "~/lib/codec/traits.ts";
export { decodeVarInt, encodeVarInt } from "~/lib/codec/varint.ts";

// Primitives
export { Bool, I16, I32, I64, I8, U16, U32, U64, U8 } from "~/lib/codec/primitives.ts";

// Bytes & Strings
export { Bytes, Str } from "~/lib/codec/bytes.ts";

// Bitcoin-specific
export { Bytes32, U24, U48, U56 } from "~/lib/codec/bitcoin.ts";
