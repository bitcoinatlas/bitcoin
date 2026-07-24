import { ArrayCodec, Bytes, Codec, NullableCodec, StructCodec, U32, U64LE, VarInt, Void } from "@nomadshiba/codec";
import { Schema } from "~/app/libs/routing/Router.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";

// TODO: move these codecs to relevant places later

export type Block = Codec.InferOutput<typeof Block>;
export const Block = new StructCodec({
	header: WireBlockHeader,
	height: U32,
	size: new NullableCodec(VarInt),
});

export type BlockSummary = Codec.InferOutput<typeof BlockSummary>;
export const BlockSummary = new StructCodec({
	txCount: U32,
	reward: U64LE,
	coinbaseScriptSig: Bytes,
});

export type RoutesSchema = typeof ROUTES_SCHEMA;
export const ROUTES_SCHEMA = {
	"GET /v1/block?from=:from&take=:take": { input: Void, output: new ArrayCodec(Block) },
	"GET /v1/block?to=:to&take=:take": { input: Void, output: new ArrayCodec(Block) },
	"GET /v1/block/tip": { input: Void, output: new NullableCodec(Block) },
	"GET /v1/block/:hashOrHeight": { input: Void, output: new NullableCodec(Block) },
	"GET /v1/block/:hashOrHeight/summary": { input: Void, output: new NullableCodec(BlockSummary) },
	"GET /v1/block/:hashOrHeight/txs": { input: Void, output: new ArrayCodec(WireTx) },
	"GET /v1/tx/:txId": { input: Void, output: new NullableCodec(WireTx) },
	"GET /exit": { input: Void, output: Void },
} as const satisfies Schema;
