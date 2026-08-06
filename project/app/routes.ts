import { ArrayCodec, Bytes, Codec, NullableCodec, StructCodec, U32, VarInt, Void } from "@nomadshiba/codec";
import { RouterSchema } from "~/libs/routing/mod.ts";
import { WireBlockHeader } from "@project/codecs";
import { WireTx } from "@project/codecs";
import { Bytes32 } from "@project/codecs";

export type BlockInfo = Codec.InferOutput<typeof BlockInfo>;
export const BlockInfo = new StructCodec({
	wireSize: U32,
	reward: VarInt,
	txCount: VarInt,
	coinbaseScriptSig: Bytes,
});

export type Block = Codec.InferOutput<typeof Block>;
export const Block = new StructCodec({
	header: WireBlockHeader,
	height: U32,
	info: new NullableCodec(BlockInfo),
});

export type TxSummary = Codec.InferOutput<typeof TxSummary>;
export const TxSummary = new StructCodec({
	txId: Bytes32,
	inputs: VarInt,
	outputs: VarInt,
	wireSize: VarInt,
});

export type Tx = Codec.InferOutput<typeof Tx>;
export const Tx = WireTx;

export type Schema = typeof SCHEMA;
export const SCHEMA = {
	"GET /v1/block?from=:from&take=:take": { input: Void, output: new ArrayCodec(Block) },
	"GET /v1/block?to=:to&take=:take": { input: Void, output: new ArrayCodec(Block) },
	"GET /v1/block/tip": { input: Void, output: new NullableCodec(Block) },
	"GET /v1/block/:hashOrHeight": { input: Void, output: new NullableCodec(Block) },
	"GET /v1/block/:hashOrHeight/txs": { input: Void, output: new ArrayCodec(TxSummary) },
	"GET /v1/tx/:txId": { input: Void, output: new NullableCodec(Tx) },
	"GET /exit": { input: Void, output: Void },
} as const satisfies RouterSchema;
