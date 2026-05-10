import { ArrayCodec, NullableCodec, StructCodec, Void } from "@nomadshiba/codec";
import type { EndpointSchema } from "~/lib/EndpointRouter.ts";
import { WireBlockHeader } from "~/lib/codec/wire/WireBlockHeader.ts";
import { WireTx } from "~/lib/codec/wire/WireTx.ts";
import { U48LE } from "~/lib/codec/primitives.ts";

const Block = new StructCodec({
	header: WireBlockHeader,
	height: U48LE,
});

export const ENDPOINT_SCHEMA = {
	"GET /v1/block": { input: Void, output: new ArrayCodec(Block) },
	"GET /v1/block/tip": { input: Void, output: new NullableCodec(Block) },
	"GET /v1/block/:hashOrHeight": { input: Void, output: new NullableCodec(Block) },
	"GET /v1/block/:hashOrHeight/txs": { input: Void, output: new ArrayCodec(WireTx) },
	"GET /v1/tx/:txId": { input: Void, output: new NullableCodec(WireTx) },
} as const satisfies EndpointSchema;
