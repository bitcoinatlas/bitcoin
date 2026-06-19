import { ArrayCodec, NullableCodec, StructCodec, U32, Void } from "@nomadshiba/codec";
import type { EndpointSchema } from "~/app/backend/utils/EndpointRouter.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";

const Block = new StructCodec({
	header: WireBlockHeader,
	height: U32,
});

const Tx = new StructCodec({
	wire: WireTx,
	stored: StoredTx,
});

export const ENDPOINT_SCHEMA = {
	"GET /v1/block": { input: Void, output: new ArrayCodec(Block) },
	"GET /v1/block/tip": { input: Void, output: new NullableCodec(Block) },
	"GET /v1/block/:hashOrHeight": { input: Void, output: new NullableCodec(Block) },
	"GET /v1/block/:hashOrHeight/txs": { input: Void, output: new ArrayCodec(Tx) },
	"GET /v1/tx/:txId": { input: Void, output: new NullableCodec(Tx) },
	"GET /exit": { input: Void, output: Void },
} as const satisfies EndpointSchema;
