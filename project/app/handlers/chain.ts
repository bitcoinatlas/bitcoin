import { ArrayCodec } from "@nomadshiba/codec";
import { decodeHex } from "@std/encoding";
import { RouterSchema } from "~/libs/routing/mod.ts";
import { endpointRouter } from "~/router.ts";
import { Block, Schema, TxSummary } from "~/routes.ts";
import { chainStore } from "~/chain/ChainStore.ts";
import { StoredTx } from "@project/codecs";
import { WireTx } from "@project/codecs";
import { sha256d } from "@project/hashes";

const MAX_BLOCK_TAKE = 210;
const MAX_TX_TAKE = 50;

function parseHashOrHeight(raw: string): { kind: "height"; height: number } | { kind: "hash"; hash: Uint8Array } {
	if (raw.length === 64) {
		return { kind: "hash", hash: decodeHex(raw).reverse() };
	}
	return { kind: "height", height: Number(raw) };
}

function resolveHeight(raw: string): number | undefined {
	const parsed = parseHashOrHeight(raw);
	if (parsed.kind === "height") return Number.isInteger(parsed.height) ? parsed.height : undefined;
	return chainStore.stores.blockhash.get(parsed.hash);
}

async function getBlockByHeight(height: number): Promise<RouterSchema.InferResultInput<Schema, "GET /v1/block/:hashOrHeight">> {
	const header = await chainStore.stores.header.getAsync(height);
	if (!header) return null;
	const block = await chainStore.stores.block.getAsync(height);
	if (!block) return { header, height, info: null };
	const [coinbaseTx] = await chainStore.stores.tx.getAsync(block.txPointer, StoredTx);
	const coinbaseInput = coinbaseTx.inputs[0];
	if (!coinbaseInput) return { header, height, info: null };
	return {
		header,
		height,
		info: {
			wireSize: block.wireSize,
			reward: block.reward,
			txCount: block.txCount,
			coinbaseScriptSig: coinbaseInput.scriptSig,
		},
	};
}

async function getHeaderByRangeAsync(from: number, to: number): Promise<Block[]> {
	const [headers, blocks] = await Promise.all([
		chainStore.stores.header.sliceAsync(from, to + 1),
		chainStore.stores.block.sliceAsync(from, to + 1),
	]);
	return await Promise.all(headers.map(async (header, index): Promise<Block> => {
		const height = from + index;
		const block = blocks[index];
		if (!block) return { header, height, info: null };
		const [coinbaseTx] = await chainStore.stores.tx.getAsync(block.txPointer, StoredTx);
		const coinbaseInput = coinbaseTx.inputs[0];
		if (!coinbaseInput) return { header, height, info: null };
		return {
			header,
			height,
			info: {
				wireSize: block.wireSize,
				reward: block.reward,
				txCount: block.txCount,
				coinbaseScriptSig: coinbaseInput.scriptSig,
			},
		};
	}));
}

endpointRouter.registerHandler("GET /v1/block?from=:from&take=:take", async ({ params }) => {
	const from = Math.max(0, Number(params.search.from));
	if (isNaN(from)) {
		return { status: "BadRequest", message: "Invalid 'from' parameter" };
	}
	const take = Math.min(MAX_BLOCK_TAKE, Number(params.search.take));
	if (isNaN(take)) {
		return { status: "BadRequest", message: "Invalid 'take' parameter" };
	}
	return { status: "OK", data: await getHeaderByRangeAsync(from, from + take - 1) };
});

endpointRouter.registerHandler("GET /v1/block?to=:to&take=:take", async ({ params }) => {
	const to = Math.max(0, Number(params.search.to));
	if (isNaN(to)) {
		return { status: "BadRequest", message: "Invalid 'to' parameter" };
	}
	const take = Math.min(MAX_BLOCK_TAKE, Number(params.search.take));
	if (isNaN(take)) {
		return { status: "BadRequest", message: "Invalid 'take' parameter" };
	}
	return { status: "OK", data: await getHeaderByRangeAsync(Math.max(0, to - take + 1), to) };
});

endpointRouter.registerHandler("GET /v1/block/tip", async () => {
	const height = chainStore.stores.header.size() - 1;
	if (height < 0) throw new Error("not suppose to happen");
	return { status: "OK", data: await getBlockByHeight(height) };
});

endpointRouter.registerHandler("GET /v1/block/:hashOrHeight", async ({ params }) => {
	const height = resolveHeight(params.pathname.hashOrHeight);
	console.log(height);
	if (height === undefined) return { status: "OK", data: null };
	return { status: "OK", data: await getBlockByHeight(height) };
});

endpointRouter.registerHandler("GET /v1/block/:hashOrHeight/txs", async ({ params }) => {
	const height = resolveHeight(params.pathname.hashOrHeight);
	if (height === undefined) return { status: "OK", data: [] };
	const block = await chainStore.stores.block.getAsync(height);
	if (block === undefined) return { status: "OK", data: [] };
	const [txs] = await chainStore.stores.tx.getAsync(block.txPointer, new ArrayCodec(StoredTx, { size: block.txCount }));

	const fromRaw = params.search && "from" in params.search ? Number(params.search["from"]) : 0;
	const takeRaw = params.search && "take" in params.search ? Number(params.search["take"]) : MAX_TX_TAKE;
	const from = Number.isFinite(fromRaw) ? Math.max(0, Math.trunc(fromRaw)) : 0;
	const take = Number.isFinite(takeRaw) && takeRaw > 0 ? Math.min(MAX_TX_TAKE, Math.trunc(takeRaw)) : MAX_TX_TAKE;

	// TODO: this can be better probably? instead of making it from, to based we can make it cursor based. like get next.
	// so we dont have to read everything from the store.
	const slice = txs.slice(from, from + take);
	return {
		status: "OK",
		data: slice.map((tx): TxSummary => {
			const wireTx = StoredTx.toWire(tx, chainStore);
			const encodedWire = WireTx.encode(wireTx); // TODO: we shouldnt need this for txId
			return {
				txId: sha256d(encodedWire) as Uint8Array<ArrayBuffer>, // TODO: we shouldnt have to cast this.
				inputs: tx.inputs.length,
				outputs: tx.outputs.length,
				wireSize: encodedWire.length,
			};
		}),
	};
});

endpointRouter.registerHandler("GET /v1/tx/:txId", async ({ params }) => {
	const txId = Uint8Array.from(decodeHex(params.pathname.txId).reverse());
	const txPointer = chainStore.stores.txid.get(txId);
	if (txPointer === undefined) return { status: "OK", data: null };
	const [tx] = await chainStore.stores.tx.getAsync(txPointer, StoredTx);
	return { status: "OK", data: StoredTx.toWire(tx, chainStore) };
});
