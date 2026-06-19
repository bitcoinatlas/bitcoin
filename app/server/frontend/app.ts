import appCss from "./app.css" with { type: "text" };
import { api } from "~/server/frontend/api.ts";
import { formatHash } from "~/server/frontend/utils/format.ts";

const appSheet = new CSSStyleSheet();
appSheet.replaceSync(appCss);
document.adoptedStyleSheets.push(appSheet);

const FUZZ_BLOCK_COUNT = 100;
const FUZZ_CONCURRENCY = 4;
let fuzzStopped = false;

async function fuzzBlockEndpoints(workerId: number) {
	let blockHeight = workerId;
	let round = 0;

	while (!fuzzStopped) {
		const height = blockHeight % FUZZ_BLOCK_COUNT;
		blockHeight += FUZZ_CONCURRENCY;
		round++;

		try {
			if (round % FUZZ_BLOCK_COUNT === 1) {
				await api.fetch("GET /v1/block/tip", {});
				await api.fetch("GET /v1/block", { search: { from: 0, take: FUZZ_BLOCK_COUNT } });
			}

			const block = await api.fetch("GET /v1/block/:hashOrHeight", { pathname: { hashOrHeight: height } });
			const txs = await api.fetch("GET /v1/block/:hashOrHeight/txs", { pathname: { hashOrHeight: height } });

			if (block) {
				await api.fetch("GET /v1/block/:hashOrHeight", { pathname: { hashOrHeight: formatHash(block.header.hash()) } });
			}

			for (const tx of txs.slice(0, 8)) {
				await api.fetch("GET /v1/tx/:txId", { pathname: { txId: formatHash(tx.stored.txId) } });
			}
		} catch (error) {
			fuzzStopped = true;
			console.error("[endpoint-fuzz] request failed", { workerId, height, error });
		}
	}
}

console.log(`[endpoint-fuzz] hammering first ${FUZZ_BLOCK_COUNT} blocks with ${FUZZ_CONCURRENCY} workers`);
for (let workerId = 0; workerId < FUZZ_CONCURRENCY; workerId++) {
	void fuzzBlockEndpoints(workerId);
}
