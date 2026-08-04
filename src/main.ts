import { chainStore } from "~/chain/ChainStore.ts";
import { GENESIS_BLOCK_HASH, GENESIS_BLOCK_HEADER_DECODED } from "~/chain/genesis.ts";
import { ARGS } from "~/env.ts";

function wireWorker(name: string, url: URL): Worker {
	const worker = new Worker(url, { type: "module", name });
	worker.addEventListener("error", (e) => {
		console.error(
			`[main] worker "${name}" error:`,
			e.message,
			e.filename ? `(${e.filename}:${e.lineno}:${e.colno})` : "",
			e.error ?? "",
		);
	});
	worker.addEventListener("messageerror", (e) => {
		console.error(`[main] worker "${name}" messageerror:`, e);
	});
	return worker;
}

if (import.meta.main) {
	// Dont wait for workers, dont wait for event loop, we can recover from anything, just destory that shit
	Deno.addSignalListener("SIGINT", () => Deno.kill(Deno.pid, "SIGKILL"));

	console.log("[main] rolling back to last pinned sizes");
	chainStore.atomic.recover();

	if (chainStore.stores.header.size() === 0) {
		const height = chainStore.stores.header.commit(GENESIS_BLOCK_HEADER_DECODED);
		chainStore.stores.blockhash.commit(GENESIS_BLOCK_HASH, chainStore.stores.blockhash.value.encode(height));
		chainStore.atomic.pin(["header", "blockhash"]);
		console.log("[main] seeded genesis header");
	}

	console.log("[main] spawning p2p + chain workers");
	const p2pWorker = wireWorker("p2p", new URL("./p2p/worker.ts", import.meta.url));
	const chainWorker = wireWorker("chain", new URL("./chain/worker.ts", import.meta.url));

	await Promise.all([
		new Promise((resolve) => p2pWorker.addEventListener("message", resolve, { once: true })),
		new Promise((resolve) => chainWorker.addEventListener("message", resolve, { once: true })),
	]);

	const syncMessageChannel = new MessageChannel();
	p2pWorker.postMessage(null, [syncMessageChannel.port1]);
	chainWorker.postMessage(null, [syncMessageChannel.port2]);

	const serverWorker = wireWorker("server", new URL("./app/app.worker.ts", import.meta.url));
	await new Promise((resolve) => serverWorker.addEventListener("message", resolve, { once: true }));
	console.log("[main] server worker up");

	if (!ARGS.background) wireWorker("gui", new URL("./app/gui.worker.ts", import.meta.url));
	console.log("[main] startup complete");
}
