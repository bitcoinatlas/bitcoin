import { ARGS } from "~/env.ts";
import { atomic } from "~/chain/atomic.ts";

const global = globalThis as typeof globalThis & { gc?(): void };

if (import.meta.main) {
	Deno.addSignalListener("SIGINT", () => Deno.exit(0));

	atomic.recover();
	const p2pWorker = new Worker(new URL("./p2p/worker.ts", import.meta.url), { type: "module", name: "p2p" });
	const chainWorker = new Worker(new URL("./chain/chain.worker.ts", import.meta.url), { type: "module", name: "chain" });
	const syncMessageChannel = new MessageChannel();
	Promise.all([
		new Promise((resolve) => chainWorker.addEventListener("message", resolve, { once: true })),
		new Promise((resolve) => p2pWorker.addEventListener("message", resolve, { once: true })),
	]).then(() => {
		p2pWorker.postMessage(null, [syncMessageChannel.port1]);
		chainWorker.postMessage(null, [syncMessageChannel.port2]);
	});

	// registerEndpoints(chainStore);
	// serve(58333);

	if (!ARGS.background) {
		new Worker(new URL("./app/gui.worker.ts", import.meta.url), { type: "module", name: "gui" });
	}
}
