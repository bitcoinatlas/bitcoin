import { ARGS } from "~/env.ts";
import { chainStorage } from "~/chain/ChainStorage.ts";

if (import.meta.main) {
	Deno.addSignalListener("SIGINT", () => {
		// Dont wait for workers, dont wait for event loop, we can recover from anything, just destory that shit
		Deno.kill(Deno.pid, "SIGKILL");
	});

	chainStorage.atomic.recover();
	const p2pWorker = new Worker(new URL("./p2p/worker.ts", import.meta.url), { type: "module", name: "p2p" });
	const chainWorker = new Worker(new URL("./chain/worker.ts", import.meta.url), { type: "module", name: "chain" });

	const syncMessageChannel = new MessageChannel();
	await Promise.all([
		new Promise((resolve) => chainWorker.addEventListener("message", resolve, { once: true })),
		new Promise((resolve) => p2pWorker.addEventListener("message", resolve, { once: true })),
	]).then(() => {
		p2pWorker.postMessage(null, [syncMessageChannel.port1]);
		chainWorker.postMessage(null, [syncMessageChannel.port2]);
	});

	const serverWorker = new Worker(new URL("./app/app.worker.ts", import.meta.url), { type: "module", name: "server" });
	await new Promise((resolve) => serverWorker.addEventListener("message", resolve, { once: true }));

	if (!ARGS.background) {
		new Worker(new URL("./app/gui.worker.ts", import.meta.url), { type: "module", name: "gui" });
	}
}
