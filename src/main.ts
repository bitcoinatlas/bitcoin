import { ARGS } from "~/env.ts";
import { chainStore } from "~/chain/ChainStorage.ts";

if (import.meta.main) {
	Deno.addSignalListener("SIGINT", () => {
		// Dont wait for workers, dont wait for event loop, we can recover from anything, just destory that shit
		Deno.kill(Deno.pid, "SIGKILL");
	});

	chainStore.atomic.rollback();

	const p2pWorker = new Worker(new URL("./p2p/worker.ts", import.meta.url), { type: "module", name: "p2p" });
	const chainWorker = new Worker(new URL("./chain/worker.ts", import.meta.url), { type: "module", name: "chain" });

	// Hand each worker its end of the sync channel immediately. Messages posted to
	// a worker before its listener attaches are queued by the runtime, so there's
	// nothing to wait for — the workers just start the moment they have the port.
	const syncMessageChannel = new MessageChannel();
	p2pWorker.postMessage(null, [syncMessageChannel.port1]);
	chainWorker.postMessage(null, [syncMessageChannel.port2]);

	const serverWorker = new Worker(new URL("./app/app.worker.ts", import.meta.url), { type: "module", name: "server" });
	await new Promise((resolve) => serverWorker.addEventListener("message", resolve, { once: true }));

	if (!ARGS.background) {
		new Worker(new URL("./app/gui.worker.ts", import.meta.url), { type: "module", name: "gui" });
	}
}
