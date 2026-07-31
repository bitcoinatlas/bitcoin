import { ARGS } from "~/env.ts";
import { chainStore } from "~/chain/ChainStore.ts";

// Surface anything a worker throws — at load or at runtime. Without this an
// uncaught error in a worker dies silently and you see nothing at all.
function wireWorker(name: string, worker: Worker): Worker {
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
	Deno.addSignalListener("SIGINT", () => {
		// Dont wait for workers, dont wait for event loop, we can recover from anything, just destory that shit
		Deno.kill(Deno.pid, "SIGKILL");
	});

	console.log("[main] rolling back to last pinned sizes");
	chainStore.atomic.rollback();

	console.log("[main] spawning p2p + chain workers");
	const p2pWorker = wireWorker("p2p", new Worker(new URL("./p2p/worker.ts", import.meta.url), { type: "module", name: "p2p" }));
	const chainWorker = wireWorker("chain", new Worker(new URL("./chain/worker.ts", import.meta.url), { type: "module", name: "chain" }));

	// Hand each worker its end of the sync channel immediately. Messages posted to
	// a worker before its listener attaches are queued by the runtime, so there's
	// nothing to wait for — the workers just start the moment they have the port.
	const syncMessageChannel = new MessageChannel();
	p2pWorker.postMessage(null, [syncMessageChannel.port1]);
	chainWorker.postMessage(null, [syncMessageChannel.port2]);
	console.log("[main] sync ports handed over");

	const serverWorker = wireWorker(
		"server",
		new Worker(new URL("./app/app.worker.ts", import.meta.url), { type: "module", name: "server" }),
	);
	await new Promise((resolve) => serverWorker.addEventListener("message", resolve, { once: true }));
	console.log("[main] server worker up");

	if (!ARGS.background) {
		wireWorker("gui", new Worker(new URL("./app/gui.worker.ts", import.meta.url), { type: "module", name: "gui" }));
	}
	console.log("[main] startup complete");
}
