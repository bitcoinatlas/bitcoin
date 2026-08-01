import { ARGS } from "~/env.ts";
import { chainStore } from "~/chain/ChainStore.ts";
import { GENESIS_BLOCK_HASH, GENESIS_BLOCK_HEADER_DECODED } from "~/chain/genesis.ts";
import { controlSab, initRootControl, markReady } from "~/libs/storage/control.ts";

// Surface anything a worker throws — at load or at runtime. Without this an
// uncaught error in a worker dies silently and you see nothing at all.
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
	Deno.addSignalListener("SIGINT", () => {
		// Dont wait for workers, dont wait for event loop, we can recover from anything, just destory that shit
		Deno.kill(Deno.pid, "SIGKILL");
	});

	// The one shared-memory control block for the whole process. Created before
	// any store access so main's own reads/writes are fenced; its `.sab` is
	// forwarded to every worker that touches a store below.
	initRootControl();
	markReady();

	console.log("[main] rolling back to last pinned sizes");
	chainStore.atomic.rollback();

	if (chainStore.stores.header.size() === 0) {
		const height = chainStore.stores.header.push(GENESIS_BLOCK_HEADER_DECODED);
		chainStore.stores.blockhash.put(GENESIS_BLOCK_HASH, height);
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

	// Hand each worker its end of the sync channel immediately. Messages posted to
	// a worker before its listener attaches are queued by the runtime, so there's
	// nothing to wait for — the workers just start the moment they have the port.
	const syncMessageChannel = new MessageChannel();
	p2pWorker.postMessage({ control: controlSab() }, [syncMessageChannel.port1]);
	chainWorker.postMessage({ control: controlSab() }, [syncMessageChannel.port2]);
	console.log("[main] sync ports handed over");

	const serverWorker = wireWorker("server", new URL("./app/app.worker.ts", import.meta.url));
	await new Promise((resolve) => serverWorker.addEventListener("message", resolve, { once: true }));
	serverWorker.postMessage({ control: controlSab() });
	console.log("[main] server worker up");

	if (!ARGS.background) {
		wireWorker("gui", new URL("./app/gui.worker.ts", import.meta.url));
	}
	console.log("[main] startup complete");
}
