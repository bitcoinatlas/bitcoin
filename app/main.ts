import { delay } from "@std/async";
import { atomic } from "~/chain/chain.ts";
import { syncBodiesFromPeers } from "~/chain/bodies.ts";
import { syncHeadersFromPeers } from "~/chain/headers.ts";
import { addPeer, addPeersFromDNS, availablePeers, expireFailed, peers } from "~/p2p/peers.ts";
import { serve } from "~/api/serve.ts";
import { DEV } from "~/config.ts";

if (import.meta.main) {
	const MAGIC = new Uint8Array([0xf9, 0xbe, 0xb4, 0xd9]); // mainnet
	const P2P_PORT = 8333;
	const HTTP_PORT = 50021;
	const SAVE_INTERVAL_MS = 1 * 60_000;
	const SAVE_HEAP_HEADROOM = 1.5 * 1024 * 1024 * 1024;
	const MAX_PEERS = 8;
	const FAILED_RETRY_MS = 5 * 60 * 1000;

	const DNS_SEEDS = [
		"dnsseed.bitcoin.dashjr.org",
	];

	serve(HTTP_PORT);

	// Local dev: single peer. For production, swap with maintain().
	addPeer("192.168.8.10", P2P_PORT, MAGIC);
	// await _maintain();

	let baselineHeap = Deno.memoryUsage().heapUsed;
	let SAVE_HEAP_THRESHOLD = baselineHeap + SAVE_HEAP_HEADROOM;
	console.log(
		`[main] baseline heap=${(baselineHeap / 1024 / 1024).toFixed(1)}MB save threshold=${
			(SAVE_HEAP_THRESHOLD / 1024 / 1024).toFixed(1)
		}MB`,
	);

	let lastSave = Date.now();
	while (true) {
		try {
			await delay(0);
			await tick();
		} catch (error) {
			console.error("[main] tick error:", error);
		}
	}

	async function tick() {
		// await maintain(); // uncomment for production peer management
		await syncHeadersFromPeers();
		const lastBody = await syncBodiesFromPeers();

		const heapUsed = Deno.memoryUsage().heapUsed;
		const elapsed = Date.now() - lastSave;
		if (elapsed >= SAVE_INTERVAL_MS || heapUsed >= SAVE_HEAP_THRESHOLD) {
			const global = globalThis as any;
			if (global.gc) {
				const pre = Deno.memoryUsage().heapUsed;
				global.gc();
				const post = Deno.memoryUsage().heapUsed;

				const mib = (bytes: number) => (bytes / 1024 / 1024).toFixed(2);
				const freed = pre - post;

				console.log(
					`%cGC%c ${mib(pre)} MiB %c→%c ${mib(post)} MiB %c(freed ${mib(freed)} MiB)`,
					"color: #888; font-weight: bold",
					"color: inherit",
					"color: #888",
					"color: inherit",
					freed > 0 ? "color: #4ade80" : "color: #f87171",
				);
			}
			const reason = heapUsed >= SAVE_HEAP_THRESHOLD
				? `heap=${(heapUsed / 1024 / 1024).toFixed(1)}MB >= ${SAVE_HEAP_THRESHOLD / 1024 / 1024}MB`
				: `elapsed=${(elapsed / 1000).toFixed(0)}s >= ${SAVE_INTERVAL_MS / 1000}s`;
			console.log(`[main] saving (${reason})`);
			await atomic.flush();
			lastSave = Date.now();
			baselineHeap = Deno.memoryUsage().heapUsed;
			SAVE_HEAP_THRESHOLD = baselineHeap + SAVE_HEAP_HEADROOM;
		}
	}

	async function _maintain() {
		expireFailed(FAILED_RETRY_MS);

		if (peers().length >= MAX_PEERS) return;

		const available = availablePeers();
		const needed = MAX_PEERS - peers().length;
		await Promise.allSettled(
			available.slice(0, needed).map(({ host, port }) => addPeer(host, port, MAGIC)),
		);

		if (peers().length >= MAX_PEERS) return;

		for (const seed of DNS_SEEDS) {
			const added = await addPeersFromDNS(seed, P2P_PORT);
			if (added > 0) break;
		}
	}
}
