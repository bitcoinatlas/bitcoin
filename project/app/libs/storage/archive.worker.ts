import { createReadStream, createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import zlib from "node:zlib";

// archive.worker — runs zstd archiving on its OWN OS thread.
//
// This exists because Deno's node:zlib runs compression on the calling thread's
// event loop (its async/stream APIs do NOT offload to libuv's threadpool the way
// real Node does). Measured: N in-process "parallel" archives via Promise.all
// gave ~0 speedup. Running each job in a separate Worker (a real thread with its
// own isolate) gives near-linear multicore scaling.
//
// The worker does the file I/O itself: it's handed paths, streams raw -> zstd ->
// tmp, and reports back. Nothing large ever crosses the isolate boundary, which
// matters because chunks are ~1GB.

export type Job = {
	id: number;
	index: number;
	rawPath: string;
	tmpPath: string;
	params: zlib.ZstdOptions["params"];
};

export type Done = { id: number; index: number; ok: true; archivedSize: number };
export type Failed = { id: number; index: number; ok: false; error: string };
export type Result = Done | Failed;

self.onmessage = async (event: MessageEvent<Job>) => {
	const { id, index, rawPath, tmpPath, params } = event.data;
	try {
		const source = createReadStream(rawPath);
		const transform = zlib.createZstdCompress({ params });
		const sink = createWriteStream(tmpPath);
		await pipeline(source, transform, sink);
		const archivedSize = Deno.statSync(tmpPath).size;
		const result: Done = { id, index, ok: true, archivedSize };
		self.postMessage(result);
	} catch (e) {
		const result: Failed = { id, index, ok: false, error: e instanceof Error ? e.stack ?? e.message : String(e) };
		self.postMessage(result);
	}
};
