/**
 * KVStore Write-Path Phase Profiler
 *
 * Times every sub-step of the new flat-buffer KVStore write pipeline at 1M entries.
 *
 *   deno run -A ./profile.ts
 */

import { BytesCodec } from "@nomadshiba/codec";
import { Database } from "@db/sqlite";
import { join } from "@std/path";
import { Uint8ArrayMap } from "~/lib/Uint8ArrayMap.ts";

// ─── Config ───────────────────────────────────────────────────────────────────

const KEY_SIZE = 32;
const VALUE_SIZE = 128;
const ENTRIES = 1_000_000;
const SHARD_COUNT = 16;

// ─── Helpers ──────────────────────────────────────────────────────────────────

function randomBytes(size: number): Uint8Array {
	const buf = new Uint8Array(size);
	crypto.getRandomValues(buf);
	return buf;
}

type Phase = { name: string; ms: number; count: number };
const phases: Phase[] = [];

function record(name: string, ms: number, count: number) {
	phases.push({ name, ms, count });
}

function time<T>(fn: () => T): [T, number] {
	const t0 = performance.now();
	const r = fn();
	return [r, performance.now() - t0];
}

async function timeAsync<T>(fn: () => Promise<T>): Promise<[T, number]> {
	const t0 = performance.now();
	const r = await fn();
	return [r, performance.now() - t0];
}

// ─── Profile ──────────────────────────────────────────────────────────────────

async function run(dir: string, keys: Uint8Array[], values: Uint8Array[]) {
	const keyCodec = new BytesCodec({ size: KEY_SIZE });
	const valueCodec = new BytesCodec({ size: VALUE_SIZE });
	const entryStride = KEY_SIZE + VALUE_SIZE;

	// Open shards
	type ShardDB = { db: Database; stmtInsert: ReturnType<Database["prepare"]> };
	const shards: ShardDB[] = [];
	for (let i = 0; i < SHARD_COUNT; i++) {
		const db = new Database(join(dir, `shard-${i}.db`));
		db.exec(`CREATE TABLE IF NOT EXISTS kv (key BLOB PRIMARY KEY, value BLOB)`);
		db.exec(`PRAGMA journal_mode=DELETE`);
		db.exec(`PRAGMA synchronous=NORMAL`);
		db.exec(`PRAGMA cache_size=-16384`);
		shards.push({ db, stmtInsert: db.prepare(`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`) });
	}

	// ── Phase 1: encode keys + values ───────────────────────────────────────
	const encodedKeys: Uint8Array[] = [];
	const encodedValues: Uint8Array[] = [];
	const [, p1] = time(() => {
		for (let i = 0; i < ENTRIES; i++) {
			encodedKeys.push(keyCodec.encode(keys[i]!));
			encodedValues.push(valueCodec.encode(values[i]!));
		}
	});
	record("1. encode (key + value)", p1, ENTRIES);

	// ── Phase 2: batchBuf.set (flat buffer append + batchIndex.set) ──────────
	// This mirrors the new batch.set() path: append to flat buffer, update index.
	let batchBuf = new Uint8Array(entryStride * ENTRIES);
	const batchIndex = new Uint8ArrayMap<number>(ENTRIES);
	const [, p2] = time(() => {
		let batchCount = 0;
		for (let i = 0; i < ENTRIES; i++) {
			const kBytes = encodedKeys[i]!;
			const vBytes = encodedValues[i]!;
			// No duplicate keys in this benchmark, so no in-place overwrite needed.
			const off = batchCount * entryStride;
			batchBuf.set(kBytes, off);
			batchBuf.set(vBytes, off + KEY_SIZE);
			batchIndex.set(kBytes, off);
			batchCount++;
		}
	});
	record("2. batchBuf append + batchIndex.set", p2, ENTRIES);

	// ── Phase 3: batch.apply() — merge batchBuf into staged ──────────────────
	// Mirrors the new apply(): count new keys, grow stagedBuffer, copy entries.
	let stagedBuffer = new Uint8Array(0);
	let stagedCount = 0;
	const stagedIndex = new Uint8ArrayMap<number>(ENTRIES);
	const [, p3] = time(() => {
		// Pre-count new keys (all new in this benchmark since staged starts empty)
		let newCount = 0;
		for (let i = 0; i < ENTRIES; i++) {
			const kOff = i * entryStride;
			if (stagedIndex.get(batchBuf.subarray(kOff, kOff + KEY_SIZE)) === undefined) newCount++;
		}
		// Grow staged once
		stagedBuffer = new Uint8Array(newCount * entryStride);
		// Copy entries + populate stagedIndex
		for (let i = 0; i < ENTRIES; i++) {
			const kOff = i * entryStride;
			const kBytes = batchBuf.subarray(kOff, kOff + KEY_SIZE);
			const vBytes = batchBuf.subarray(kOff + KEY_SIZE, kOff + entryStride);
			const off = stagedCount * entryStride;
			stagedBuffer.set(kBytes, off);
			stagedBuffer.set(vBytes, off + KEY_SIZE);
			stagedIndex.set(kBytes, off);
			stagedCount++;
		}
	});
	record("3. batch.apply (merge batchBuf → staged)", p3, ENTRIES);

	// ── Phase 4a: createWAL() — prepend header, one memcopy ──────────────────
	let walBuf!: Uint8Array;
	const [, p4a] = time(() => {
		const bodyLen = stagedCount * entryStride;
		const buf = new Uint8Array(4 + bodyLen);
		new DataView(buf.buffer).setUint32(0, stagedCount, true);
		buf.set(stagedBuffer.subarray(0, bodyLen), 4); // single memcopy
		walBuf = buf;
	});
	record("4a. createWAL: prepend header (1 memcopy)", p4a, ENTRIES);

	// ── Phase 4b: Deno.writeFile ──────────────────────────────────────────────
	const walPath = join(dir, "data.wal");
	const [, p4b] = await timeAsync(() => Deno.writeFile(walPath, walBuf, { create: true }));
	record(`4b. Deno.writeFile (${(walBuf.length / 1024 / 1024).toFixed(0)}MB WAL)`, p4b, 1);

	// ── Phase 5a: shard routing + lazy BEGIN ─────────────────────────────────
	const active = new Uint8Array(SHARD_COUNT);
	let pos = 4;
	const [, p5a] = time(() => {
		for (let i = 0; i < ENTRIES; i++) {
			const s = (walBuf[pos] as number) % SHARD_COUNT;
			if (!active[s]) {
				shards[s]!.db.exec("BEGIN");
				active[s] = 1;
			}
			pos += entryStride;
		}
	});
	record("5a. shard routing + BEGIN per shard", p5a, ENTRIES);

	// ── Phase 5b: stmtInsert.run × N ─────────────────────────────────────────
	pos = 4;
	const [, p5b] = time(() => {
		for (let i = 0; i < ENTRIES; i++) {
			const s = (walBuf[pos] as number) % SHARD_COUNT;
			shards[s]!.stmtInsert.run(
				walBuf.subarray(pos, pos + KEY_SIZE),
				walBuf.subarray(pos + KEY_SIZE, pos + entryStride),
			);
			pos += entryStride;
		}
	});
	record("5b. stmtInsert.run × N", p5b, ENTRIES);

	// ── Phase 5c: COMMIT per shard ───────────────────────────────────────────
	const [, p5c] = time(() => {
		for (let s = 0; s < SHARD_COUNT; s++) {
			if (active[s]) shards[s]!.db.exec("COMMIT");
		}
	});
	record("5c. COMMIT × shards", p5c, SHARD_COUNT);

	// ── Phase 6: staged.clear() ───────────────────────────────────────────────
	const [, p6] = time(() => {
		stagedBuffer = new Uint8Array(0);
		stagedCount = 0;
		stagedIndex.clear();
	});
	record("6. staged.clear()", p6, ENTRIES);

	// ── Phase 7: Deno.remove (WAL delete) ─────────────────────────────────────
	const [, p7] = await timeAsync(() => Deno.remove(walPath).catch(() => {}));
	record("7. Deno.remove (WAL)", p7, 1);

	for (const shard of shards) shard.db.close();
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
	console.log("KVStore Write-Path Profiler (flat-buffer architecture)");
	console.log(`  Entries: ${ENTRIES.toLocaleString()}, Key: ${KEY_SIZE}B, Value: ${VALUE_SIZE}B, Shards: ${SHARD_COUNT}`);

	console.log("\n  Generating test data...");
	const keys: Uint8Array[] = [];
	const values: Uint8Array[] = [];
	for (let i = 0; i < ENTRIES; i++) {
		keys.push(randomBytes(KEY_SIZE));
		values.push(randomBytes(VALUE_SIZE));
	}

	const dir = await Deno.makeTempDir({ prefix: "kvprofile_" });
	try {
		console.log("  Running...\n");
		await run(dir, keys, values);
	} finally {
		await Deno.remove(dir, { recursive: true });
	}

	// ── Results ───────────────────────────────────────────────────────────────
	const totalMs = phases.reduce((s, p) => s + p.ms, 0);

	const W = [46, 10, 13, 8];
	const header = "Phase".padEnd(W[0]!) + "ms".padStart(W[1]!) + "ops/sec".padStart(W[2]!) + "% total".padStart(W[3]!);
	console.log("═".repeat(header.length));
	console.log("WRITE PATH BREAKDOWN — 1M entries");
	console.log("═".repeat(header.length));
	console.log(header);
	console.log("─".repeat(header.length));

	for (const p of phases) {
		const ops = p.count / (p.ms / 1000);
		const pct = ((p.ms / totalMs) * 100).toFixed(1);
		const bar = "█".repeat(Math.round((p.ms / totalMs) * 20));
		console.log(
			p.name.padEnd(W[0]!) +
				p.ms.toFixed(1).padStart(W[1]!) +
				(isFinite(ops) ? ops.toFixed(0) : "N/A").padStart(W[2]!) +
				`${pct}%`.padStart(W[3]!) +
				"  " + bar,
		);
	}

	console.log("─".repeat(header.length));
	console.log("TOTAL".padEnd(W[0]!) + totalMs.toFixed(1).padStart(W[1]!));
	const totalOps = ENTRIES / (totalMs / 1000);
	console.log(`\nEffective throughput: ${totalOps.toFixed(0)} ops/sec`);

	console.log("\n  Top bottlenecks:");
	const sorted = [...phases].sort((a, b) => b.ms - a.ms);
	for (const p of sorted.slice(0, 3)) {
		const pct = ((p.ms / totalMs) * 100).toFixed(1);
		console.log(`    ${pct.padStart(5)}%  ${p.name}  (${p.ms.toFixed(0)}ms)`);
	}
}

main().catch(console.error);
