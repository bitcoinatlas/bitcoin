import { BytesCodec } from "@nomadshiba/codec";
import { Database } from "@db/sqlite";
import { DatabaseSync } from "node:sqlite";
import { createArrayStore } from "~/lib/storage/ArrayStore.ts";
import { createBlobStore } from "~/lib/storage/BlobStore.ts";
import { createKVStore } from "~/lib/storage/KVStore.ts";

// ─── KVStore constants ────────────────────────────────────────────────────────
const KEY_SIZE = 32;
const VALUE_SIZE = 128;
const KV_TOTAL = 1_000_000;
const KV_READ_SAMPLES = 10_000;
const KV_BATCH_SIZE = 100_000;

// ─── ArrayStore constants ─────────────────────────────────────────────────────
const ARRAY_ITEM_SIZE = 64;
const ARRAY_TOTAL = 1_000_000;
const ARRAY_READ_SAMPLES = 10_000;
const ARRAY_RANGE_SIZE = 100;
const ARRAY_BATCH_SIZE = 100_000;

// ─── BlobStore constants ──────────────────────────────────────────────────────
const BLOB_SIZE = 512;
const BLOB_TOTAL = 100_000;
const BLOB_READ_SAMPLES = 5_000;

// ─── Helpers ──────────────────────────────────────────────────────────────────

function randomBytes(size: number): Uint8Array {
	const buf = new Uint8Array(size);
	crypto.getRandomValues(buf);
	return buf;
}

async function dirSize(path: string): Promise<number> {
	let total = 0;
	for await (const entry of Deno.readDir(path)) {
		const entryPath = `${path}/${entry.name}`;
		if (entry.isFile) {
			total += (await Deno.stat(entryPath)).size;
		} else if (entry.isDirectory) {
			total += await dirSize(entryPath);
		}
	}
	return total;
}

// ─── KVStore ─────────────────────────────────────────────────────────────────

async function benchmarkKVStore(dir: string, keys: Uint8Array[], values: Uint8Array[]) {
	console.log("\n  KVStore");

	const store = await createKVStore({
		name: "bench_kv",
		path: `${dir}/bench_kv`,
		keyCodec: new BytesCodec({ size: KEY_SIZE }),
		valueCodec: new BytesCodec({ size: VALUE_SIZE }),
	});

	// Writes — batch → WAL save → WAL apply → WAL discard
	console.log("    Writing...");
	const writeStart = performance.now();
	for (let i = 0; i < KV_TOTAL; i += KV_BATCH_SIZE) {
		const end = Math.min(i + KV_BATCH_SIZE, KV_TOTAL);
		const batch = store.batch();
		for (let j = i; j < end; j++) batch.set(keys[j]!, values[j]!);
		batch.apply();
		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();

		const elapsed = (performance.now() - writeStart) / 1000;
		const rate = end / elapsed;
		console.log(
			`      ${(end / KV_TOTAL * 100).toFixed(1)}% - ${end.toLocaleString()} entries (${
				rate.toFixed(0)
			} ops/sec)`,
		);
	}
	const writeOps = KV_TOTAL / ((performance.now() - writeStart) / 1000);
	console.log(`      Total: ${writeOps.toFixed(0)} ops/sec`);

	// Reads (single)
	console.log("    Reading (single)...");
	const readStart = performance.now();
	for (let i = 0; i < KV_READ_SAMPLES; i++) await store.get(keys[i]!);
	const readOps = KV_READ_SAMPLES / ((performance.now() - readStart) / 1000);
	console.log(`      ${readOps.toFixed(0)} ops/sec`);

	// Reads (batch)
	console.log("    Reading (batch)...");
	const batchReadStart = performance.now();
	for (let i = 0; i < KV_READ_SAMPLES; i += 100) {
		await store.getMany(keys.slice(i, Math.min(i + 100, KV_READ_SAMPLES)));
	}
	const batchReadOps = KV_READ_SAMPLES / ((performance.now() - batchReadStart) / 1000);
	console.log(`      ${batchReadOps.toFixed(0)} ops/sec`);

	store.close();

	const fileSize = await dirSize(`${dir}/bench_kv`);
	console.log(`      File: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);

	return { name: "KVStore", writeOps, readOps, batchReadOps, fileSize };
}

// ─── SQLite ───────────────────────────────────────────────────────────────────

async function benchmarkSQLite(dir: string, keys: Uint8Array[], values: Uint8Array[]) {
	console.log("\n  SQLite (node:sqlite)");
	const db = new DatabaseSync(`${dir}/bench_sqlite.db`);
	db.exec(`PRAGMA journal_mode = WAL`);
	db.exec(`PRAGMA synchronous = NORMAL`);
	db.exec(`PRAGMA cache_size=-262144`);
	db.exec(`CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)`);

	// Writes
	console.log("    Writing...");
	const writeStart = performance.now();
	const insertStmt = db.prepare("INSERT INTO kv (key, value) VALUES (?, ?)");
	for (let i = 0; i < KV_TOTAL; i += KV_BATCH_SIZE) {
		const end = Math.min(i + KV_BATCH_SIZE, KV_TOTAL);
		db.exec("BEGIN");
		for (let j = i; j < end; j++) insertStmt.run(keys[j]!, values[j]!);
		db.exec("COMMIT");

		const elapsed = (performance.now() - writeStart) / 1000;
		const rate = end / elapsed;
		console.log(
			`      ${(end / KV_TOTAL * 100).toFixed(1)}% - ${end.toLocaleString()} entries (${
				rate.toFixed(0)
			} ops/sec)`,
		);
	}
	const writeOps = KV_TOTAL / ((performance.now() - writeStart) / 1000);
	console.log(`      Total: ${writeOps.toFixed(0)} ops/sec`);

	// Reads (single)
	console.log("    Reading (single)...");
	const readStart = performance.now();
	const selectStmt = db.prepare("SELECT value FROM kv WHERE key = ?");
	for (let i = 0; i < KV_READ_SAMPLES; i++) selectStmt.get(keys[i]!);
	const readOps = KV_READ_SAMPLES / ((performance.now() - readStart) / 1000);
	console.log(`      ${readOps.toFixed(0)} ops/sec`);

	// Reads (batch)
	console.log("    Reading (batch)...");
	const batchReadStart = performance.now();
	const placeholders = new Array(100).fill("?").join(",");
	const batchStmt = db.prepare(`SELECT value FROM kv WHERE key IN (${placeholders})`);
	for (let i = 0; i < KV_READ_SAMPLES; i += 100) {
		batchStmt.all(...keys.slice(i, Math.min(i + 100, KV_READ_SAMPLES)));
	}
	const batchReadOps = KV_READ_SAMPLES / ((performance.now() - batchReadStart) / 1000);
	console.log(`      ${batchReadOps.toFixed(0)} ops/sec`);

	db.close();

	const stat = await Deno.stat(`${dir}/bench_sqlite.db`);
	console.log(`      File: ${(stat.size / 1024 / 1024).toFixed(2)} MB`);

	return { name: "node:sqlite", writeOps, readOps, batchReadOps, fileSize: stat.size };
}

// ─── @db/sqlite ───

async function benchmarkDbSQLite(dir: string, keys: Uint8Array[], values: Uint8Array[]) {
	console.log("\n SQlite (@db/sqlite)");
	const db = new Database(`${dir}/bench_db_sqlite.db`);
	db.exec(`PRAGMA journal_mode = WAL`);
	db.exec(`PRAGMA synchronous = NORMAL`);
	db.exec(`PRAGMA cache_size=-262144`);
	db.exec(`CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)`);

	// Writes
	console.log("    Writing...");
	const writeStart = performance.now();
	const insertStmt = db.prepare("INSERT INTO kv (key, value) VALUES (?, ?)");
	for (let i = 0; i < KV_TOTAL; i += KV_BATCH_SIZE) {
		const end = Math.min(i + KV_BATCH_SIZE, KV_TOTAL);
		db.transaction(() => {
			for (let j = i; j < end; j++) insertStmt.run(keys[j]!, values[j]!);
		})();

		const elapsed = (performance.now() - writeStart) / 1000;
		const rate = end / elapsed;
		console.log(
			`      ${(end / KV_TOTAL * 100).toFixed(1)}% - ${end.toLocaleString()} entries (${
				rate.toFixed(0)
			} ops/sec)`,
		);
	}
	const writeOps = KV_TOTAL / ((performance.now() - writeStart) / 1000);
	console.log(`      Total: ${writeOps.toFixed(0)} ops/sec`);

	// Reads (single)
	console.log("    Reading (single)...");
	const readStart = performance.now();
	const selectStmt = db.prepare("SELECT value FROM kv WHERE key = ?");
	for (let i = 0; i < KV_READ_SAMPLES; i++) selectStmt.get(keys[i]!);
	const readOps = KV_READ_SAMPLES / ((performance.now() - readStart) / 1000);
	console.log(`      ${readOps.toFixed(0)} ops/sec`);

	// Reads (batch)
	console.log("    Reading (batch)...");
	const batchReadStart = performance.now();
	const placeholders = new Array(100).fill("?").join(",");
	const batchStmt = db.prepare(`SELECT value FROM kv WHERE key IN (${placeholders})`);
	for (let i = 0; i < KV_READ_SAMPLES; i += 100) {
		batchStmt.all(...keys.slice(i, Math.min(i + 100, KV_READ_SAMPLES)));
	}
	const batchReadOps = KV_READ_SAMPLES / ((performance.now() - batchReadStart) / 1000);
	console.log(`      ${batchReadOps.toFixed(0)} ops/sec`);

	db.close();

	const stat = await Deno.stat(`${dir}/bench_db_sqlite.db`);
	console.log(`      File: ${(stat.size / 1024 / 1024).toFixed(2)} MB`);

	return { name: "@db/sqlite", writeOps, readOps, batchReadOps, fileSize: stat.size };
}

// ─── @db/sqlite + WAL overhead (mirrors KVStore internals exactly) ────────────

async function benchmarkDbSQLiteWithWAL(dir: string, keys: Uint8Array[], values: Uint8Array[]) {
	console.log("\n  @db/sqlite + WAL");
	const dbPath = `${dir}/bench_db_sqlite_wal.db`;
	const walPath = `${dir}/bench_db_sqlite.wal`;
	const db = new Database(dbPath);
	db.exec(`PRAGMA journal_mode = DELETE`);
	db.exec(`PRAGMA synchronous = NORMAL`);
	db.exec(`PRAGMA cache_size=-262144`);
	db.exec(`CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)`);

	const KEY_SIZE = keys[0]!.length;
	const VALUE_SIZE = values[0]!.length;
	const entryStride = KEY_SIZE + VALUE_SIZE;
	const stmtInsert = db.prepare("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)");
	const txInsert = db.transaction((entryCount: number, buffer: Uint8Array) => {
		let pos = 0;
		for (let i = 0; i < entryCount; i++) {
			stmtInsert.run(buffer.subarray(pos, pos + KEY_SIZE), buffer.subarray(pos + KEY_SIZE, pos + entryStride));
			pos += entryStride;
		}
	});

	// Writes — serialize to WAL buffer, write file, txInsert, delete file
	console.log("    Writing...");
	const writeStart = performance.now();
	for (let i = 0; i < KV_TOTAL; i += KV_BATCH_SIZE) {
		const end = Math.min(i + KV_BATCH_SIZE, KV_TOTAL);
		const entryCount = end - i;

		// Serialize
		const buf = new Uint8Array(4 + entryCount * entryStride);
		new DataView(buf.buffer).setUint32(0, entryCount, true);
		let pos = 4;
		for (let j = i; j < end; j++) {
			buf.set(keys[j]!, pos);
			buf.set(values[j]!, pos + KEY_SIZE);
			pos += entryStride;
		}

		// WAL write
		await Deno.writeFile(walPath, buf, { create: true });

		// SQLite transaction
		txInsert(entryCount, buf.subarray(4));

		// WAL delete
		await Deno.remove(walPath);

		const elapsed = (performance.now() - writeStart) / 1000;
		const rate = end / elapsed;
		console.log(
			`      ${(end / KV_TOTAL * 100).toFixed(1)}% - ${end.toLocaleString()} entries (${
				rate.toFixed(0)
			} ops/sec)`,
		);
	}
	const writeOps = KV_TOTAL / ((performance.now() - writeStart) / 1000);
	console.log(`      Total: ${writeOps.toFixed(0)} ops/sec`);

	// Reads (single)
	console.log("    Reading (single)...");
	const readStart = performance.now();
	const selectStmt = db.prepare("SELECT value FROM kv WHERE key = ?");
	for (let i = 0; i < KV_READ_SAMPLES; i++) selectStmt.get(keys[i]!);
	const readOps = KV_READ_SAMPLES / ((performance.now() - readStart) / 1000);
	console.log(`      ${readOps.toFixed(0)} ops/sec`);

	// Reads (batch)
	console.log("    Reading (batch)...");
	const batchReadStart = performance.now();
	const placeholders = new Array(100).fill("?").join(",");
	const batchStmt = db.prepare(`SELECT value FROM kv WHERE key IN (${placeholders})`);
	for (let i = 0; i < KV_READ_SAMPLES; i += 100) {
		batchStmt.all(...keys.slice(i, Math.min(i + 100, KV_READ_SAMPLES)));
	}
	const batchReadOps = KV_READ_SAMPLES / ((performance.now() - batchReadStart) / 1000);
	console.log(`      ${batchReadOps.toFixed(0)} ops/sec`);

	db.close();

	const stat = await Deno.stat(dbPath);
	console.log(`      File: ${(stat.size / 1024 / 1024).toFixed(2)} MB`);

	return { name: "@db/sqlite+CustomWAL", writeOps, readOps, batchReadOps, fileSize: stat.size };
}

// ─── Deno KV ──────────────────────────────────────────────────────────────────

// Limits: max 1000 mutations per atomic, max 10 keys per getMany.
const DENO_KV_ATOMIC_LIMIT = 1000;
const DENO_KV_GETMANY_LIMIT = 10;

async function benchmarkDenoKV(dir: string, keys: Uint8Array[], values: Uint8Array[]) {
	console.log("\n  Deno KV");

	const kvPath = `${dir}/bench_deno_kv`;
	const kv = await Deno.openKv(kvPath);

	// Writes — atomic batches of 1000 (API limit)
	console.log("    Writing...");
	const writeStart = performance.now();
	for (let i = 0; i < KV_TOTAL; i += KV_BATCH_SIZE) {
		const end = Math.min(i + KV_BATCH_SIZE, KV_TOTAL);
		for (let j = i; j < end; j += DENO_KV_ATOMIC_LIMIT) {
			const batchEnd = Math.min(j + DENO_KV_ATOMIC_LIMIT, end);
			const atomic = kv.atomic();
			for (let k = j; k < batchEnd; k++) atomic.set([keys[k]!], values[k]!);
			await atomic.commit();
		}
		const elapsed = (performance.now() - writeStart) / 1000;
		const rate = end / elapsed;
		console.log(
			`      ${(end / KV_TOTAL * 100).toFixed(1)}% - ${end.toLocaleString()} entries (${
				rate.toFixed(0)
			} ops/sec)`,
		);
	}
	const writeOps = KV_TOTAL / ((performance.now() - writeStart) / 1000);
	console.log(`      Total: ${writeOps.toFixed(0)} ops/sec`);

	// Reads (single)
	console.log("    Reading (single)...");
	const readStart = performance.now();
	for (let i = 0; i < KV_READ_SAMPLES; i++) await kv.get([keys[i]!]);
	const readOps = KV_READ_SAMPLES / ((performance.now() - readStart) / 1000);
	console.log(`      ${readOps.toFixed(0)} ops/sec`);

	// Reads (batch) — getMany limit is 10 keys
	console.log(`    Reading (batch/${DENO_KV_GETMANY_LIMIT})...`);
	const batchReadStart = performance.now();
	for (let i = 0; i < KV_READ_SAMPLES; i += DENO_KV_GETMANY_LIMIT) {
		const end = Math.min(i + DENO_KV_GETMANY_LIMIT, KV_READ_SAMPLES);
		await kv.getMany(keys.slice(i, end).map((k) => [k]));
	}
	const batchReadOps = KV_READ_SAMPLES / ((performance.now() - batchReadStart) / 1000);
	console.log(`      ${batchReadOps.toFixed(0)} ops/sec`);

	kv.close();

	let fileSize = 0;
	for (const suffix of ["", "-wal", "-shm"]) {
		fileSize += await Deno.stat(`${kvPath}${suffix}`).then((s) => s.size).catch(() => 0);
	}
	console.log(`      File: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);

	return { name: "Deno KV", writeOps, readOps, batchReadOps, fileSize };
}

// ─── ArrayStore ───────────────────────────────────────────────────────────────

async function benchmarkArrayStore(dir: string, items: Uint8Array[]) {
	console.log("\n  ArrayStore");

	const store = await createArrayStore({
		name: "bench_array",
		path: `${dir}/bench_array`,
		codec: new BytesCodec({ size: ARRAY_ITEM_SIZE }),
	});

	// Writes
	console.log("    Writing...");
	const writeStart = performance.now();
	for (let i = 0; i < ARRAY_TOTAL; i += ARRAY_BATCH_SIZE) {
		const end = Math.min(i + ARRAY_BATCH_SIZE, ARRAY_TOTAL);
		const batch = store.batch();
		for (let j = i; j < end; j++) batch.push(items[j]!);
		batch.apply();
		const wal = await store.createWAL();
		await wal.apply();
		await wal.discard();

		const elapsed = (performance.now() - writeStart) / 1000;
		const rate = end / elapsed;
		console.log(
			`      ${(end / ARRAY_TOTAL * 100).toFixed(1)}% - ${end.toLocaleString()} items (${
				rate.toFixed(0)
			} ops/sec)`,
		);
	}
	const writeOps = ARRAY_TOTAL / ((performance.now() - writeStart) / 1000);
	console.log(`      Total: ${writeOps.toFixed(0)} ops/sec`);

	// Reads (single)
	console.log("    Reading (single)...");
	const readStart = performance.now();
	for (let i = 0; i < ARRAY_READ_SAMPLES; i++) await store.get(i);
	const readOps = ARRAY_READ_SAMPLES / ((performance.now() - readStart) / 1000);
	console.log(`      ${readOps.toFixed(0)} ops/sec`);

	// Reads (sequential range)
	console.log(`    Reading (range/${ARRAY_RANGE_SIZE})...`);
	const rangeStart = performance.now();
	for (let i = 0; i < ARRAY_READ_SAMPLES; i += ARRAY_RANGE_SIZE) {
		await store.slice(i, Math.min(ARRAY_RANGE_SIZE, ARRAY_READ_SAMPLES - i));
	}
	const rangeOps = ARRAY_READ_SAMPLES / ((performance.now() - rangeStart) / 1000);
	console.log(`      ${rangeOps.toFixed(0)} ops/sec`);

	store.close();

	const fileSize = await dirSize(`${dir}/bench_array`);
	console.log(`      File: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);

	return { name: "ArrayStore", writeOps, readOps, rangeOps, fileSize };
}

// ─── BlobStore ────────────────────────────────────────────────────────────────

async function benchmarkBlobStore(dir: string, blobs: Uint8Array[]) {
	console.log("\n  BlobStore");

	const store = await createBlobStore({
		name: "bench_blob",
		path: `${dir}/bench_blob`,
	});

	// Writes — one batch for all blobs, record pointers
	console.log("    Writing...");
	const pointers: number[] = [];
	const writeStart = performance.now();
	const batch = store.batch();
	for (let i = 0; i < BLOB_TOTAL; i++) pointers.push(batch.append(blobs[i]!));
	batch.apply();
	const wal = await store.createWAL();
	await wal.apply();
	await wal.discard();
	const writeElapsed = (performance.now() - writeStart) / 1000;
	const writeOps = BLOB_TOTAL / writeElapsed;
	const writeMBps = (BLOB_TOTAL * BLOB_SIZE) / 1024 / 1024 / writeElapsed;
	console.log(`      Total: ${writeOps.toFixed(0)} ops/sec  (${writeMBps.toFixed(1)} MB/s)`);

	// Reads (single)
	console.log("    Reading (single)...");
	const readStart = performance.now();
	for (let i = 0; i < BLOB_READ_SAMPLES; i++) await store.get(pointers[i]!, BLOB_SIZE);
	const readElapsed = (performance.now() - readStart) / 1000;
	const readOps = BLOB_READ_SAMPLES / readElapsed;
	const readMBps = (BLOB_READ_SAMPLES * BLOB_SIZE) / 1024 / 1024 / readElapsed;
	console.log(`      ${readOps.toFixed(0)} ops/sec  (${readMBps.toFixed(1)} MB/s)`);

	let fileSize = 0;
	for await (const e of Deno.readDir(`${dir}/bench_blob`)) {
		if (e.isFile && e.name.startsWith("chunk_")) {
			fileSize += (await Deno.stat(`${dir}/bench_blob/${e.name}`)).size;
		}
	}
	console.log(`      File: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);

	return { name: "BlobStore", writeOps, writeMBps, readOps, readMBps, fileSize };
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
	const dir = await Deno.makeTempDir({ prefix: "benchmark_" });
	try {
		// ── KV benchmarks ──────────────────────────────────────────────────────────
		console.log(`KV Store Benchmark`);
		console.log(`  Entries: ${KV_TOTAL.toLocaleString()}, Key: ${KEY_SIZE}B, Value: ${VALUE_SIZE}B`);
		console.log(`\n  Generating KV test data...`);
		const keys: Uint8Array[] = [];
		const values: Uint8Array[] = [];
		for (let i = 0; i < KV_TOTAL; i++) {
			keys.push(randomBytes(KEY_SIZE));
			values.push(randomBytes(VALUE_SIZE));
		}

		const kvResults = [
			await benchmarkKVStore(dir, keys, values),
			await benchmarkSQLite(dir, keys, values),
			await benchmarkDbSQLite(dir, keys, values),
			await benchmarkDbSQLiteWithWAL(dir, keys, values),
			// await benchmarkDenoKV(dir, keys, values), // too slow, skipped
		];

		console.log("\n" + "=".repeat(62));
		console.log("KV RESULTS");
		console.log("=".repeat(62));
		console.log("Store              Writes      Single    Batch(10/100)      Size");
		console.log("-".repeat(62));
		for (const r of kvResults) {
			console.log(
				`${r.name.padEnd(17)} ${r.writeOps.toFixed(0).padStart(9)} ${r.readOps.toFixed(0).padStart(11)} ${
					r.batchReadOps.toFixed(0).padStart(11)
				} ${(r.fileSize / 1024 / 1024).toFixed(1).padStart(6)}MB`,
			);
		}

		// ── ArrayStore benchmark ───────────────────────────────────────────────────
		console.log(`\nArrayStore Benchmark`);
		console.log(`  Items: ${ARRAY_TOTAL.toLocaleString()}, Item size: ${ARRAY_ITEM_SIZE}B`);
		console.log(`\n  Generating array test data...`);
		const arrayItems: Uint8Array[] = [];
		for (let i = 0; i < ARRAY_TOTAL; i++) arrayItems.push(randomBytes(ARRAY_ITEM_SIZE));

		const arrayResult = await benchmarkArrayStore(dir, arrayItems);

		console.log("\n" + "=".repeat(62));
		console.log("ARRAY RESULTS");
		console.log("=".repeat(62));
		console.log("Store              Writes      Single       Range      Size");
		console.log("-".repeat(62));
		console.log(
			`${arrayResult.name.padEnd(17)} ${arrayResult.writeOps.toFixed(0).padStart(9)} ${
				arrayResult.readOps.toFixed(0).padStart(11)
			} ${arrayResult.rangeOps.toFixed(0).padStart(11)} ${
				(arrayResult.fileSize / 1024 / 1024).toFixed(1).padStart(6)
			}MB`,
		);

		// ── BlobStore benchmark ────────────────────────────────────────────────────
		console.log(`\nBlobStore Benchmark`);
		console.log(`  Blobs: ${BLOB_TOTAL.toLocaleString()}, Blob size: ${BLOB_SIZE}B`);
		console.log(`\n  Generating blob test data...`);
		const blobItems: Uint8Array[] = [];
		for (let i = 0; i < BLOB_TOTAL; i++) blobItems.push(randomBytes(BLOB_SIZE));

		const blobResult = await benchmarkBlobStore(dir, blobItems);

		console.log("\n" + "=".repeat(65));
		console.log("BLOB RESULTS");
		console.log("=".repeat(65));
		console.log("Store              Writes    Write MB/s    Single  Read MB/s  Size");
		console.log("-".repeat(65));
		console.log(
			`${blobResult.name.padEnd(17)} ${blobResult.writeOps.toFixed(0).padStart(9)} ${
				blobResult.writeMBps.toFixed(1).padStart(11)
			} ${blobResult.readOps.toFixed(0).padStart(9)} ${blobResult.readMBps.toFixed(1).padStart(9)} ${
				(blobResult.fileSize / 1024 / 1024).toFixed(1).padStart(6)
			}MB`,
		);
	} finally {
		await Deno.remove(dir, { recursive: true });
	}
}

main().catch(console.error);
