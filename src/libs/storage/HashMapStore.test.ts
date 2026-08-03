import { assertEquals } from "@std/assert";
import { join } from "@std/path";
import { StructCodec, U64, U32 } from "@nomadshiba/codec";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { HashMapStore, HashMapStoreOptions } from "~/libs/storage/HashMapStore.ts";

const recordCodec = new StructCodec({ previous: U64, key: Bytes32, value: U32 });

function tmpDir(): string {
	return Deno.makeTempDirSync({ prefix: "hashmap_test_" });
}

function key32(n: number): Uint8Array {
	const b = new Uint8Array(32);
	new DataView(b.buffer).setUint32(0, n);
	new DataView(b.buffer).setUint32(28, n * 2654435761 >>> 0);
	return b;
}

type Options = HashMapStoreOptions<typeof Bytes32, typeof U32>;
function options(path: string, overrides?: Partial<Options>): Options {
	return {
		path,
		key: Bytes32,
		value: U32,
		maxEntrySize: 32 + 4, // key + value
		loadFactor: { target: 4, maxDrift: 0.5 },
		writable: true,
		bucketCount: 16,
		blob: { path: join(path, "entries"), chunkSize: 1 << 16 },
		...overrides,
	};
}

/** Simulate a worker: mmap + encode record + commit, then the app persists. */
function write(map: HashMapStore<typeof Bytes32, typeof U32>, key: Uint8Array, value: number): void {
	const offset = map.next(32 + 4);
	const view = map.mmap();
	const record = recordCodec.encode({ previous: 0, key, value }); // previous patched by persist()
	view.set(record, 0);
	map.commit(offset + record.length);
}

Deno.test("mmap/commit/persist roundtrip", () => {
	const w = HashMapStore.open(options(tmpDir()));
	for (let i = 0; i < 50; i++) write(w, key32(i), i * 7);
	w.persist(w.size());
	for (let i = 0; i < 50; i++) assertEquals(w.get(key32(i)), i * 7);
	assertEquals(w.get(key32(1000)), undefined);
	w.close();
});

Deno.test("reader sees exactly what was persisted", () => {
	const dir = tmpDir();
	const w = HashMapStore.open(options(dir));
	for (let i = 0; i < 50; i++) write(w, key32(i), i * 7);
	const committedSize = w.size();
	w.persist(committedSize);
	w.close();

	const r = HashMapStore.open(options(dir, { writable: false }));
	r.reveal(committedSize);
	for (let i = 0; i < 50; i++) assertEquals(r.get(key32(i)), i * 7, `reader ${i}`);
	assertEquals(r.get(key32(1000)), undefined);
	r.close();
});

Deno.test("duplicate keys: newest wins via chain", () => {
	const w = HashMapStore.open(options(tmpDir(), { bucketCount: 2 }));
	write(w, key32(5), 111);
	w.persist(w.size());
	const firstPointer = w.getPointer(key32(5))!;
	write(w, key32(5), 222);
	w.persist(w.size());
	assertEquals(w.get(key32(5)), 222); // newest wins
	assertEquals(w.getPointer(key32(5)) !== firstPointer, true); // distinct entries
	w.close();
});

Deno.test("getPointer/getValueAndPointer", () => {
	const w = HashMapStore.open(options(tmpDir()));
	write(w, key32(3), 33);
	w.persist(w.size());
	const pointer = w.getPointer(key32(3))!;
	const [value, returnedPointer] = w.getValueAndPointer(key32(3))!;
	assertEquals(value, 33);
	assertEquals(returnedPointer, pointer);
	w.close();
});

Deno.test("truncate undoes persisted tail, survivors findable", () => {
	const path = tmpDir();
	const w = HashMapStore.open(options(path));
	for (let i = 0; i < 30; i++) write(w, key32(i), i);
	w.persist(w.size());
	const cutPoint = w.size();
	for (let i = 30; i < 50; i++) write(w, key32(i), i);
	w.persist(w.size());

	w.truncate(cutPoint);

	for (let i = 0; i < 30; i++) assertEquals(w.get(key32(i)), i, `survivor ${i}`);
	for (let i = 30; i < 50; i++) assertEquals(w.get(key32(i)), undefined, `dropped ${i}`);
	write(w, key32(30), 12345);
	w.persist(w.size());
	assertEquals(w.get(key32(30)), 12345);
	w.close();
});

Deno.test("truncate(0) drops everything", () => {
	const w = HashMapStore.open(options(tmpDir()));
	for (let i = 0; i < 40; i++) write(w, key32(i), i);
	w.persist(w.size());
	w.truncate(0);
	assertEquals(w.size(), 0);
	for (let i = 0; i < 40; i++) assertEquals(w.get(key32(i)), undefined);
	w.close();
});
