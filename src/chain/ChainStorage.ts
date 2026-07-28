import { StructCodec, VarInt } from "@nomadshiba/codec";
import { join } from "@std/path";
import { Block } from "~/app/routes.ts";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U48 } from "~/codec/primitives/U48.ts";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";
import { StoredPubkeyPointer } from "~/codec/stored/StoredPubkeyPointer.ts";
import { StoredScriptPubKey } from "~/codec/stored/StoredScriptPubkey.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxPointer } from "~/codec/stored/StoredTxPointer.ts";
import { StoredTxs } from "~/codec/stored/StoredTxs.ts";
import { COINBASE_TXID, GB, MINUTE } from "~/constants.ts";
import { BASE_DATA_DIR } from "~/env.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";
import { ArrayStore } from "~/libs/storage/ArrayStore.ts";
import { Atomic } from "~/libs/storage/Atomic.ts";
import { BlobStore, CompressionOptions } from "~/libs/storage/BlobStore.ts";
import { HashMapStore } from "~/libs/storage/HashMapStore.ts";

const COMPRESSION_OPTIONS: CompressionOptions = {
	maxInflatedChunkAge: 15 * MINUTE,
	maxInflatedChunks: 8,
	zstd: {
		compress: {
			compressionLevel: 19,
			enableLongDistanceMatching: 1,
			windowLog: 27, // maybe make it 24 later?
			checksumFlag: 1, // 4-byte frame checksum, cheap integrity guard
			contentSizeFlag: 1, // size in frame header — works on the sync path,
		},
		decompress: {
			windowLogMax: 27,
		},
	},
};

export class ChainStorage {
	public readonly atomic = Atomic.open({
		path: join(BASE_DATA_DIR, "meta"),
		stores: {
			headers: ArrayStore.open({
				path: join(BASE_DATA_DIR, "headers"),
				item: StoredBlockHeader,
				itemsPerChunk: 1_000_000,
				writable: self.name === "chain",
			}),
			blocks: ArrayStore.open({
				path: join(BASE_DATA_DIR, "blocks"),
				item: StoredTxPointer,
				itemsPerChunk: 1_000_000,
				writable: self.name === "chain",
			}),
			txs: HashMapStore.open({
				path: join(BASE_DATA_DIR, "txs"),
				key: Bytes32,
				value: StoredTx,
				pointer: StoredTxPointer,
				buckets: {
					itemsPerChunk: 1_000_000,
				},
				entries: {
					maxChunkSize: 1 * GB,
					compression: COMPRESSION_OPTIONS,
				},
				targetRatio: .7,
				maxRatioDrift: .15,
				writable: self.name === "chain",
			}),
			pubkeys: HashMapStore.open({
				path: join(BASE_DATA_DIR, "pubkeys"),
				key: Bytes32,
				value: StoredScriptPubKey, // TODO: value might also include HEAD and TAIL of the linked list of outputs or something
				pointer: StoredPubkeyPointer,
				buckets: {
					itemsPerChunk: 1_000_000,
				},
				entries: {
					maxChunkSize: 1 * GB,
				},
				targetRatio: .7,
				maxRatioDrift: .15,
				writable: self.name === "chain",
			}),
			spenders: HashMapStore.open({
				path: join(BASE_DATA_DIR, "spenders"),
				key: new StructCodec({ tx: StoredTxPointer, output: VarInt }),
				value: StoredTxPointer, // spender tx
				pointer: U48,
				buckets: {
					itemsPerChunk: 1_000_000,
				},
				entries: {
					maxChunkSize: 1 * GB,
				},
				targetRatio: .7,
				maxRatioDrift: .15,
				writable: self.name === "chain",
			}),
		},
	});
	public readonly stores = this.atomic.stores;
	private readonly hashIndex: FastUint8ArrayMap<number>;
	private readonly indexHeightByHashChannel: BroadcastChannel;

	private constructor() {
		const length = this.stores.headers.size();
		const channel = new BroadcastChannel("ChainStore.indexHeightByHash");
		const index = new FastUint8ArrayMap<number>(Math.max(256, length));

		const STEP = 100_000;
		for (let from = 0; from < length; from += STEP) {
			const to = Math.min(length, from + STEP);
			const headers = this.stores.headers.slice(from, to);
			// TODO: this takes so much time in every instance we do this.
			// TODO: instead add a lazy memcopy option to ArrayStore, where it caches once read from disk.
			// TODO: so we dont load everything from disk at once. and also dont have to think about memory abstraction in places.
			// TODO: i mean both in memory headers and KvStore hash index. both should be on disk, and have lazy memcache option.
			// TODO: tho i believe rocks already has cache, but it has to pass ffi, this way its cached in v8
			for (let i = 0; i < headers.length; i++) {
				index.set(headers[i]!.hash(), from + i);
			}
		}

		channel.addEventListener("message", (event) => {
			const { hash, height } = event.data;
			index.set(hash, height);
		});

		this.indexHeightByHashChannel = channel;
		this.hashIndex = index;
	}

	private static main_: ChainStorage;
	public static main() {
		return this.main_ ??= new ChainStorage();
	}

	public getHeightByHash(hash: Uint8Array): number | undefined {
		return this.hashIndex.get(hash);
	}

	public indexHeightByHash(hash: Uint8Array, height: number) {
		this.hashIndex.set(hash, height);
		this.indexHeightByHashChannel.postMessage({ hash, height });
	}

	public getHeaderByHeight(height: number): StoredBlockHeader | undefined {
		if (height < 0 || height >= this.stores.headers.size()) return undefined;
		return this.stores.headers.get(height);
	}

	public async getHeaderByHeightAsync(height: number): Promise<Block | undefined> {
		if (height < 0 || height >= this.stores.headers.size()) return undefined;
		const header = await this.stores.headers.getAsync(height);
		if (!header) return undefined;
		const [current, next] = await Promise.all([
			this.stores.blocks.getAsync(height).then((n) => n ?? 0),
			this.stores.blocks.getAsync(height + 1).then((n) => n ?? this.stores.txs.size()),
		]);
		const size = next - current;
		return { height, header, size };
	}

	public getHeaderByRange(from: number, to: number): Array<Block> {
		const length = this.stores.headers.size();
		from = Math.max(0, from);
		to = Math.min(length - 1, to);
		if (to < from) return [];
		const headers = this.stores.headers.slice(from, to + 1);
		let next = this.stores.blocks.get(from) ?? 0;
		return headers.map((header, i) => {
			const height = from + i;
			const current = next;
			// TODO: This excludes the pubkeys and header, later store wire size on the stored blocks. shouldnt take much space anyway, because its per block
			next = this.stores.blocks.get(height + 1) ?? this.stores.txs.size();
			const size = next - current;
			return { height, header, size };
		});
	}

	public async getHeaderByRangeAsync(from: number, to: number): Promise<Array<Block>> {
		const length = this.stores.headers.size();
		const lo = Math.max(0, from);
		const hi = Math.min(length - 1, to);
		if (hi < lo) return [];
		const headers = await this.stores.headers.sliceAsync(lo, hi + 1);
		// Fetch every block pointer in [lo, hi+1] up front in parallel. The sync
		// version threads `next` through the loop, but doing that with `await`
		// inside Promise.all races the shared variable — each closure reads the
		// same initial `next` as its `current` before any reassignment lands, so
		// every size ends up measured from the `lo` baseline. Pulling them all
		// into an array first keeps the reads parallel and the sizes correct.
		const pointers = await Promise.all(
			Array.from(
				{ length: headers.length + 1 },
				(_, i) => this.stores.blocks.getAsync(lo + i).then((p) => p ?? (i === 0 ? 0 : this.stores.txs.size())),
			),
		);
		return headers.map((header, i) => {
			const height = lo + i;
			const current = pointers[i]!;
			const next = pointers[i + 1]!;
			const size = next - current;
			return { height, header, size };
		});
	}

	public getChainTip(): { height: number; header: StoredBlockHeader } | undefined {
		const height = this.hashIndex.size() - 1;
		const header = this.getHeaderByHeight(height);
		if (!header) return undefined;
		return { height, header };
	}

	public async getChainTipAsync(): Promise<Block | undefined> {
		const height = this.hashIndex.size() - 1;
		if (height < 0) return undefined;
		const header = await this.getHeaderByHeightAsync(height);

		return header;
	}

	public getTxsByBlockHeight(height: number): StoredTx[] | undefined {
		const pointer = height === 0 ? 0 : this.stores.blocks.get(height);
		if (pointer === undefined) return undefined;
		return this.stores.txs.get(pointer, StoredTxs);
	}

	public async getTxsByBlockHeightAsync(height: number): Promise<StoredTx[] | undefined> {
		const pointer = height === 0 ? 0 : await this.stores.blocks.getAsync(height);
		if (pointer === undefined) return undefined;
		return this.stores.txs.getAsync(pointer, StoredTxs);
	}

	public getTxById(txId: Uint8Array): StoredTx | undefined {
		const pointer = this.stores.txid.get(txId);
		if (pointer === undefined) return undefined;
		return this.stores.txs.get(pointer, StoredTx);
	}

	public async getTxByIdAsync(txId: Uint8Array): Promise<StoredTx | undefined> {
		const pointer = await this.stores.txid.getAsync(txId);
		if (pointer === undefined) return undefined;
		return this.stores.txs.getAsync(pointer, StoredTx);
	}

	public getTxByPointer(pointer: StoredTxPointer): StoredTx {
		return this.stores.txs.get(pointer, StoredTx);
	}

	public async getTxByPointerAsync(pointer: StoredTxPointer): Promise<StoredTx> {
		return this.stores.txs.getAsync(pointer, StoredTx);
	}

	public getPrevOutTxId(input: StoredTxInput): Uint8Array<ArrayBuffer> {
		const { kind, value } = input.prevOut.txId;
		if (kind === "pointer") return this.getTxByPointer(value).txId;
		if (kind === "coinbase") return COINBASE_TXID;
		throw new Error(`getPrevOutTxId doesn't handle txId kind: ${kind satisfies never}`);
	}

	public async getPrevOutTxIdAsync(input: StoredTxInput): Promise<Uint8Array<ArrayBuffer>> {
		const { kind, value } = input.prevOut.txId;
		if (kind === "pointer") return (await this.getTxByPointerAsync(value)).txId;
		if (kind === "coinbase") return COINBASE_TXID;
		throw new Error(`getPrevOutTxId doesn't handle txId kind: ${kind satisfies never}`);
	}

	public getScriptPubKeyFromPointer(pointer: number): StoredScriptPubKey {
		return this.stores.pubkeys.get(pointer, StoredScriptPubKey);
	}

	public async getScriptPubKeyFromPointerAsync(pointer: number): Promise<StoredScriptPubKey> {
		return this.stores.pubkeys.getAsync(pointer, StoredScriptPubKey);
	}
}
export const chainStorage = ChainStorage.main();
