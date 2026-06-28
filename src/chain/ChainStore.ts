import { RocksDatabase, RocksDatabaseOptions } from "@harperfast/rocksdb-js";
import { sha256 } from "@noble/hashes/sha2";
import { delay } from "@std/async";
import { join } from "@std/path";
import { formatHash } from "~/app/frontend/utils/format.ts";
import { rawScriptPubKey, ScriptPubKey } from "~/chain/ScriptPubKey.ts";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U40 } from "~/codec/primitives/U40.ts";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";
import { StoredPointer } from "~/codec/stored/StoredPointer.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/codec/stored/StoredTxOutput.ts";
import { StoredTxs } from "~/codec/stored/StoredTxs.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";
import { COINBASE_TXID, MAX_BLOCK_SIZE, MAX_BLOCK_WEIGHT, MAX_NON_WITNESS_BLOCK_SIZE } from "~/constants.ts";
import { BASE_DATA_DIR } from "~/env.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";
import { Queue } from "~/libs/collections/Queue.ts";
import { Uint8ArrayMap } from "~/libs/collections/Uint8ArrayMap.ts";
import { formatDuration } from "~/libs/formatting/mod.ts";
import { ArrayStore } from "~/libs/storage/ArrayStore.ts";
import { Atomic, InferStores } from "~/libs/storage/Atomic.ts";
import { BlobStore } from "~/libs/storage/BlobStore.ts";
import { KvStore } from "~/libs/storage/KvStore.ts";

const GiB = 1024 ** 3;
const totalRam = Deno.systemMemoryInfo().total;

// Reserve for OS + page cache + V8 + other processes. Scale the reserve with
// total RAM (a 64 GiB box can spare proportionally more than an 8 GiB one).
const osReserve = Math.max(2 * GiB, totalRam * 0.25);

const writeBufferSize = 2 * GiB; // additive (costToCache:false)

// Block cache gets a slice of what remains, clamped to a sane band.
const available = totalRam - osReserve - writeBufferSize;
const blockCacheSize = Math.max(
	1 * GiB, // floor — below this rocksdb thrashes
	Math.min(available * 0.6, 32 * GiB), // 60% of remainder, hard ceiling
);

export class ChainStore {
	public readonly blockHashToHeightMap: Uint8ArrayMap<number>;
	public readonly atomic = atomic;

	private p2pChannel: MessagePort;
	private p2pMessageQueue: Queue<{ type: string; data: any }>;

	private constructor(p2pChannel: MessagePort, initialHeaders: WireBlockHeader[]) {
		this.p2pChannel = p2pChannel;
		this.p2pMessageQueue = new Queue(1000);
		this.blockHashToHeightMap = new Uint8ArrayMap<number>(Math.max(256, initialHeaders.length * 2));
		for (let i = 0; i < initialHeaders.length; i++) {
			this.blockHashToHeightMap.set(initialHeaders[i]!.hash(), i);
		}
	}

	static start(p2pChannel: MessagePort): ChainStore {
		const headers = atomic.stores.header.slice(0, atomic.stores.header.length());
		console.log(`[chain] loaded ${headers.length} headers from disk`);
		const self = new ChainStore(p2pChannel, headers);

		p2pChannel.addEventListener("message", (event) => self.p2pMessageQueue.enqueue(event.data));
		const startHeaders = atomic.stores.header.slice(0, atomic.stores.header.length());
		console.log(`[chain] handing ${startHeaders.length} headers to worker`);
		const startData = WireBlockHeaders.encode(startHeaders);
		p2pChannel.postMessage({ type: "seek", data: atomic.stores.block.length() - 1 });
		p2pChannel.postMessage({ type: "start", data: startData }, [startData.buffer]);
		return self;
	}

	private startTime: number | undefined;
	private totalTxs: number = 0;
	private totalBlocks: number = 0;
	private totalSize: number = 0;
	async tick(): Promise<void> {
		const message = this.p2pMessageQueue.dequeue();
		if (!message) {
			await delay(0);
			return;
		}
		if (message.type === "blocks") {
			if (this.startTime) {
				const passed = performance.now() - this.startTime;
				const passedSeconds = passed / 1000;
				const speedTxs = this.totalTxs / passedSeconds;
				const speedSize = (this.totalSize / 1024 / 1024) / passedSeconds;
				const speedBlocks = this.totalBlocks / passedSeconds;
				console.log(
					`[chain] sustained speed`,
					`${speedBlocks.toFixed(1)}blocks/s`,
					`${speedTxs.toFixed(0)}txs/s`,
					`${speedSize.toFixed(2)}MiB/s`,
					`time=${formatDuration(passed)}`,
				);
			}
			this.startTime ??= performance.now();
			const buffer = message.data as Uint8Array;
			console.log(`[chain] new chunk to consume size=${buffer.length}`);
			let offset = 0;
			let blocks = 0;
			while (offset < buffer.length) {
				const [txs, size] = StoredTxs.decode(buffer.subarray(offset));
				offset += size;
				blocks++;
				this.appendTxs(txs, atomic.stores.block.length());
				if (this.startTime) {
					this.totalTxs += txs.length;
					this.totalSize += size;
					this.totalBlocks++;
				}
			}
			this.p2pChannel.postMessage({ type: "consume" });
			console.log(`[chain] consumed blocks count=${blocks} bytes=${offset} height=${atomic.stores.block.length() - 1}`);

			return;
		}

		if (message.type === "headers") {
			const headers = WireBlockHeaders.decodeValue(message.data);
			const { height } = this.pushHeaders(headers);
			console.log(`[chain] tick headers height=${height} count=${headers.length}`);
			return;
		}

		if (message.type === "reorg") {
			console.log(`[chain] tick reorg keepHeight=${message.data}`);
			this.reorg(message.data);
			return;
		}
	}

	private pushHeaders(headers: WireBlockHeader[]): { height: number } {
		try {
			atomic.pin();
			let height = atomic.stores.header.length();
			for (const header of headers) {
				height = atomic.stores.header.push(header);
				this.blockHashToHeightMap.set(header.hash(), height);
			}
			atomic.commit();
			return { height };
		} catch (reason) {
			console.error("Failed to append block header:", reason);
			Deno.exit(1);
		}
	}

	private reorg(keepHeight: number): void {
		const blocksHeight = atomic.stores.block.length() - 1;
		console.log(`[chain] reorg: keepHeight=${keepHeight} currentTip=${blocksHeight}`);
		if (keepHeight >= blocksHeight) return; // nothing to undo

		// byte offset in the tx blob where the orphaned suffix begins
		const cutOffset = atomic.stores.block.get(keepHeight + 1);
		if (cutOffset === undefined) throw new Error(`reorg: no block pointer at ${keepHeight + 1}`);

		// spender array cut = the spender base of the first tx of the first orphaned block
		const firstOrphanTxs = this.getTxsByBlockHeight(keepHeight + 1);
		if (!firstOrphanTxs?.length) throw new Error(`reorg: no txs at ${keepHeight + 1}`);
		const spenderCut = firstOrphanTxs[0]!.spender;
		console.log(`[chain] reorg: cutOffset=${cutOffset} spenderCut=${spenderCut}`);

		const batch = atomic.batch();

		// tombstone orphaned txid / pubkey entries (pointer >= cutOffset only)
		for (let h = keepHeight + 1; h <= blocksHeight; h++) {
			const txs = this.getTxsByBlockHeight(h);
			if (!txs) continue;
			for (const tx of txs) {
				const existingTxid = batch.txid.get(tx.txId);
				if (existingTxid !== undefined && existingTxid >= cutOffset) {
					batch.txid.delete(tx.txId); // assumed KvStore.delete
				}
				for (const output of tx.outputs) {
					if (output.scriptPubKey.kind === "pointer") continue;
					const hash = sha256(rawScriptPubKey(output.scriptPubKey, this._rawScriptPubKeyBuffer));
					const existingPub = batch.pubkey.get(hash);
					if (existingPub !== undefined && existingPub >= cutOffset) {
						batch.pubkey.delete(hash); // assumed KvStore.delete
					}
				}
			}
			const header = atomic.stores.header.get(h);
			if (header) this.blockHashToHeightMap.delete(header.hash());
		}

		batch.txid.apply();
		batch.pubkey.apply();

		// truncate the array/blob stores down to the surviving prefix
		atomic.stores.header.truncate(keepHeight + 1);
		atomic.stores.block.truncate(keepHeight + 1);
		atomic.stores.spender.truncate(spenderCut);
		atomic.stores.tx.truncate(cutOffset);
		console.log(`[chain] reorg complete: stores truncated to height=${keepHeight}`);
	}

	getHeaderByHeight(height: number): StoredBlockHeader | undefined {
		const header = atomic.stores.header.get(height);
		if (!header) return undefined;
		return header;
	}

	getHeaderByRange(
		from: number,
		to: number,
	): Array<{ height: number; header: WireBlockHeader }> {
		const headers = atomic.stores.header.slice(from, to + 1);
		return headers.map((header, i) => ({ height: from + i, header: header }));
	}

	getHeaderByHash(hash: Uint8Array): StoredBlockHeader | undefined {
		const height = this.blockHashToHeightMap.get(hash);
		if (height === undefined) return undefined;
		return this.getHeaderByHeight(height);
	}

	getTxByPointer(pointer: StoredPointer): StoredTx {
		const storedTx = atomic.stores.tx.get(pointer, StoredTx, { readAheadSize: 400_000 });
		return storedTx;
	}

	getTxById(txId: Uint8Array): StoredTx | undefined {
		const pointer = atomic.stores.txid.get(txId);
		if (pointer === undefined) return undefined;
		return this.getTxByPointer(pointer);
	}

	getTxsByBlockPointer(pointer: StoredPointer): StoredTx[] | undefined {
		const storedTxs = atomic.stores.tx.get(pointer, StoredTxs, { readAheadSize: MAX_BLOCK_WEIGHT });
		return storedTxs;
	}

	getTxsByBlockHeight(height: number): StoredTx[] | undefined {
		const pointer = height === 0 ? 0 : atomic.stores.block.get(height);
		if (pointer === undefined) return undefined;
		return this.getTxsByBlockPointer(pointer);
	}

	getTxsByBlockHash(hash: Uint8Array): StoredTx[] | undefined {
		const height = this.blockHashToHeightMap.get(hash);
		if (height === undefined) return undefined;
		return this.getTxsByBlockHeight(height);
	}

	getHeightByHash(hash: Uint8Array): number | undefined {
		return this.blockHashToHeightMap.get(hash);
	}

	getHashByHeight(height: number): Uint8Array | undefined {
		const header = this.getHeaderByHeight(height);
		if (!header) return undefined;
		return header.hash();
	}

	getBlockPointerByHeight(height: number): StoredPointer | undefined {
		return atomic.stores.block.get(height);
	}

	getBlockPointerByHash(hash: Uint8Array): StoredPointer | undefined {
		const height = this.blockHashToHeightMap.get(hash);
		if (height === undefined) return undefined;
		return this.getBlockPointerByHeight(height);
	}

	getChainTip(): { height: number; header: StoredBlockHeader } | undefined {
		const height = atomic.stores.header.length() - 1;
		if (height < 0) return undefined;
		const header = this.getHeaderByHeight(height);
		if (!header) return undefined;
		return { height, header };
	}

	getTxOutputByPointer(
		pointer: number,
		batches?: InferBatches<typeof atomic, "tx"> | InferStores<typeof atomic, "tx">,
	): StoredTxOutput {
		return (batches ?? atomic.stores).tx.get(pointer, StoredTxOutput);
	}

	getScriptPubKey(
		output: StoredTxOutput,
		batches?: InferBatches<typeof atomic, "tx"> | InferStores<typeof atomic, "tx">,
	): ScriptPubKey {
		if (output.scriptPubKey.kind === "pointer") {
			const resolved = this.getTxOutputByPointer(output.scriptPubKey.value, batches);
			if (resolved.scriptPubKey.kind === "pointer") {
				throw new Error([
					`scriptPubKey resolution failed: pointer ${output.scriptPubKey.value} points to another pointer.`,
					`Expected direct ScriptPubKey at that offset.`,
				].join(" "));
			}
			return resolved.scriptPubKey;
		} else {
			return output.scriptPubKey;
		}
	}

	getPrevOutTxId(input: StoredTxInput): Uint8Array {
		const txId = input.prevOut.txId;
		const { kind, value } = txId;
		if (kind === "raw") {
			return value;
		}

		if (kind === "pointer") {
			return this.getTxByPointer(value).txId;
		}

		if (kind === "coinbase") {
			return COINBASE_TXID;
		}

		throw new Error(`getPrevOutTxId doesn't handle txId kind: ${kind satisfies never}`);
	}
}
