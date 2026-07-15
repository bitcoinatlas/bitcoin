import { delay } from "@std/async";
import { sha256 } from "@noble/hashes/sha2";
import { atomic } from "~/chain/atomic.ts";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxPointer } from "~/codec/stored/StoredTxPointer.ts";
import { StoredTxs } from "~/codec/stored/StoredTxs.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";
import { COINBASE_TXID, MAX_BLOCK_WEIGHT } from "~/constants.ts";
import { Queue } from "~/libs/collections/Queue.ts";
import { Uint8ArrayMap } from "~/libs/collections/Uint8ArrayMap.ts";
import { MessagePortLike } from "~/libs/message/mod.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";

export class ChainStore {
	public readonly blockHashToHeightMap: Uint8ArrayMap<number>;
	public readonly atomic = atomic;

	private p2pChannel: MessagePortLike;
	private p2pMessageQueue: Queue<{ type: string; data: any }>;

	private consumers: Worker[];
	private consumerPubkeys: Uint8Array[][];
	private consumerIndex: number;
	private consumerInitialized: number;
	private consumersInitilizedPromise: PromiseWithResolvers<void>;
	private consumerPubkeyPointerCache: FastUint8ArrayMap<number>;
	private consumerStoredTxs: Uint8Array[][];

	private constructor(p2pChannel: MessagePortLike, initialHeaders: WireBlockHeader[]) {
		this.p2pChannel = p2pChannel;
		this.p2pMessageQueue = new Queue(1000);
		this.blockHashToHeightMap = new Uint8ArrayMap<number>(Math.max(256, initialHeaders.length * 2));
		for (let i = 0; i < initialHeaders.length; i++) {
			this.blockHashToHeightMap.set(initialHeaders[i]!.hash(), i);
		}

		this.consumers = new Array(navigator.hardwareConcurrency);
		this.consumerPubkeys = new Array(navigator.hardwareConcurrency);
		this.consumerStoredTxs = new Array(navigator.hardwareConcurrency);
		this.consumerIndex = 0;
		this.consumerInitialized = 0;
		this.consumersInitilizedPromise = Promise.withResolvers();
		this.consumerPubkeyPointerCache = new FastUint8ArrayMap();
		for (let i = 0; i < this.consumers.length; i++) {
			const worker = new Worker(new URL("./consume.worker.ts", import.meta.url), { name: `consumer-${i}` });
			this.consumers[i] = worker;
			this.consumerPubkeys[i] = [];
		}
	}

	static start(p2pChannel: MessagePortLike): ChainStore {
		const headers = atomic.stores.headers.slice(0, atomic.stores.headers.length());
		console.log(`[chain] loaded ${headers.length} headers from disk`);
		const self = new ChainStore(p2pChannel, headers);

		p2pChannel.addEventListener("message", (event) => self.p2pMessageQueue.enqueue(event.data));
		const startHeaders = atomic.stores.headers.slice(0, atomic.stores.headers.length());
		console.log(`[chain] handing ${startHeaders.length} headers to worker`);
		const startData = WireBlockHeaders.encode(startHeaders);
		p2pChannel.postMessage({ type: "seek", data: atomic.stores.blocks.length() - 1 });
		p2pChannel.postMessage({ type: "start", data: startData }, [startData.buffer]);
		return self;
	}

	getHeaderByHeight(height: number): StoredBlockHeader | undefined {
		const header = atomic.stores.headers.get(height);
		if (!header) return undefined;
		return header;
	}

	getHeaderByRange(
		from: number,
		to: number,
	): Array<{ height: number; header: WireBlockHeader }> {
		const headers = atomic.stores.headers.slice(from, to + 1);
		return headers.map((header, i) => ({ height: from + i, header: header }));
	}

	getHeaderByHash(hash: Uint8Array): StoredBlockHeader | undefined {
		const height = this.blockHashToHeightMap.get(hash);
		if (height === undefined) return undefined;
		return this.getHeaderByHeight(height);
	}

	getTxByPointer(pointer: StoredTxPointer): StoredTx {
		const storedTx = atomic.stores.txs.get(pointer, StoredTx, { readAheadSize: 400_000 });
		return storedTx;
	}

	getTxById(txId: Uint8Array): StoredTx | undefined {
		const pointer = atomic.stores.txid.get(txId);
		if (pointer === undefined) return undefined;
		return this.getTxByPointer(pointer.pointer);
	}

	getTxsByBlockPointer(pointer: StoredTxPointer): StoredTx[] | undefined {
		const storedTxs = atomic.stores.txs.get(pointer, StoredTxs, { readAheadSize: MAX_BLOCK_WEIGHT });
		return storedTxs;
	}

	getTxsByBlockHeight(height: number): StoredTx[] | undefined {
		const pointer = height === 0 ? 0 : atomic.stores.blocks.get(height);
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

	getBlockPointerByHeight(height: number): StoredTxPointer | undefined {
		return atomic.stores.blocks.get(height);
	}

	getBlockPointerByHash(hash: Uint8Array): StoredTxPointer | undefined {
		const height = this.blockHashToHeightMap.get(hash);
		if (height === undefined) return undefined;
		return this.getBlockPointerByHeight(height);
	}

	getChainTip(): { height: number; header: StoredBlockHeader } | undefined {
		const height = atomic.stores.headers.length() - 1;
		if (height < 0) return undefined;
		const header = this.getHeaderByHeight(height);
		if (!header) return undefined;
		return { height, header };
	}

	getPrevOutTxId(input: StoredTxInput): Uint8Array {
		const txId = input.prevOut.txId;
		const { kind, value } = txId;

		if (kind === "pointer") {
			return this.getTxByPointer(value).txId;
		}

		if (kind === "coinbase") {
			return COINBASE_TXID;
		}

		throw new Error(`getPrevOutTxId doesn't handle txId kind: ${kind satisfies never}`);
	}

	async tick(): Promise<void> {
		const message = this.p2pMessageQueue.dequeue();
		if (!message) {
			await delay(0);
			return;
		}
		if (message.type === "blocks") {
			const chunk = message.data as Uint8Array;
			await this.handleBlocksMessage(chunk);
			return;
		}

		if (message.type === "headers") {
			const [headers] = WireBlockHeaders.decode(message.data);
			await this.handleHeadersMessage(headers);
			return;
		}

		if (message.type === "reorg") {
			console.log(`[chain] tick reorg keepHeight=${message.data}`);
			const height = message.data as number;
			this.handleReorgMesage(height);
			return;
		}
	}

	private handleReorgMesage(_keepHeight: number): void {
		throw new Error("Not Implemented");
	}

	private async handleHeadersMessage(headers: WireBlockHeaders) {
		try {
			let height = atomic.stores.headers.length();
			atomic.trx((stores) => {
				for (const header of headers) {
					height = stores.headers.push(header);
					this.blockHashToHeightMap.set(header.hash(), height);
				}
			});
			console.log(`[chain] tick headers height=${height} count=${headers.length}`);
			return { height };
		} catch (reason) {
			console.error("Failed to append block header:", reason);
			Deno.exit(1);
		}
	}

	private async handleBlocksMessage(chunk: Uint8Array) {
		// TODO: Later make this easier to read and better.
		// Maybe p2p can send all of the chunks we need at once.
		// We can also have a single worker that handles all of the chunks that spawns its own workers.
		// We can have an better pipelines to handle things like this in general as well, maybe?
		// etc. think about it later. For now, this is fine.
		console.log(`[chain] new chunk to consume size=${chunk.length}`);

		const index = this.consumerIndex;
		const first = index === 0;
		this.consumerIndex = (this.consumerIndex + 1) % this.consumers.length;
		const last = this.consumerIndex === 0;
		const consumer = this.consumers[index]!;

		if (first) {
			this.consumerInitialized = 0;
			this.consumersInitilizedPromise = Promise.withResolvers<void>();
			this.consumerPubkeyPointerCache.clear();
		}

		// TODO: here at first stage probably we should also handle prevOutTx
		consumer.addEventListener("message", (event) => {
			const { pubkeys, txIds } = event.data as { pubkeys: Uint8Array[]; txIds: Uint8Array[] };
			this.consumerPubkeys[index] = pubkeys;
			this.consumerInitialized++;
			if (this.consumerInitialized === this.consumers.length) {
				this.consumersInitilizedPromise.resolve();
			}
		}, { once: true });
		consumer.postMessage({ stage: "init", data: chunk }, [chunk.buffer]);

		if (last) {
			// await first stage of all consumers to finish
			await this.consumersInitilizedPromise.promise;
			await atomic.trx(async (stores, trx) => {
				let processCount = 0;
				const processPromise = Promise.withResolvers<void>();

				for (let index = 0; index < this.consumers.length; index++) {
				}

				// start and wait next stages of all consumers here
				for (let index = 0; index < this.consumers.length; index++) {
					// make sure here we are only looking for pubkeys the worker couldn't find it self.
					const consumer = this.consumers[index]!;
					const pubkeys = this.consumerPubkeys[index]!;
					const pubkeyPointers = new BigUint64Array(pubkeys.length);
					for (let index = 0; index < pubkeys.length; index++) {
						const pubkey = pubkeys[index]!;
						const pubkeyHash = sha256(pubkey);
						const memoryPubkeyPointer = this.consumerPubkeyPointerCache.get(pubkeyHash);
						let pubkeyPointer = memoryPubkeyPointer ?? stores.pubkey.get(pubkeyHash);
						if (pubkeyPointer === undefined) {
							pubkeyPointer = stores.pubkeys.append(pubkey);
							stores.pubkey.set(pubkeyHash, pubkeyPointer, trx);
							if (memoryPubkeyPointer === undefined) {
								this.consumerPubkeyPointerCache.put(pubkeyHash, pubkeyPointer);
							}
						}
						pubkeyPointers[index] = BigInt(pubkeyPointer);
					}

					consumer.addEventListener("message", (event) => {
						processCount++;
						this.consumerStoredTxs[index] = event.data as Uint8Array[];
						if (processCount !== this.consumers.length) return;
						processPromise.resolve();
					}, { once: true });
					consumer.postMessage({ stage: "process", data: pubkeyPointers }, [pubkeyPointers.buffer]);
				}

				await processPromise.promise;

				for (let index = 0; index < this.consumers.length; index++) {
					const chunk = this.consumerStoredTxs[index]!;
					for (const block of chunk) {
						const blockPointer = stores.txs.append(block);
						stores.blocks.push(blockPointer);
					}
				}
			});

			// let p2p know that we have consumed every chunk
			for (let index = 0; index < this.consumers.length; index++) {
				this.p2pChannel.postMessage({ type: "consume" });
			}
		}
	}
}
