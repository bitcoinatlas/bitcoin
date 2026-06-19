import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts"; // adjust path
import type { PeerChainNode } from "~/p2p/PeerChainNode.ts";

export class PeerChain implements Iterable<PeerChainNode> {
	private nodes: PeerChainNode[];
	private index = new FastUint8ArrayMap<number>();

	constructor(nodes: PeerChainNode[] = []) {
		this.nodes = nodes;
		this.reindex();
	}

	[Symbol.iterator](): ArrayIterator<PeerChainNode> {
		return this.nodes.values();
	}

	public entries(): ArrayIterator<[number, PeerChainNode]> {
		return this.nodes.entries();
	}

	public values(): ArrayIterator<PeerChainNode> {
		return this.nodes.values();
	}

	public height(): number {
		return this.nodes.length - 1;
	}

	public tip(): PeerChainNode | undefined {
		return this.nodes.at(-1);
	}

	public cumulativeWork(): bigint {
		return this.tip()?.cumulativeWork ?? 0n;
	}

	public at(height: number): PeerChainNode | undefined {
		return this.nodes.at(height);
	}

	public length(): number {
		return this.nodes.length;
	}

	// O(1) hash lookups: reorg fork-finding, blacklist checks, getdata, body fetch.
	public heightOf(hash: Uint8Array): number | undefined {
		return this.index.get(hash);
	}

	public hasHash(hash: Uint8Array): boolean {
		return this.index.has(hash);
	}

	public push(...ns: PeerChainNode[]): number {
		for (const n of ns) {
			this.index.set(n.header.hash(), this.nodes.length);
			this.nodes.push(n);
		}
		return this.nodes.length;
	}

	public concat(ns: PeerChainNode[]): void {
		for (const n of ns) {
			this.index.set(n.header.hash(), this.nodes.length);
			this.nodes.push(n);
		}
	}

	public reorg(keepHeight: number): void {
		this.nodes.length = keepHeight + 1;
		this.reindex();
	}

	public clear(): void {
		this.nodes.length = 0;
		this.index.clear();
	}

	private reindex(): void {
		this.index.clear();
		for (const [h, node] of this.nodes.entries()) {
			this.index.set(node.header.hash(), h);
		}
	}
}
