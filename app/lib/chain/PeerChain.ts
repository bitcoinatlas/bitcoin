import { PeerChainNode } from "./PeerChainNode.ts";

export class PeerChain implements Iterable<PeerChainNode> {
	private nodes: PeerChainNode[];

	constructor(nodes: PeerChainNode[]) {
		this.nodes = nodes;
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

	public truncate(height: number): void {
		this.nodes.length = height + 1;
	}

	public clear(): void {
		this.nodes.length = 0;
	}

	public push(...headers: PeerChainNode[]): number {
		return this.nodes.push(...headers);
	}

	public concat(headers: PeerChainNode[]): void {
		this.nodes = this.nodes.concat(headers);
	}

	public at(height: number): PeerChainNode | undefined {
		return this.nodes.at(height);
	}

	public length(): number {
		return this.nodes.length;
	}
}
