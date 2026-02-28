import { BlockHeader } from "~/lib/satoshi/primitives/BlockHeader.ts";
import { ChainNode } from "./ChainNode.ts";

const EMPTY_NODE: ChainNode = {
	header: new Uint8Array(BlockHeader.stride),
	hash: new Uint8Array(32),
	cumulativeWork: 0n,
};

export class Chain implements Iterable<ChainNode> {
	private chain: ChainNode[];

	constructor(use: ChainNode[]) {
		this.chain = use;
	}

	[Symbol.iterator](): ArrayIterator<Readonly<ChainNode>> {
		return this.chain.values();
	}

	public entries(): ArrayIterator<[number, Readonly<ChainNode>]> {
		return this.chain.entries();
	}

	public values(): ArrayIterator<Readonly<ChainNode>> {
		return this.chain.values();
	}

	public height(): number {
		return this.chain.length - 1;
	}

	public tip(): ChainNode {
		return this.chain.at(-1) ?? EMPTY_NODE;
	}

	public truncate(height: number): void {
		this.chain.length = height + 1;
	}

	public clear(): void {
		this.chain.length = 0;
	}

	public append(...headers: ChainNode[]): void {
		this.chain.push(...headers);
	}

	public at(height: number): ChainNode | undefined {
		return this.chain.at(height);
	}

	public length(): number {
		return this.chain.length;
	}
}
