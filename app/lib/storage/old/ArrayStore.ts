import type { Codec } from "@nomadshiba/codec";
import type { Transaction, Transactionable } from "./Store.ts";

export class ArrayStoreTransaction<T extends Codec<any>> implements Transaction {
}

export type ArrayStoreOptions<T extends Codec<any>> = {
	path: string;
	codec: T;
};

export class ArrayStore<T extends Codec<any>> implements Transactionable {
	public readonly path: string;
	public readonly codec: T;
	private readonly memory: Codec.InferOutput<T>[];

	constructor(options: ArrayStoreOptions<T>) {
		if (options.codec.stride < 0) throw new Error("Codec must have fixed size");
		this.path = options.path;
		this.codec = options.codec;
		this.memory = [];
	}

	push(items: Codec.InferOutput<T>[]): void {
		this.memory.push(...items);
	}
}
