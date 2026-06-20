/**
 * Fixed-capacity FIFO queue backed by a ring buffer.
 *
 * - O(1) enqueue / dequeue
 * - No Array.prototype.shift (no re-indexing)
 * - No reallocation: the backing array is allocated once
 *
 * Capacity is rounded up to a power of two so wrap-around can use a
 * bitmask (`& mask`) instead of the slower `% length`.
 */
export class Queue<T> {
	private readonly buffer: (T | undefined)[];
	private readonly mask: number;
	private head = 0; // index of the next item to dequeue
	private tail = 0; // index of the next free slot to enqueue into
	private size_ = 0;

	constructor(capacity = 16) {
		let cap = 1;
		while (cap < capacity) cap <<= 1; // next power of two
		this.buffer = new Array<T | undefined>(cap);
		this.mask = cap - 1;
	}

	size(): number {
		return this.size_;
	}

	capacity(): number {
		return this.buffer.length;
	}

	isEmpty(): boolean {
		return this.size_ === 0;
	}

	isFull(): boolean {
		return this.size_ === this.buffer.length;
	}

	/** Returns true on success, false if the queue is full. */
	enqueue(value: T): boolean {
		if (this.size_ === this.buffer.length) return false;
		this.buffer[this.tail] = value;
		this.tail = (this.tail + 1) & this.mask;
		this.size_++;
		return true;
	}

	/** Returns the oldest item, or undefined if empty. */
	dequeue(): T | undefined {
		if (this.size_ === 0) return undefined;
		const value = this.buffer[this.head] as T;
		this.buffer[this.head] = undefined; // drop reference so it can be GC'd
		this.head = (this.head + 1) & this.mask;
		this.size_--;
		return value;
	}

	/** Look at the oldest item without removing it. */
	peek(): T | undefined {
		return this.size_ === 0 ? undefined : (this.buffer[this.head] as T);
	}

	clear(): void {
		this.buffer.fill(undefined);
		this.head = this.tail = this.size_ = 0;
	}

	/** Iterate from oldest to newest without mutating. */
	*[Symbol.iterator](): IterableIterator<T> {
		for (let i = 0; i < this.size_; i++) {
			yield this.buffer[(this.head + i) & this.mask] as T;
		}
	}
}
