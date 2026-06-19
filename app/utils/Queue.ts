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
	private readonly _buffer: (T | undefined)[];
	private readonly _mask: number;
	private _head = 0; // index of the next item to dequeue
	private _tail = 0; // index of the next free slot to enqueue into
	private _size = 0;

	constructor(capacity = 16) {
		let cap = 1;
		while (cap < capacity) cap <<= 1; // next power of two
		this._buffer = new Array<T | undefined>(cap);
		this._mask = cap - 1;
	}

	size(): number {
		return this._size;
	}

	capacity(): number {
		return this._buffer.length;
	}

	isEmpty(): boolean {
		return this._size === 0;
	}

	isFull(): boolean {
		return this._size === this._buffer.length;
	}

	/** Returns true on success, false if the queue is full. */
	enqueue(value: T): boolean {
		if (this._size === this._buffer.length) return false;
		this._buffer[this._tail] = value;
		this._tail = (this._tail + 1) & this._mask;
		this._size++;
		return true;
	}

	/** Returns the oldest item, or undefined if empty. */
	dequeue(): T | undefined {
		if (this._size === 0) return undefined;
		const value = this._buffer[this._head] as T;
		this._buffer[this._head] = undefined; // drop reference so it can be GC'd
		this._head = (this._head + 1) & this._mask;
		this._size--;
		return value;
	}

	/** Look at the oldest item without removing it. */
	peek(): T | undefined {
		return this._size === 0 ? undefined : (this._buffer[this._head] as T);
	}

	clear(): void {
		this._buffer.fill(undefined);
		this._head = this._tail = this._size = 0;
	}

	/** Iterate from oldest to newest without mutating. */
	*[Symbol.iterator](): IterableIterator<T> {
		for (let i = 0; i < this._size; i++) {
			yield this._buffer[(this._head + i) & this._mask] as T;
		}
	}
}
