/**
 * Represents a durable transaction that can be committed, rolled back,
 * and finalized after successful completion.
 */
export type Transaction = {
	/**
	 * Apply all staged changes to the underlying storage.
	 * This should write a recovery log to disk (or equivalent)
	 * to allow crash recovery if the process fails during commit.
	 *
	 * @returns A promise that resolves once the commit is applied.
	 */
	commit(): Promise<void>;

	/**
	 * Undo all staged or partially applied changes.
	 * Safe to call if commit failed or before finalize.
	 * This should restore the system to the pre-transaction state.
	 */
	rollback(): void;

	/**
	 * Finalize the transaction after a successful commit.
	 * Typically deletes the recovery log and releases any locks
	 * held during commit. After finalize, the transaction is fully durable.
	 *
	 * @returns A promise that resolves once finalization is complete.
	 */
	finalize(): Promise<void>;
};

export type Transactionable = {
	transaction(): Transaction;
	flush(): Promise<void>;
};
