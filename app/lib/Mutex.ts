type MutexRelease = () => void;

export class Mutex {
	private mutex: Promise<any> = Promise.resolve();

	async lock(): Promise<MutexRelease> {
		const { promise, resolve } = Promise.withResolvers<void>();
		const current = this.mutex;
		this.mutex = current.then(() => promise);
		await current;
		return resolve;
	}
}
