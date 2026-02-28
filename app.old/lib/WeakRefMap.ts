export class WeakRefMap<K, V extends object> {
	private map = new Map<K, WeakRef<V>>();
	private finalizer = new FinalizationRegistry<K>((key) => this.map.delete(key));

	set(key: K, value: V): void {
		const oldRef = this.map.get(key);
		if (oldRef) {
			if (oldRef.deref() === value) return;
			this.finalizer.unregister(oldRef);
		}
		const newRef = new WeakRef(value);
		this.map.set(key, newRef);
		this.finalizer.register(value, key, newRef);
	}

	get(key: K): V | undefined {
		return this.map.get(key)?.deref();
	}
}
