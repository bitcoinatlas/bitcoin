export abstract class Store<K, V> {
	abstract init(path: string): Promise<void>;
	abstract get(key: K): Promise<V | undefined>;
	abstract set(key: K, value: V): Promise<void>;
}
