export abstract class Store {
	abstract size(): number;
	abstract resize(size: number): void;
}
