export abstract class Store {
	abstract size(): number;
	abstract truncate(size: number): void;
}
