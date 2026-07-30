export abstract class Store {
	abstract size(): number;
	abstract resize(size: number): void;
	abstract sync(): void;
}
