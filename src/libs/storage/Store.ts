export abstract class Store {
	abstract size(): number;
	abstract resize(size: number): void; // TODO: seperate this into truncate() and persist()
	abstract sync(): void;
}
