export abstract class Store {
	public abstract size(): number;
	public abstract reveal(size: number): void;
	public abstract truncate(size: number): void;
	public abstract sync(): void;
}
