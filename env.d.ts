export {};

declare global {
	interface ObjectConstructor {
		entries<T>(o: T): [Extract<keyof T, string>, T[Extract<keyof T, string>]][];
		keys<T>(o: T): Extract<keyof T, string>[];
	}
}
