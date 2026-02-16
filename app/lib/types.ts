export type PartialTuple<T extends readonly unknown[]> =
	| T
	| (T extends [infer F, ...infer R] ? [F] | [F, ...PartialTuple<R>] : T);

export type NonReadonly<T> = { -readonly [K in keyof T]: T[K] };

