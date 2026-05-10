export type _ = any;
export type PromiseOrValue<T> = T | Promise<T>;
export type OmitByValue<T, V> = {
	[K in keyof T as T[K] extends V ? never : K]: T[K];
};
export type PickByValue<T, V> = {
	[K in keyof T as T[K] extends V ? K : never]: T[K];
};
export type NeverFallback<TMaybeNever, TFallback> = [TMaybeNever] extends [never] ? TFallback : TMaybeNever;
export type Empty = { [K in never]?: never };
export type StringInput = string | number | bigint;
