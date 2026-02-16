// ── Core Utilities: Structs, Traits, and Impls Without Classes ──

export type Trait<Self = any> = {
	[key: string]: (self: Self, ...args: any) => any;
};

export type Impl<Self = any, Traits extends Trait<Self> = {}> = {
	[key: string]:
		| ((...args: any) => any)
		| ((self: Self, ...args: any) => any);
} & Traits;

export type ExtractTrait<T, Self> = {
	[K in keyof T as T[K] extends (self: Self, ...args: any) => any ? K : never]: T[K];
};

export type Dyn<T extends Trait> = {
	[K in keyof T]: T[K] extends (self: any, ...args: infer A) => infer R ? (...args: A) => R : T[K];
};

export function dyn<T extends Impl<Self>, Self>(
	impl: T,
	instance: Self,
): Dyn<ExtractTrait<T, Self>> {
	return new Proxy(impl, {
		get(target, prop, receiver) {
			const value = Reflect.get(target, prop, receiver);
			if (typeof value === "function") return (...args: any[]) => value(instance, ...args);
			return value;
		},
	}) as never;
}
