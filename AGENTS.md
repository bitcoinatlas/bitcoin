# Rust-Flavored TypeScript: Structs, Traits, and Impls Without Classes

## Core Utilities (copy/paste)

```typescript
export type Trait<Self = any> = {
	[key: string]: (self: Self, ...args: any) => any;
};

export type Impl<Self, Traits extends Trait<Self> = {}> = {
	[key: string]:
		| ((...args: any) => Promise<Self> | Self)
		| ((self: Self, ...args: any) => any);
} & Traits;

export type DefaultImpl<T extends Trait> = Partial<T>;

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

type ExtractTrait<T, Self> = {
	[K in keyof T as T[K] extends (self: Self, ...args: any) => any ? K : never]: T[K];
};
```

## Structs

Structs are plain data types (no methods).

```typescript
export type Circle = { radius: number };
export type Square = { size: number };
export type Point = { x: number; y: number };
```

Struct composition uses nesting (not intersection).

```typescript
// ✅ Nested composition
export type Square = {
	size: number;
	position: Point;
};

// ❌ Intersection
export type Square = Point & {
	size: number;
};
```

## Traits

Traits are generic types. Every method takes `self: Self` as the first parameter. `Self` defaults to `any`.

```typescript
export type Drawable<Self = any> = {
	draw(self: Self): void;
};

export type Resizeable<Self = any> = {
	resize(self: Self, factor: number): void;
};
```

### Supertraits

A trait can require another trait via intersection.

```typescript
export type Fancy<Self = any> = Drawable<Self> & {
	sparkle(self: Self): void;
};
```

### Default Impls

A trait can have a companion helper (often a `XDefaults<Self>()` factory) that returns a default implementation.

```typescript
export const DrawableDefaults = <Self>() =>
	({
		draw(self) {
			console.log(`Drawing ${JSON.stringify(self)}`);
		},
	}) satisfies DefaultImpl<Drawable<Self>>;
```

## Impls

Impls are `const` objects (same name as the struct type). Always use `satisfies` on the object literal.

- `satisfies Impl<Self>`: inherent methods only
- `satisfies Impl<Self, Traits>`: inherent + trait methods

Rules for method typing:

- Inherent methods (e.g. `create`) **must** have an explicit return type.
- Trait methods **must not** annotate args/return; they get contextual types from `satisfies`.

```typescript
export const Point = {
	create(x: number, y: number): Point {
		return { x, y };
	},
} satisfies Impl<Point>;

export const Circle = {
	...DrawableDefaults<Circle>(),
	create(radius: number): Circle {
		return { radius };
	},
} satisfies Impl<Circle, Drawable<Circle>>;

export const Square = {
	create(size: number): Square {
		return { size };
	},
	draw(self) {
		console.log(`Drawing a square with size ${self.size}`);
	},
	resize(self, factor) {
		self.size *= factor;
	},
} satisfies Impl<Square, Drawable<Square> & Resizeable<Square>>;
```

Usage:

```typescript
const square = Square.create(10);
Square.draw(square);
Square.resize(square, 2);
Square.draw(square);
```

## dyn() and Dyn (trait objects)

In this pattern, traits are _functions over data_: methods look like `draw(self, ...)`.

Use `Dyn<Trait>` when you want a value that represents “some concrete type implementing this trait” — i.e. when you need
to **store** or **accept** something _by its trait_ rather than by its concrete struct type.

`dyn(impl, instance)` creates that trait object by binding `instance` to `impl` (so the returned methods don’t need an
explicit `self` parameter).

### Why it matters (storing a trait in a struct)

You can’t store an unbound trait surface (`Drawable`) and then pass a `Circle`:

```ts
export type DrawRunner = {
	// ❌ refers to the trait surface, not a bound instance
	drawable: Drawable;
};

export const DrawRunner = {
	create(drawable: Drawable): DrawRunner {
		return { drawable };
	},
} satisfies Impl<DrawRunner>;

// ❌ circle is data, not a Drawable
DrawRunner.create(circle);
//                ^^^^^^ 'Circle' is not assignable to 'Drawable'.
```

Instead, store a **trait object**: `Dyn<Drawable>`.

```ts
export type DrawRunner = {
	drawable: Dyn<Drawable>;
};

export const DrawRunner = {
	create(drawable: Dyn<Drawable>): DrawRunner {
		return { drawable };
	},
	run(self: DrawRunner) {
		self.drawable.draw();
	},
} satisfies Impl<DrawRunner>;
```

Now any struct with a `Drawable` impl can be wrapped and passed in:

```ts
const circle = Circle.create(5);

// ✅ Circle implements Drawable
const runner = DrawRunner.create(dyn(Circle, circle));
DrawRunner.run(runner);
```

## Rules

1. Structs are `type` aliases: pure data, no methods
2. Struct composition uses nesting (no intersection types for “embedding”)
3. Traits are generic types: `type X<Self = any> = { method(self: Self, ...): ... }`
4. Impls are `const` objects: `const X = { ... } satisfies Impl<X, Traits?>`
5. Inherent methods: explicit return type annotation required
6. Trait methods: no arg/return annotations; types come from `satisfies`
7. Methods return plain data (structs) only
8. Compose traits with `&`
9. Use `Dyn<Trait>` to type and `dyn(impl, instance)` to create trait objects — needed whenever you store or accept data
   by its trait rather than its concrete type
