import { Codec } from "@nomadshiba/codec";
import { RouteResponse, RouteResponseOptions } from "~/app/libs/routing/RouterResponse.ts";
import { _, PromiseOrValue } from "~/types.ts";
import { OptionalizeEmpty } from "~/app/libs/types/utils.ts";

type SchemaKeyGeneric = `${string} /${string}`;
export type Schema = { [key: SchemaKeyGeneric]: { input: Codec<_>; output: Codec<_> } };

export namespace Schema {
	export type Key<TSchema extends Schema = Schema> = Extract<keyof TSchema, SchemaKeyGeneric>;

	export type InferParamsInput<TKey extends Key> = OptionalizeEmpty<{
		pathname: Record<MapPathParams<InferPattern<TKey>["Path"]>[number], string | bigint | number>;
		search: Record<MapSearchParams<InferPattern<TKey>["Search"]>[number], string | bigint | number>;
	}>;
	export type InferParamsOutput<TKey extends Key> = OptionalizeEmpty<{
		pathname: Record<MapPathParams<InferPattern<TKey>["Path"]>[number], string>;
		search: Record<MapSearchParams<InferPattern<TKey>["Search"]>[number], string>;
	}>;

	export type InferDataInput<TSchema extends Schema, TKey extends keyof TSchema> = Codec.InferInput<InferItem<TSchema, TKey>["input"]>;
	export type InferDataOutput<TSchema extends Schema, TKey extends keyof TSchema> = Codec.InferOutput<
		InferItem<TSchema, TKey>["input"]
	>;

	export type InferResultInput<TSchema extends Schema, TKey extends keyof TSchema> = Codec.InferInput<InferItem<TSchema, TKey>["output"]>;
	export type InferResultOutput<TSchema extends Schema, TKey extends keyof TSchema> = Codec.InferOutput<
		InferItem<TSchema, TKey>["output"]
	>;

	// Internal Helpers
	type InferItem<T extends Schema, K extends keyof T> = Extract<T[K], Schema[keyof Schema]>;
	type IsParam<T extends string> = T extends `:${infer U}` ? U : never;
	type InferPattern<K extends Key> = K extends `${string} ${infer Path}?${infer Search}` ? { Path: Path; Search: Search }
		: K extends `${string} ${infer Path}` ? { Path: Path; Search: "" }
		: never;
	type MapPathParams<T extends string> = T extends `/${infer U}/${infer Rest}` ? [IsParam<U>, ...MapPathParams<`/${Rest}`>]
		: T extends `/${infer U}` ? [IsParam<U>]
		: [];
	type MapSearchParams<T extends string> = T extends `${string}=${infer U}&${infer Rest}` ? [IsParam<U>, ...MapSearchParams<Rest>]
		: T extends `${string}=${infer U}` ? [IsParam<U>]
		: [];
}

type RouteEvent = { request: Request; url: URL };

type OmitCodec<T> = T extends { data: unknown } ? Omit<T, "codec"> : T;

type RouteHandler<
	TSchema extends Schema = _,
	TKey extends Schema.Key<TSchema> = _,
	TMeta = _,
> = (
	options: RouteHandlerOptions<TSchema, TKey, TMeta>,
) => PromiseOrValue<RouteHandlerResult<TSchema, TKey>>;

type RouteHandlerOptions<
	TSchema extends Schema,
	TKey extends Schema.Key<TSchema>,
	TMeta,
> = {
	event: RouteEvent;
	params: Schema.InferParamsOutput<TKey>;
	data: Schema.InferDataOutput<TSchema, Schema.Key<TSchema>>;
	meta: TMeta;
};

type RouteHandlerResult<TSchema extends Schema, TKey extends Schema.Key<TSchema>> = OmitCodec<
	RouteResponseOptions<Schema.InferResultInput<TSchema, TKey>>
>;

type RouteMiddlewareOptions<TSchema extends Schema = _> = {
	event: RouteEvent;
	params: Schema.InferParamsOutput<Schema.Key<TSchema>>;
	data: Schema.InferDataOutput<TSchema, Schema.Key<TSchema>>;
};

type RouteMiddlewareResult<TMeta = _> = { meta: TMeta };

type Bucket = {
	pattern: URLPattern;
	methods: Map<string, {
		input: Codec<_>;
		output: Codec<_>;
		handler: RouteHandler | null;
	}>;
}[];
export class Router<const TSchema extends Schema, TMeta> {
	private readonly metaMiddleware?: (
		options: RouteMiddlewareOptions,
	) => PromiseOrValue<RouteMiddlewareResult>;
	public readonly schema: TSchema;
	public readonly prefixBuckets: readonly (readonly [string, Bucket])[];

	constructor(params: {
		metaMiddleware?: (options: RouteMiddlewareOptions<TSchema>) => PromiseOrValue<RouteMiddlewareResult<TMeta>>;
		schema: TSchema;
	}) {
		this.schema = params.schema;
		this.metaMiddleware = params.metaMiddleware;

		const prefixBucketMap = new Map<string, Map<string, Bucket[number]>>();
		for (const [key, { input, output }] of Object.entries(this.schema)) {
			const [method, pattern] = key.split(" ") as [string, string];
			const [pathname, search] = pattern.split("?") as [string, string] | [string];
			const colonIndex = pathname.indexOf(":");
			const prefix = colonIndex === -1 ? pathname : pathname.slice(0, colonIndex);

			let bucket = prefixBucketMap.get(prefix);
			if (!bucket) {
				bucket = new Map();
				prefixBucketMap.set(prefix, bucket);
			}
			let patternMatch = bucket.get(pattern);
			if (!patternMatch) {
				const patternUrl = new URLPattern({ pathname, search });
				patternMatch = { pattern: patternUrl, methods: new Map() };
				bucket.set(pattern, patternMatch);
			}
			patternMatch.methods.set(method, { input, output, handler: null });
		}
		this.prefixBuckets = prefixBucketMap.entries()
			.map(([prefix, bucket]) =>
				[
					prefix,
					bucket.values().toArray()
						.sort((a, b) => b.pattern.pathname.split("/").length - a.pattern.pathname.split("/").length),
				] as const
			).toArray()
			.sort(([a], [b]) => b.split("/").length - a.split("/").length);
	}

	registerHandler<TKey extends Schema.Key<TSchema>>(
		key: TKey,
		handler: RouteHandler<TSchema, TKey, TMeta>,
	) {
		const [method, pattern] = key.split(" ") as [string, string];
		const [pathname, search] = pattern.split("?") as [string, string] | [string];
		for (const [prefix, bucket] of this.prefixBuckets) {
			if (!pathname.startsWith(prefix)) continue;
			for (const { pattern, methods } of bucket.values()) {
				const match = pattern.test({ pathname, search });
				if (!match) continue;
				const item = methods.get(method);
				if (!item) continue;
				item.handler = handler;
			}
		}
	}

	async resolveRequest(request: Request): Promise<Response> {
		const url = new URL(request.url);
		const event = { request, url };

		const { pathname, search } = url;
		const { method } = request;

		let hasPatternMatch = false;

		for (const [prefix, bucket] of this.prefixBuckets) {
			if (!pathname.startsWith(prefix)) continue;
			for (const { pattern, methods } of bucket.values()) {
				const match = pattern.exec({ pathname, search });
				if (!match) continue;
				const item = methods.get(method);
				if (!item) {
					// Pattern matched but method didn't — remember this,
					// but keep looking for a more specific pattern that does match.
					hasPatternMatch = true;
					continue;
				}
				const { handler } = item;
				if (!handler) return new RouteResponse({ status: "NotImplemented" });

				const params = {
					pathname: match.pathname.groups,
					search: Object.fromEntries(url.searchParams.entries()),
				};

				const contentType = request.headers.get("Content-Type");

				let data;
				if (contentType === "application/octet-stream") {
					try {
						[data] = item.input.decode(await request.bytes());
					} catch (reason) {
						return new RouteResponse({ status: "BadRequest", message: String(reason) });
					}
				} else if (contentType !== null) {
					return new RouteResponse({ status: "UnsupportedMediaType" });
				}

				try {
					const { meta } = await this.metaMiddleware?.({ event, params, data }) ?? {};
					const options = await handler({ event, params, data, meta });

					if ("data" in options) {
						return new RouteResponse({ ...options, codec: item.output });
					}
					return new RouteResponse(options);
				} catch (reason) {
					if (reason instanceof Response) {
						return reason;
					}

					console.error(reason);

					const message = String(reason);
					return new RouteResponse({ status: "InternalServerError", message });
				}
			}
		}

		if (hasPatternMatch) {
			return new RouteResponse({ status: "MethodNotAllowed" });
		}
		return new RouteResponse({ status: "NotFound" });
	}
}
