import { Codec } from "@nomadshiba/codec";
import { EndpointResponse, EndpointResponseOptions } from "~/server/utils/EndpointResponse.ts";
import { _, PromiseOrValue } from "~/types.ts";

type SchemaKeyGeneric = `${string} /${string}`;
export type EndpointSchema = { [key: SchemaKeyGeneric]: { input: Codec<_>; output: Codec<_> } };
export type EndpointSchemaKey<TSchema extends EndpointSchema = EndpointSchema> = Extract<
	keyof TSchema,
	SchemaKeyGeneric
>;

export namespace EndpointSchema {
	export type InferParamsOutput<TSchemaKey extends EndpointSchemaKey> = {
		pathname: Record<MapPathParams<InferPattern<TSchemaKey>["Path"]>[number], string>;
		search:
			& Record<MapSearchParams<InferPattern<TSchemaKey>["Search"]>[number], string>
			// deno-lint-ignore ban-types
			& Record<string & {}, string | undefined>;
	};

	export type InferParamsInput<TSchemaKey extends EndpointSchemaKey> = {
		pathname: Record<MapPathParams<InferPattern<TSchemaKey>["Path"]>[number], string | number | bigint>;
		search:
			& Record<MapSearchParams<InferPattern<TSchemaKey>["Search"]>[number], string | number | bigint>
			// deno-lint-ignore ban-types
			& Record<string & {}, string | number | bigint | undefined>;
	};

	// Internal Helpers
	type IsParam<T extends string> = T extends `:${infer U}` ? U : never;
	type InferPattern<K extends EndpointSchemaKey> = K extends `${string} ${infer Path}?${infer Search}` ? { Path: Path; Search: Search }
		: K extends `${string} ${infer Path}` ? { Path: Path; Search: "" }
		: never;
	type MapPathParams<T extends string> = T extends `/${infer U}/${infer Rest}` ? [IsParam<U>, ...MapPathParams<`/${Rest}`>]
		: T extends `/${infer U}` ? [IsParam<U>]
		: [];
	type MapSearchParams<T extends string> = T extends `${string}=${infer U}&${infer Rest}` ? [IsParam<U>, ...MapSearchParams<Rest>]
		: T extends `${string}=${infer U}` ? [IsParam<U>]
		: [];
}

export type EndpointEvent = { request: Request; url: URL };

type InferItem<T extends EndpointSchema, K extends keyof T> = Extract<T[K], EndpointSchema[keyof EndpointSchema]>;

export type EndpointHandler<
	TSchema extends EndpointSchema = _,
	TSchemaKey extends EndpointSchemaKey<TSchema> = _,
	TMeta = _,
> = (
	options: EndpointHandlerOptions<TSchema, TSchemaKey, TMeta>,
) => PromiseOrValue<EndpointHandlerResult<TSchema, TSchemaKey>>;

export type EndpointHandlerOptions<
	TSchema extends EndpointSchema,
	TSchemaKey extends EndpointSchemaKey<TSchema>,
	TMeta,
> = {
	event: EndpointEvent;
	params: EndpointSchema.InferParamsOutput<TSchemaKey>;
	data: Codec.InferOutput<InferItem<TSchema, TSchemaKey>["input"]>;
	meta: TMeta;
};

export type EndpointHandlerResult<TSchema extends EndpointSchema, TSchemaKey extends EndpointSchemaKey<TSchema>> = EndpointResponseOptions<
	Codec.InferInput<InferItem<TSchema, TSchemaKey>["output"]>
>;

export type EndpointMiddlewareOptions<TSchema extends EndpointSchema = _> = {
	event: EndpointEvent;
	params: EndpointSchema.InferParamsOutput<EndpointSchemaKey<TSchema>>;
	data: Codec.InferOutput<InferItem<TSchema, EndpointSchemaKey<TSchema>>["input"]>;
};

export type EndpointMiddlewareResult<TMeta = _> = { meta: TMeta };

type Bucket = {
	pattern: URLPattern;
	methods: Map<string, {
		input: Codec<_>;
		output: Codec<_>;
		handler: EndpointHandler | null;
	}>;
}[];
export class EndpointRouter<const TSchema extends EndpointSchema, TMeta> {
	private readonly metaMiddleware?: (
		options: EndpointMiddlewareOptions,
	) => PromiseOrValue<EndpointMiddlewareResult>;
	public readonly schema: TSchema;
	public readonly prefixBuckets: readonly (readonly [string, Bucket])[];

	constructor(params: {
		metaMiddleware?: (
			options: EndpointMiddlewareOptions<TSchema>,
		) => PromiseOrValue<EndpointMiddlewareResult<TMeta>>;
		schema: TSchema;
	}) {
		this.schema = params.schema;
		this.metaMiddleware = params.metaMiddleware;

		const prefixBucketMap = new Map<string, Map<string, Bucket[number]>>();
		for (const [key, { input, output }] of Object.entries(this.schema)) {
			const [method, pathname] = key.split(" ") as [string, string];
			const colonIndex = pathname.indexOf(":");
			const prefix = colonIndex === -1 ? pathname : pathname.slice(0, colonIndex);

			let bucket = prefixBucketMap.get(prefix);
			if (!bucket) {
				bucket = new Map();
				prefixBucketMap.set(prefix, bucket);
			}
			let patternMatch = bucket.get(pathname);
			if (!patternMatch) {
				const pattern = new URLPattern({ pathname });
				patternMatch = { pattern, methods: new Map() };
				bucket.set(pathname, patternMatch);
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

	registerHandler<TSchemaKey extends EndpointSchemaKey<TSchema>>(
		key: TSchemaKey,
		handler: EndpointHandler<TSchema, TSchemaKey, TMeta>,
	) {
		const [method, pathname] = key.split(" ") as [string, string];
		for (const [prefix, bucket] of this.prefixBuckets) {
			if (!pathname.startsWith(prefix)) continue;
			for (const { pattern, methods } of bucket.values()) {
				const match = pattern.test({ pathname });
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
				if (!handler) return new EndpointResponse({ status: "NotImplemented" });

				const params = {
					pathname: match.pathname.groups,
					search: Object.fromEntries(url.searchParams.entries()),
				};

				const contentType = request.headers.get("Content-Type");
				if (contentType !== "application/octet-stream") {
					return new EndpointResponse({
						status: "UnsupportedMediaType",
						message: "Content-Type must be application/octet-stream",
					});
				}

				let data;
				try {
					[data] = item.input.decode(await request.bytes());
				} catch (reason) {
					return new EndpointResponse({ status: "BadRequest", message: String(reason) });
				}

				try {
					const { meta } = await this.metaMiddleware?.({ event, params, data }) ?? {};
					const options = await handler({ event, params, data, meta });

					if ("data" in options) {
						options.format = { kind: "application/octet-stream", codec: item.output };
						return new EndpointResponse(options);
					}
					return new EndpointResponse(options);
				} catch (reason) {
					if (reason instanceof Response) {
						return reason;
					}

					console.error(reason);

					const message = String(reason);
					return new EndpointResponse({ status: "InternalServerError", message });
				}
			}
		}

		if (hasPatternMatch) {
			return new EndpointResponse({ status: "MethodNotAllowed" });
		}
		return new EndpointResponse({ status: "NotFound" });
	}
}
