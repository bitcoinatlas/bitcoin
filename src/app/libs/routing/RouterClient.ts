import { Codec } from "@nomadshiba/codec";
import { Schema, SchemaKey } from "~/app/libs/routing/Router.ts";
import { _ } from "~/types.ts";
import { OptionalizeEmpty } from "~/app/libs/types/utils.ts";

type InferItem<T extends Schema, K extends keyof T> = Extract<T[K], Schema[keyof Schema]>;

export type ClientRequestOptions<
	TSchema extends Schema,
	TSchemaKey extends SchemaKey<TSchema>,
> = OptionalizeEmpty<{
	params: Schema.InferParams<TSchemaKey>;
	data?: Codec.InferInput<InferItem<TSchema, TSchemaKey>["input"]>;
	request?: RequestInit;
}>;

export class ClientError extends Error {
	constructor(public readonly response: Response, message: string) {
		super(`${response.status} ${response.statusText}: ${message}`);
		this.name = "ClientError";
	}
}

export class RouterClient<const TSchema extends Schema> {
	private constructor(
		public readonly schema: TSchema,
		private readonly baseUrl: string,
		private readonly fetchFn: typeof fetch,
	) {}

	static create<const TSchema extends Schema>(params: {
		baseUrl: string | URL;
		schema: TSchema;
		fetch?: typeof fetch;
	}): RouterClient<TSchema> {
		return new RouterClient(
			params.schema,
			String(params.baseUrl).replace(/\/$/, ""),
			params.fetch ?? fetch,
		);
	}

	async fetch<TSchemaKey extends SchemaKey<TSchema>>(
		key: TSchemaKey,
		options: ClientRequestOptions<TSchema, TSchemaKey>,
	): Promise<Codec.InferOutput<InferItem<TSchema, TSchemaKey>["output"]>> {
		const item = this.schema[key] as Schema[keyof Schema];
		const [method, pattern] = key.split(" ") as [string, string];
		const [pathnameTemplate] = pattern.split("?") as [string, string] | [string];

		const pathnameParams = (options.params?.pathname ?? {}) as Record<string, string>;
		const pathname = pathnameTemplate
			.split("/")
			.map((segment) => {
				if (!segment.startsWith(":")) return segment;
				const name = segment.slice(1);
				const value = pathnameParams[name];
				if (value === undefined) throw new Error(`Missing pathname param: ${name}`);
				return encodeURIComponent(value);
			})
			.join("/");

		const url = new URL(this.baseUrl + pathname);
		for (const [name, value] of Object.entries((options.params?.search ?? {}) as Record<string, string | undefined>)) {
			if (value === undefined) continue;
			url.searchParams.set(name, value);
		}

		const headers = new Headers(options.request?.headers ?? {});
		let body: BodyInit | null = null;
		if (options.data !== undefined) {
			body = new Blob([item.input.encode(options.data)]);
			headers.set("Content-Type", "application/octet-stream");
		}

		const response = await this.fetchFn(url, { ...options.request, method, headers, body });

		if (!response.ok) {
			throw new ClientError(response, await response.text());
		}

		const [data] = item.output.decode(new Uint8Array(await response.arrayBuffer()));
		return data as Codec.InferOutput<InferItem<TSchema, TSchemaKey>["output"]>;
	}
}
