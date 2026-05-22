import { Codec } from "@nomadshiba/codec";
import { STATUS_TEXT } from "@std/http";
import { ENDPOINT_SCHEMA } from "~/api/schema.ts";
import type { EndpointSchema, EndpointSchemaKey } from "~/lib/EndpointRouter.ts";
import { Empty, NeverFallback, StringInput } from "~/types.ts";

type TSchema = typeof ENDPOINT_SCHEMA;

type MakeOptionalIfEmpty<T> =
	& { [K in keyof T as [keyof T[K]] extends [never] ? K : never]?: T[K] }
	& { [K in keyof T as [keyof T[K]] extends [never] ? never : K]: T[K] };

type OptionalIfEmpty<T> = T extends { search: infer S } ? MakeOptionalIfEmpty<Omit<T, "search">> & { search?: S }
	: MakeOptionalIfEmpty<T>;

async function apiFetch<TSchemaKey extends EndpointSchemaKey<TSchema>>(
	key: TSchemaKey,
	params: OptionalIfEmpty<
		EndpointSchema.InferParamsInput<TSchemaKey> & {
			data: NeverFallback<Codec.InferInput<TSchema[TSchemaKey]["input"]>, Empty>;
			headers?: Partial<Record<string, string>>;
		}
	>,
): Promise<Codec.InferOutput<TSchema[TSchemaKey]["output"]>>;
async function apiFetch(
	key: EndpointSchemaKey,
	params: {
		headers?: Partial<Record<string, string>>;
		pathname?: Partial<Record<string, StringInput>>;
		search?: Partial<Record<string, StringInput>>;
		data?: unknown;
	},
) {
	const schemaItem = (ENDPOINT_SCHEMA as EndpointSchema)[key]!;

	const [method, pathname] = key.split(" ") as [string, string];

	const url = new URL(
		pathname.split("/").values().map((part) => {
			if (!part.startsWith(":")) return part;
			return params.pathname?.[part.slice(1)];
		}).toArray().join("/"),
		location.href,
	);
	Object.entries(params.search ?? {}).forEach(([k, value]) => {
		if (value) url.searchParams.set(k, String(value));
	});

	const bytes = schemaItem.input.encode(params.data);
	const body = bytes.length ? new Blob([bytes]) : undefined;
	const response = await fetch(url, { method, body, headers: { "Content-Type": "application/octet-stream" } });

	if (!response.ok) {
		const message = await response.text();
		console.log({
			endpoint: key,
			params,
			result: { status: STATUS_TEXT[response.status as never] ?? response.status, message },
		});
		throw new Error(message);
	} else {
		const responseBytes = await response.bytes();
		const [data] = schemaItem.output.decode(responseBytes);
		console.log({
			endpoint: key,
			params,
			result: {
				status: STATUS_TEXT[response.status as never] ?? response.status,
				data,
			},
		});
		return data;
	}
}

export const api = { fetch: apiFetch };
