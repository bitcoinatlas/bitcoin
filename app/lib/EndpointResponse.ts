import { ErrorStatus, RedirectStatus, STATUS_CODE, STATUS_TEXT, SuccessfulStatus } from "@std/http";
import { Codec } from "@nomadshiba/codec";
import { _, PickByValue } from "~/types.ts";

export type EndpointResponseOptions<TData = unknown> =
	| {
		status: keyof PickByValue<typeof STATUS_CODE, SuccessfulStatus>;
		data: TData;
		format?: { kind: "application/octet-stream"; codec: Codec<_, TData> };
		response?: ResponseInit;
	}
	| { status: keyof PickByValue<typeof STATUS_CODE, RedirectStatus>; location: string | URL; response?: ResponseInit }
	| { status: keyof PickByValue<typeof STATUS_CODE, ErrorStatus>; message?: string; response?: ResponseInit };

export class EndpointResponse<TData = unknown> extends Response {
	constructor(options: EndpointResponseOptions<TData>) {
		const status = STATUS_CODE[options.status];
		const headers = new Headers(options.response?.headers ?? {});
		let body: BodyInit | null = null;
		if ("data" in options) {
			const { format } = options;
			if (!format) {
				throw new Error("Data response must include a format");
			}
			const { kind } = format;
			if (kind === "application/octet-stream") {
				const bytes = format.codec.encode(options.data);
				const blob = new Blob([bytes]);
				body = blob;
			} else {
				throw new Error(`Unhandled Content-Type: ${kind satisfies never}`);
			}
			headers.set("Content-Type", kind);
		} else if ("location" in options) {
			headers.set("Location", String(options.location));
		} else {
			body = options.message ?? STATUS_TEXT[status];
			headers.set("Content-Type", "text/plain");
		}
		super(body, { ...options.response, status, headers });
	}
}
