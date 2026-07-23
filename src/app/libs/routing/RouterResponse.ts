import { ErrorStatus, RedirectStatus, STATUS_CODE, STATUS_TEXT, SuccessfulStatus } from "@std/http";
import { Codec } from "@nomadshiba/codec";
import { _, PickByValue } from "~/types.ts";

export type RouteResponseOptions<TData = unknown> =
    | {
        status: keyof PickByValue<typeof STATUS_CODE, SuccessfulStatus>;
        data: TData;
        codec: Codec<_, TData>;
        response?: ResponseInit;
    }
    | { status: keyof PickByValue<typeof STATUS_CODE, RedirectStatus>; location: string | URL; response?: ResponseInit }
    | { status: keyof PickByValue<typeof STATUS_CODE, ErrorStatus>; message?: string; response?: ResponseInit };

export class RouteResponse<TData = unknown> extends Response {
    constructor(options: RouteResponseOptions<TData>) {
        const status = STATUS_CODE[options.status];
        const headers = new Headers(options.response?.headers ?? {});
        let body: BodyInit | null = null;
        if ("data" in options) {
            const bytes = options.codec.encode(options.data);
            body = new Blob([bytes]);
            headers.set("Content-Type", "application/octet-stream");
        } else if ("location" in options) {
            headers.set("Location", String(options.location));
        } else {
            body = options.message ?? STATUS_TEXT[status];
            headers.set("Content-Type", "text/plain");
        }
        super(body, { ...options.response, status, headers });
    }
}
