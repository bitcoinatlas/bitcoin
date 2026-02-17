import { Codec } from "~/lib/codec/mod.ts";

export type PeerMessage<Self = any, T = any> = Codec<T> {
	command(): string;
};
