import { Codec } from "~/lib/codec/mod.ts";

export type PeerMessage<Self = any, Item = any> = Codec<Self, Item> & {
	command(self: Self): string;
};
