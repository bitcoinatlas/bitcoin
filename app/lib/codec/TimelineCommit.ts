import { ArrayCodec, Codec, StructCodec, u32LE } from "@nomadshiba/codec";
import { timelineEntry } from "~/lib/codec/TimelineEntry.ts";
import { bytes32, compactSize } from "~/lib/codec/primitives.ts";

export type TimelineCommit = Codec.Infer<typeof timelineCommit>;
export type TimelineCommitHeader = TimelineCommit["header"];
export const timelineCommit = new StructCodec({
	header: new StructCodec({
		version: u32LE,
		prevHash: bytes32,
		merkleRoot: bytes32,
		timestamp: u32LE,
		bits: u32LE,
		nonce: u32LE,
	}),
	entries: new ArrayCodec(timelineEntry, { countCodec: compactSize }),
});
