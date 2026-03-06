import { TimelineCommitHeader } from "./codec/TimelineCommit.ts";
import { TimelineEntry } from "./codec/TimelineEntry.ts";

export class Commit {
	header: TimelineCommitHeader;
	headerHash: Uint8Array;
	async entires(): Promise<TimelineEntry[] | undefined> {
	}
}

export class Timeline {
	commits: Commit[];
	verificationHeight: bigint;
	orderedDownloadHeight: bigint;
}
