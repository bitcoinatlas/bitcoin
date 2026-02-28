export type ChainNode = Readonly<{
	header: Uint8Array;
	hash: Uint8Array;
	cumulativeWork: bigint;
}>;
