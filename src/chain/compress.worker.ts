// One last general-purpose compression pass to compress and increase the entropy density of the remaining data
// Applied on txs blobstore chunks
import zlib from "node:zlib";
import { readFileSync, writeFileSync } from "node:fs";

const { constants } = zlib;

function compressFile(src: string, dst: string) {
	const input = readFileSync(src);
	const output = zlib.zstdCompressSync(input, {
		params: {
			[constants.ZSTD_c_compressionLevel]: 19,
			[constants.ZSTD_c_enableLongDistanceMatching]: 1,
			[constants.ZSTD_c_windowLog]: 27,
			[constants.ZSTD_c_checksumFlag]: 1, // 4-byte frame checksum, cheap integrity guard
			[constants.ZSTD_c_contentSizeFlag]: 1, // size in frame header — works on the sync path
			[constants.ZSTD_c_nbWorkers]: 4, // in-process analog of CLI -T0, IF the build supports it
		},
	});
	writeFileSync(dst, output);
}

function decompressFile(src: string, dst: string) {
	const input = readFileSync(src);
	const output = zlib.zstdDecompressSync(input, {
		params: {
			[constants.ZSTD_c_compressionLevel]: 19,
			[constants.ZSTD_c_enableLongDistanceMatching]: 1,
			[constants.ZSTD_c_windowLog]: 27,
			[constants.ZSTD_c_checksumFlag]: 1, // 4-byte frame checksum, cheap integrity guard
			[constants.ZSTD_c_contentSizeFlag]: 1, // size in frame header — works on the sync path
			[constants.ZSTD_c_nbWorkers]: 4, // in-process analog of CLI -T0, IF the build supports it
		},
	});
	writeFileSync(dst, output);
}
