// File system utilities

/**
 * Read exactly `buf.length` bytes from a file into the buffer.
 * Returns the number of bytes actually read.
 * Useful for reading complete blocks of data from files.
 */
export async function readFileFull(file: Deno.FsFile, buffer: Uint8Array): Promise<number> {
	let totalRead = 0;
	while (totalRead < buffer.length) {
		const n = await file.read(buffer.subarray(totalRead));
		if (n === null || n === 0) break;
		totalRead += n;
	}
	return totalRead;
}
