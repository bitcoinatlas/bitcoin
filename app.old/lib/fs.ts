export async function readFileExact(file: Deno.FsFile, bytes: Uint8Array): Promise<void> {
	let totalRead = 0;
	while (totalRead < bytes.length) {
		const nread = await file.read(bytes.subarray(totalRead));
		if (nread === null) {
			throw new Error("Unexpected end of file");
		}
		totalRead += nread;
	}
}

export function readFileExactSync(file: Deno.FsFile, bytes: Uint8Array): void {
	let totalRead = 0;
	while (totalRead < bytes.length) {
		const nread = file.readSync(bytes.subarray(totalRead));
		if (nread === null) {
			throw new Error("Unexpected end of file");
		}
		totalRead += nread;
	}
}
