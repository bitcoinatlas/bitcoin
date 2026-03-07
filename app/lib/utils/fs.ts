export async function readFileFull(file: Deno.FsFile, buffer: Uint8Array): Promise<number> {
	let totalRead = 0;
	while (totalRead < buffer.length) {
		const n = await file.read(buffer.subarray(totalRead));
		if (n === null || n === 0) break;
		totalRead += n;
	}
	return totalRead;
}

export async function writeFileFull(file: Deno.FsFile, buffer: Uint8Array): Promise<number> {
	let totalWritten = 0;
	while (totalWritten < buffer.length) {
		const n = await file.write(buffer.subarray(totalWritten));
		if (n === 0) throw new Error("Failed to write to file");
		totalWritten += n;
	}
	return totalWritten;
}
