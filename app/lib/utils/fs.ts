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

export async function readFile(file: Deno.FsFile, length: number): Promise<Uint8Array> {
	const data = new Uint8Array(length);
	let bytesRead = 0;
	while (bytesRead < length) {
		const n = await file.read(data.subarray(bytesRead));
		if (n === null) {
			throw new Error("Unexpected end of file");
		}
		bytesRead += n;
	}

	return data;
}

export async function writeFile(file: Deno.FsFile, data: Uint8Array): Promise<void> {
	let bytesWritten = 0;
	while (bytesWritten < data.length) {
		const n = await file.write(data.subarray(bytesWritten));
		if (n === 0) {
			throw new Error("Failed to write to file");
		}
		bytesWritten += n;
	}
}
