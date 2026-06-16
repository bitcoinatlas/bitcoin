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

export async function readFileInto(file: Deno.FsFile, target: Uint8Array): Promise<void> {
	let bytesRead = 0;
	while (bytesRead < target.length) {
		const n = await file.read(target.subarray(bytesRead));
		if (n === null) {
			throw new Error("Unexpected end of file");
		}
		bytesRead += n;
	}
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
