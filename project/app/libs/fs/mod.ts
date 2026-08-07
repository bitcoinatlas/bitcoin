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

export function readFileSync(file: Deno.FsFile, length: number): Uint8Array {
	const data = new Uint8Array(length);
	let bytesRead = 0;
	while (bytesRead < length) {
		const n = file.readSync(data.subarray(bytesRead));
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

export function readFileIntoSync(file: Deno.FsFile, target: Uint8Array): void {
	let bytesRead = 0;
	while (bytesRead < target.length) {
		const n = file.readSync(target.subarray(bytesRead));
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

export function writeFileSync(file: Deno.FsFile, data: Uint8Array): void {
	let bytesWritten = 0;
	while (bytesWritten < data.length) {
		const n = file.writeSync(data.subarray(bytesWritten));
		if (n === 0) {
			throw new Error("Failed to write to file");
		}
		bytesWritten += n;
	}
}

/**
 * Remove a path, treating "already gone" as success. Deno has no `force`
 * option: `removeSync` throws NotFound on a missing path, and `recursive`
 * only enables removing non-empty dirs — it does NOT swallow NotFound
 * (that's shell `rm -f`, not `rm -r`). Callers that speculatively remove
 * variants that usually don't exist (archive/tmp/lock sidecars) need this.
 */
export function rm(path: string): void {
	try {
		Deno.removeSync(path, { recursive: true });
	} catch (error) {
		if (!(error instanceof Deno.errors.NotFound)) throw error;
	}
}
