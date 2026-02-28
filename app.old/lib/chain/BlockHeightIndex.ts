import { HeightStore } from "./HeightIndex.ts";
import { StoredPointer } from "./old/StoredPointer.ts";

export class BlockHeightIndex extends HeightStore<StoredPointer> {
	constructor(path: string) {
		super(StoredPointer, path);
	}
}
