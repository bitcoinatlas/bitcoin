export class BytesView<T extends ArrayBufferLike> extends DataView<T> {
	constructor(bytes: Uint8Array<T>, byteOffset?: number, byteLength?: number) {
		super(bytes.buffer, bytes.byteOffset + (byteOffset ?? 0), byteLength ?? bytes.byteLength);
	}
}
