import { Codec } from "@nomadshiba/codec";

export class NullableNumaric<T extends Codec<number>> extends Codec<Codec.InferOutput<T> | null, Codec.InferInput<T> | null> {
	public override stride: T["stride"];
	private readonly numaric: T;

	public constructor(numaric: T) {
		super();
		this.numaric = numaric;
		this.stride = numaric.stride;
	}

	public override encoder(value: Codec.InferInput<T> | null, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public override encoder(value: Codec.InferInput<T> | null, target: Uint8Array, offset: number): number;
	public override encoder(value: Codec.InferInput<T> | null, target: any, offset: any): number | Uint8Array<ArrayBuffer> {
		if (value === null) return this.numaric.encoder(0, target, offset);
		return this.numaric.encoder(value + 1, target, offset);
	}
	public override decoder(data: Uint8Array, offset: number): [Codec.InferOutput<T> | null, number] {
		const [output, size] = this.numaric.decoder(data, offset);
		if (!output) return [null, size];
		return [output - 1, size];
	}
}
