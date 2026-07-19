import { sha256 } from "@noble/hashes/sha2";
import { createHasher, Hash, type Input } from "@noble/hashes/utils";

export class SHA256D extends Hash<SHA256D> {
	public override blockLen = 64;
	public override outputLen = 32;
	protected pass1 = sha256.create();
	protected pass2 = sha256.create();

	public override update(buf: Input): this {
		this.pass1.update(buf);
		return this;
	}

	public override digestInto(out: Uint8Array): void {
		this.pass1.digestInto(out);
		this.pass2.update(out.length === this.outputLen ? out : out.subarray(0, this.outputLen));
		this.pass2.digestInto(out);
		this.destroy();
	}

	public override digest(): Uint8Array {
		const out = new Uint8Array(this.outputLen);
		this.digestInto(out);
		return out;
	}

	public override destroy(): void {
		this.pass1.destroy();
		this.pass2.destroy();
	}

	public override _cloneInto(to?: SHA256D): SHA256D {
		to ??= new SHA256D();
		this.pass1._cloneInto(to.pass1);
		this.pass2._cloneInto(to.pass2);
		return to;
	}

	override clone(): SHA256D {
		return this._cloneInto();
	}
}

export const sha256d = createHasher(() => new SHA256D());
