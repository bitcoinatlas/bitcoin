const DEFAULT_BASE = 1000;
const DEFAULT_UNITS = ["", "K", "M", "G", "T", "P", "E", "Z"] as const;

export type BigNumberFormatOptions = {
	base?: number;
	units?: readonly string[];
	separator?: string;
	minimumFractionDigits?: Intl.NumberFormatOptions["minimumFractionDigits"];
	maximumFractionDigits?: Intl.NumberFormatOptions["maximumFractionDigits"];
};

export class BigNumberFormat {
	private readonly base: number;
	private readonly units: readonly string[];
	private readonly separator: string;
	private readonly numberFormat: Intl.NumberFormat;

	public constructor(locale?: Intl.LocalesArgument, options?: BigNumberFormatOptions) {
		this.base = options?.base ?? DEFAULT_BASE;
		this.units = options?.units ?? DEFAULT_UNITS;
		this.separator = options?.separator ?? " ";
		this.numberFormat = new Intl.NumberFormat(locale, {
			maximumFractionDigits: options?.maximumFractionDigits ?? 2,
			minimumFractionDigits: options?.minimumFractionDigits,
		});
	}

	public format(value: number): string {
		if (!Number.isFinite(value)) return "—";
		let i = 0;
		while (Math.abs(value) >= this.base && i < this.units.length - 1) {
			value /= this.base;
			i++;
		}
		const unit = this.units[i];
		return `${this.numberFormat.format(value)}${unit ? this.separator : ""}${unit}`;
	}
}
