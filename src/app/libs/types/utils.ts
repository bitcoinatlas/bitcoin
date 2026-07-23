export type OptionalizeEmpty<T> =
	& {
		[
			K in keyof T as [keyof T[K]] extends [never] ? never : (string extends keyof T[K] ? never
				: Record<keyof T[K], undefined> extends T[K] ? never
				: K)
		]: T[K];
	}
	& { [K in keyof T]?: T[K] };
