import { StringCodec } from "@nomadshiba/codec";
import { validate } from "@std/uuid";

export const UUID = new StringCodec({ size: 36 }).transform((candidate) => {
	if (!validate(candidate)) {
		throw new Error(`Invalid UUID, candidate=${candidate}`);
	}
	return candidate;
});
