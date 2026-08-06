import { BytesCodec, Codec, VarInt } from "@nomadshiba/codec";

// TODO: known pattern based optimization
export type StoredPubKey = Codec.InferOutput<typeof StoredPubKey>;
export const StoredPubKey = new BytesCodec({ sizer: VarInt });
