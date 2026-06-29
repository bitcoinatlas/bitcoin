import { BytesCodec, Codec, VarInt } from "@nomadshiba/codec";

// TODO: known pattern based optimization
export type StoredScriptPubKey = Codec.InferOutput<typeof StoredScriptPubKey>;
export const StoredScriptPubKey = new BytesCodec({ sizer: VarInt });
