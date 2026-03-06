import { u48LE } from "~/lib/codec/primitives.ts";

// Global pointer to anything stored on the chain
export type StoredPointer = number;

export const storedPointer = u48LE;
