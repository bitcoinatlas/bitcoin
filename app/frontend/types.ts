import { Builder, Sync } from "@purifyjs/core";

export type BuilderOrNode<T extends Node> = Builder<T> | T;
export type SyncOrValue<T> = Sync<T> | T;
