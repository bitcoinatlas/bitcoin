export const MiB = 1024 ** 2;
export const GiB = 1024 ** 3;

export const WITNESS_DATA_WEIGHT = 1;
export const NON_WITNESS_DATA_WEIGHT = 4;
export const MAX_BLOCK_SIZE = 4 * 1000 * 1000;
export const MAX_BLOCK_WEIGHT = 4 * 1000 * 1000;
export const MAX_NON_WITNESS_BLOCK_SIZE = MAX_BLOCK_SIZE / NON_WITNESS_DATA_WEIGHT;

export const MAX_SCRIPT_SIZE = 10_000; // look into it SCRIPT_ERR_SCRIPT_SIZE

export const COINBASE_TXID = new Uint8Array(32);
export const COINBASE_VOUT = 0xFFFFFFFF;

// TODO: Later keep this inside the workers, tell it to workers while initilizing them.
// this way we can terminate workers and restart them when settings changed.
export const PARALLELISM = navigator.hardwareConcurrency;
