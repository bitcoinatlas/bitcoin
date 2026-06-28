export const WITNESS_DATA_WEIGHT = 1;
export const NON_WITNESS_DATA_WEIGHT = 4;
export const MAX_BLOCK_SIZE = 4 * 1000 * 1000;
export const MAX_BLOCK_WEIGHT = 4 * 1000 * 1000;
export const MAX_NON_WITNESS_BLOCK_SIZE = MAX_BLOCK_SIZE / NON_WITNESS_DATA_WEIGHT;

export const MAX_SCRIPT_SIZE = 10_000; // look into it SCRIPT_ERR_SCRIPT_SIZE

export const COINBASE_TXID = new Uint8Array(32);
export const COINBASE_VOUT = 0xFFFFFFFF;
