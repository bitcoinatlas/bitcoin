export const KiB = 1024 ** 1;
export const MiB = 1024 ** 2;
export const GiB = 1024 ** 3;

export const KB = 1000 ** 1;
export const MB = 1000 ** 2;
export const GB = 1000 ** 3;

// International Fixed Calendar
export const SECOND = 1000;
export const MINUTE = SECOND * 60;
export const HOUR = MINUTE * 60;
export const DAY = HOUR * 24;
export const WEEK = DAY * 7;
export const MONTH = WEEK * 4;
export const LEAP = DAY * 1.2425;
export const YEAR = MONTH * 13 + LEAP;

// Gregorian Calendar
export const SECOND_G = 1000;
export const MINUTE_G = SECOND_G * 60;
export const HOUR_G = MINUTE_G * 60;
export const DAY_G = HOUR_G * 24;
export const WEEK_G = DAY_G * 7;
export const MONTH_G = DAY_G * 30;
export const YEAR_G = DAY_G * 365;

export const WITNESS_DATA_WEIGHT = 1;
export const NON_WITNESS_DATA_WEIGHT = 4;
export const MAX_BLOCK_SIZE = 4 * MB;
export const MAX_BLOCK_WEIGHT = 4 * MB;
export const MAX_NON_WITNESS_BLOCK_SIZE = MAX_BLOCK_SIZE / NON_WITNESS_DATA_WEIGHT;

export const MAX_SCRIPT_SIZE = 10 * KB; // look into it SCRIPT_ERR_SCRIPT_SIZE

export const COINBASE_TXID = new Uint8Array(32);
export const COINBASE_VOUT = 0xFFFFFFFF;
