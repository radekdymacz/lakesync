/** HLC timestamp: 64-bit value with [48-bit wall clock ms][16-bit logical counter] */
export type HLCTimestamp = bigint & { readonly __brand: "HLCTimestamp" };
