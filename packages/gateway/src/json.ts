/**
 * BigInt-safe JSON replacer.
 *
 * Converts BigInt values to strings so they survive `JSON.stringify`,
 * which otherwise throws on BigInt.
 */
export function bigintReplacer(_key: string, value: unknown): unknown {
	return typeof value === "bigint" ? value.toString() : value;
}

/**
 * BigInt-aware JSON reviver.
 *
 * Restores string-encoded HLC timestamps (fields ending in `Hlc` or `hlc`)
 * back to BigInt so they match the branded `HLCTimestamp` type.
 */
export function bigintReviver(key: string, value: unknown): unknown {
	if (typeof value === "string" && /hlc$/i.test(key)) {
		return BigInt(value);
	}
	return value;
}
