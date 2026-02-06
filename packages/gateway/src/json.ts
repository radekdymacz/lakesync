/**
 * BigInt-safe JSON replacer.
 *
 * Converts BigInt values to strings so they survive `JSON.stringify`,
 * which otherwise throws on BigInt.
 */
export function bigintReplacer(_key: string, value: unknown): unknown {
	return typeof value === "bigint" ? value.toString() : value;
}
