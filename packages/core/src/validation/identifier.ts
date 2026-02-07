import { SchemaError } from "../result/errors";
import { Err, Ok, type Result } from "../result/result";

/** Valid SQL identifier: starts with letter or underscore, alphanumeric + underscore, max 64 chars. */
const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]{0,63}$/;

/**
 * Check whether a string is a valid SQL identifier.
 *
 * Valid identifiers start with a letter or underscore, contain only
 * alphanumeric characters and underscores, and are at most 64 characters long.
 *
 * @param name - The identifier to validate
 * @returns `true` if valid, `false` otherwise
 */
export function isValidIdentifier(name: string): boolean {
	return IDENTIFIER_RE.test(name);
}

/**
 * Assert that a string is a valid SQL identifier, returning a Result.
 *
 * @param name - The identifier to validate
 * @returns Ok(undefined) if valid, Err(SchemaError) if invalid
 */
export function assertValidIdentifier(name: string): Result<void, SchemaError> {
	if (isValidIdentifier(name)) {
		return Ok(undefined);
	}
	return Err(
		new SchemaError(
			`Invalid SQL identifier: "${name}". Identifiers must start with a letter or underscore, contain only alphanumeric characters and underscores, and be at most 64 characters long.`,
		),
	);
}

/**
 * Quote a SQL identifier using double quotes as defence-in-depth.
 *
 * Any embedded double-quote characters are escaped by doubling them,
 * following the SQL standard for delimited identifiers.
 *
 * @param name - The identifier to quote
 * @returns The double-quoted identifier string
 */
export function quoteIdentifier(name: string): string {
	return `"${name.replace(/"/g, '""')}"`;
}
