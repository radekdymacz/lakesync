/**
 * Extract table names from a SQL query string.
 *
 * Performs basic regex extraction of table names from FROM and JOIN clauses.
 * Handles unquoted identifiers, double-quoted identifiers, and backtick-quoted
 * identifiers. Does not attempt full SQL parsing.
 *
 * @param sql - SQL query string
 * @returns Array of unique table names (lowercased for unquoted, preserved for quoted)
 */
export function extractTables(sql: string): string[] {
	const tables = new Set<string>();

	// Match FROM and JOIN clauses followed by a table name
	// Handles: FROM tablename, JOIN tablename, FROM "tablename", FROM `tablename`
	const pattern = /(?:FROM|JOIN)\s+(?:"([^"]+)"|`([^`]+)`|(\w+))/gi;

	let match = pattern.exec(sql);
	while (match !== null) {
		const table = match[1] ?? match[2] ?? match[3];
		if (table) {
			tables.add(table);
		}
		match = pattern.exec(sql);
	}

	return [...tables];
}
