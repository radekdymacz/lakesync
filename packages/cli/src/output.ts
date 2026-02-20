/** Print a message to stdout. */
export function print(message: string): void {
	process.stdout.write(`${message}\n`);
}

/** Print an error to stderr and exit with code 1. */
export function fatal(message: string): never {
	process.stderr.write(`Error: ${message}\n`);
	process.exit(1);
}

/** Print a warning to stderr. */
export function warn(message: string): void {
	process.stderr.write(`Warning: ${message}\n`);
}

/** Print a key-value table to stdout. */
export function printTable(
	rows: Array<Record<string, string | number | boolean | undefined>>,
): void {
	if (rows.length === 0) {
		print("(none)");
		return;
	}

	const keys = Object.keys(rows[0]!);
	const widths = keys.map((key) =>
		Math.max(key.length, ...rows.map((row) => String(row[key] ?? "").length)),
	);

	// Header
	const header = keys.map((key, i) => key.padEnd(widths[i]!)).join("  ");
	print(header);
	print(widths.map((w) => "-".repeat(w)).join("  "));

	// Rows
	for (const row of rows) {
		const line = keys.map((key, i) => String(row[key] ?? "").padEnd(widths[i]!)).join("  ");
		print(line);
	}
}
