import {
	assertValidIdentifier,
	Err,
	Ok,
	quoteIdentifier,
	type Result,
	SchemaError,
	type TableSchema,
	unwrapOrThrow,
} from "@lakesync/core";
import type { LocalDB } from "./local-db";
import { DbError } from "./types";

/** SQL column type mapping from LakeSync column types */
const COLUMN_TYPE_MAP: Record<TableSchema["columns"][number]["type"], string> = {
	string: "TEXT",
	number: "REAL",
	boolean: "INTEGER",
	json: "TEXT",
	null: "TEXT",
};

/**
 * Ensures the `_lakesync_meta` table exists in the database.
 * This table stores schema metadata for all registered tables.
 *
 * @param db - The LocalDB instance to initialise the meta table in
 * @returns A Result indicating success or a DbError
 */
async function ensureMetaTable(db: LocalDB): Promise<Result<void, DbError>> {
	return db.exec(`
		CREATE TABLE IF NOT EXISTS _lakesync_meta (
			table_name TEXT PRIMARY KEY,
			schema_version INTEGER NOT NULL DEFAULT 1,
			schema_json TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`);
}

/**
 * Registers a table schema in the local database.
 *
 * Creates the `_lakesync_meta` metadata table if it does not already exist,
 * then inserts or updates the schema entry and creates the corresponding
 * user table with columns derived from the provided TableSchema.
 *
 * The user table always includes a `_rowId TEXT PRIMARY KEY` column in
 * addition to the columns specified in the schema.
 *
 * This operation is idempotent — calling it twice with the same schema
 * produces no error.
 *
 * @param db - The LocalDB instance to register the schema in
 * @param schema - The TableSchema describing the table and its columns
 * @returns A Result indicating success or a DbError
 */
export async function registerSchema(
	db: LocalDB,
	schema: TableSchema,
): Promise<Result<void, DbError | SchemaError>> {
	const tableCheck = assertValidIdentifier(schema.table);
	if (!tableCheck.ok) return tableCheck;
	for (const col of schema.columns) {
		const colCheck = assertValidIdentifier(col.name);
		if (!colCheck.ok) return colCheck;
	}

	const metaResult = await ensureMetaTable(db);
	if (!metaResult.ok) return metaResult;

	return db.transaction((tx) => {
		const now = new Date().toISOString();
		const schemaJson = JSON.stringify(schema);

		// Insert or update schema metadata
		unwrapOrThrow(
			tx.exec(
				`INSERT INTO _lakesync_meta (table_name, schema_version, schema_json, updated_at)
				 VALUES (?, 1, ?, ?)
				 ON CONFLICT(table_name) DO UPDATE SET
				   schema_json = excluded.schema_json,
				   updated_at = excluded.updated_at`,
				[schema.table, schemaJson, now],
			),
		);

		// Build the CREATE TABLE statement with _rowId as primary key
		const quotedTable = quoteIdentifier(schema.table);
		const columnDefs = schema.columns
			.map((col) => `${quoteIdentifier(col.name)} ${COLUMN_TYPE_MAP[col.type]}`)
			.join(", ");

		const createSql = columnDefs
			? `CREATE TABLE IF NOT EXISTS ${quotedTable} (_rowId TEXT PRIMARY KEY, ${columnDefs})`
			: `CREATE TABLE IF NOT EXISTS ${quotedTable} (_rowId TEXT PRIMARY KEY)`;

		unwrapOrThrow(tx.exec(createSql));
	});
}

/**
 * Retrieves a previously registered table schema from the database.
 *
 * Ensures the `_lakesync_meta` table exists before querying, so this
 * function is safe to call even on a freshly created database.
 *
 * @param db - The LocalDB instance to query
 * @param table - The table name to look up
 * @returns A Result containing the TableSchema if found, null if the
 *          table has not been registered, or a DbError on failure
 */
export async function getSchema(
	db: LocalDB,
	table: string,
): Promise<Result<TableSchema | null, DbError>> {
	const metaResult = await ensureMetaTable(db);
	if (!metaResult.ok) return metaResult;

	const queryResult = await db.query<{ schema_json: string }>(
		"SELECT schema_json FROM _lakesync_meta WHERE table_name = ?",
		[table],
	);

	if (!queryResult.ok) return queryResult;

	const firstRow = queryResult.value[0];
	if (!firstRow) {
		return Ok(null);
	}

	try {
		const schema = JSON.parse(firstRow.schema_json) as TableSchema;
		return Ok(schema);
	} catch (err) {
		return Err(
			new DbError(
				`Failed to parse schema JSON for table "${table}"`,
				err instanceof Error ? err : undefined,
			),
		);
	}
}

/**
 * Migrates an existing table schema to a new version.
 *
 * Only additive migrations are supported — new columns can be added
 * (they will be nullable). Removing columns or changing column types
 * will result in a SchemaError.
 *
 * Both schemas must reference the same table name; mismatched table
 * names will produce a SchemaError.
 *
 * On success, the `_lakesync_meta` entry is updated with the new schema
 * and the schema version is incremented by one.
 *
 * @param db - The LocalDB instance to migrate
 * @param oldSchema - The current table schema
 * @param newSchema - The desired table schema after migration
 * @returns A Result indicating success, or a SchemaError/DbError on failure
 */
export async function migrateSchema(
	db: LocalDB,
	oldSchema: TableSchema,
	newSchema: TableSchema,
): Promise<Result<void, DbError | SchemaError>> {
	// Validate both schemas reference the same table
	if (oldSchema.table !== newSchema.table) {
		return Err(
			new SchemaError(
				`Table name mismatch: old schema references "${oldSchema.table}" but new schema references "${newSchema.table}"`,
			),
		);
	}

	const tableName = newSchema.table;

	const tableCheck = assertValidIdentifier(tableName);
	if (!tableCheck.ok) return tableCheck;
	for (const col of newSchema.columns) {
		const colCheck = assertValidIdentifier(col.name);
		if (!colCheck.ok) return colCheck;
	}

	// Build lookup maps for comparison
	const oldColumnMap = new Map<string, string>();
	for (const col of oldSchema.columns) {
		oldColumnMap.set(col.name, col.type);
	}

	const newColumnMap = new Map<string, string>();
	for (const col of newSchema.columns) {
		newColumnMap.set(col.name, col.type);
	}

	// Detect removed columns
	for (const col of oldSchema.columns) {
		if (!newColumnMap.has(col.name)) {
			return Err(
				new SchemaError(
					`Cannot remove column "${col.name}" from table "${tableName}". Only additive migrations are supported.`,
				),
			);
		}
	}

	// Detect type changes
	for (const col of newSchema.columns) {
		const oldType = oldColumnMap.get(col.name);
		if (oldType !== undefined && oldType !== col.type) {
			return Err(
				new SchemaError(
					`Cannot change type of column "${col.name}" in table "${tableName}" from "${oldType}" to "${col.type}". Type changes are not supported.`,
				),
			);
		}
	}

	// Find added columns
	const addedColumns = newSchema.columns.filter((col) => !oldColumnMap.has(col.name));

	const metaResult = await ensureMetaTable(db);
	if (!metaResult.ok) return metaResult;

	return db.transaction((tx) => {
		// Add new columns via ALTER TABLE
		const quotedTable = quoteIdentifier(tableName);
		for (const col of addedColumns) {
			unwrapOrThrow(
				tx.exec(
					`ALTER TABLE ${quotedTable} ADD COLUMN ${quoteIdentifier(col.name)} ${COLUMN_TYPE_MAP[col.type]}`,
				),
			);
		}

		// Update schema metadata with incremented version
		const now = new Date().toISOString();
		const schemaJson = JSON.stringify(newSchema);

		unwrapOrThrow(
			tx.exec(
				`UPDATE _lakesync_meta
				 SET schema_json = ?,
				     schema_version = schema_version + 1,
				     updated_at = ?
				 WHERE table_name = ?`,
				[schemaJson, now, tableName],
			),
		);
	});
}
