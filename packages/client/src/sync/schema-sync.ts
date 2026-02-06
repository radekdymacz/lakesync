import {
	Err,
	type LakeSyncError,
	Ok,
	type Result,
	SchemaError,
	type TableSchema,
} from "@lakesync/core";
import type { LocalDB } from "../db/local-db";
import { getSchema, migrateSchema } from "../db/schema-registry";
import type { DbError } from "../db/types";

/**
 * Synchronises local table schemas with server-provided schema versions.
 *
 * Compares the locally stored schema version against the server's version
 * and applies additive migrations (ALTER TABLE ... ADD COLUMN) when the
 * client is behind.
 */
export class SchemaSynchroniser {
	constructor(private readonly db: LocalDB) {}

	/**
	 * Compare local schema version with server and apply migrations if behind.
	 *
	 * If the local version is already equal to or ahead of the server version,
	 * this is a no-op. Otherwise, the local schema is migrated to match the
	 * server schema via `migrateSchema()`, which runs ALTER TABLE ... ADD COLUMN
	 * for each new column.
	 *
	 * @param table - The table name to synchronise
	 * @param serverSchema - The server's current TableSchema
	 * @param serverVersion - The server's schema version number
	 * @returns Ok on success, or Err with a LakeSyncError on failure
	 */
	async synchronise(
		table: string,
		serverSchema: TableSchema,
		serverVersion: number,
	): Promise<Result<void, LakeSyncError>> {
		// Fetch the current local schema via getSchema (ensures _lakesync_meta exists)
		const localSchemaResult = await getSchema(this.db, table);
		if (!localSchemaResult.ok) return localSchemaResult;

		const localSchema = localSchemaResult.value;

		// If no local schema is registered, we cannot migrate
		if (!localSchema) {
			return Err(
				new SchemaError(
					`Cannot synchronise schema for table "${table}": no local schema registered`,
				),
			);
		}

		// Fetch the local schema version from _lakesync_meta
		const localVersionResult = await this.getLocalVersion(table);
		if (!localVersionResult.ok) return localVersionResult;

		const localVersion = localVersionResult.value;

		// If local is already at or ahead of server, nothing to do
		if (localVersion >= serverVersion) {
			return Ok(undefined);
		}

		// Migrate the schema (diffs and runs ALTER TABLE for new columns)
		const migrateResult = await migrateSchema(this.db, localSchema, serverSchema);
		if (!migrateResult.ok) return migrateResult;

		// Update the version in _lakesync_meta to match the server version.
		// migrateSchema() increments by 1, but we need to set the exact server version
		// in case the server is more than one version ahead.
		const updateResult = await this.setLocalVersion(table, serverVersion);
		if (!updateResult.ok) return updateResult;

		return Ok(undefined);
	}

	/**
	 * Retrieve the local schema version for a given table from `_lakesync_meta`.
	 *
	 * @param table - The table name to look up
	 * @returns The schema version number, or 0 if the table is not registered
	 */
	private async getLocalVersion(table: string): Promise<Result<number, DbError>> {
		const result = await this.db.query<{ schema_version: number }>(
			"SELECT schema_version FROM _lakesync_meta WHERE table_name = ?",
			[table],
		);

		if (!result.ok) return result;

		const rows = result.value;
		if (rows.length === 0 || !rows[0]) {
			return Ok(0);
		}

		return Ok(rows[0].schema_version);
	}

	/**
	 * Set the local schema version for a given table in `_lakesync_meta`.
	 *
	 * @param table - The table name to update
	 * @param version - The version number to set
	 * @returns Ok on success, or Err with a DbError on failure
	 */
	private async setLocalVersion(table: string, version: number): Promise<Result<void, DbError>> {
		return this.db.exec("UPDATE _lakesync_meta SET schema_version = ? WHERE table_name = ?", [
			version,
			table,
		]);
	}
}
