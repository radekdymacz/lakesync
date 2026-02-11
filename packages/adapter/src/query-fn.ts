import type { ConnectorConfig } from "@lakesync/core";

/** Generic query function — abstracts any SQL database connection. */
export type QueryFn = (sql: string, params?: unknown[]) => Promise<Record<string, unknown>[]>;

/**
 * Create a raw SQL query function from a {@link ConnectorConfig}.
 *
 * Uses dynamic imports so the database drivers (pg, mysql2) are only
 * loaded when actually needed. Returns `null` for connector types that
 * do not support the standard SQL polling model (e.g. BigQuery, Jira, Salesforce).
 *
 * @param config - Validated connector configuration.
 * @returns A query function or `null` if the connector type is unsupported.
 */
export async function createQueryFn(config: ConnectorConfig): Promise<QueryFn | null> {
	switch (config.type) {
		case "postgres": {
			const { Pool } = await import("pg");
			const pool = new Pool({ connectionString: config.postgres.connectionString });
			return async (sql: string, params?: unknown[]) => {
				const result = await pool.query(sql, params);
				return result.rows as Record<string, unknown>[];
			};
		}
		case "mysql": {
			const mysql = await import("mysql2/promise");
			const pool = mysql.createPool(config.mysql.connectionString);
			return async (sql: string, params?: unknown[]) => {
				const [rows] = await pool.query(sql, params);
				return rows as Record<string, unknown>[];
			};
		}
		default:
			return null;
	}
}
