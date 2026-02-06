import { Err, LakeSyncError, Ok, type Result } from "@lakesync/core";
import type { DuckDBClient } from "./duckdb";

/**
 * Configuration for the UnionReader.
 */
export interface UnionReadConfig {
	/** The DuckDB client used to query cold (Parquet) data. */
	duckdb: DuckDBClient;
	/** The logical table name being queried. */
	tableName: string;
}

/**
 * Merges "hot" in-memory rows with "cold" Parquet data via DuckDB.
 *
 * Cold data is registered as Parquet file buffers in DuckDB. Hot data is
 * serialised to JSON and loaded via `read_json_auto`. The two sources are
 * combined with `UNION ALL` and the caller's SQL is applied on top.
 *
 * The union is exposed as a CTE named `_union`, so the caller's SQL should
 * reference `_union` as the table name.
 *
 * @example
 * ```ts
 * const reader = new UnionReader({ duckdb: client, tableName: "todos" });
 * await reader.registerColdData([{ name: "batch-1.parquet", data: parquetBytes }]);
 *
 * const result = await reader.query(
 *   "SELECT * FROM _union WHERE completed = true",
 *   [{ id: "row-3", title: "New task", completed: false }],
 * );
 * ```
 */
export class UnionReader {
	private readonly _config: UnionReadConfig;
	private readonly _coldSources: string[] = [];
	private _hotCounter = 0;

	constructor(config: UnionReadConfig) {
		this._config = config;
	}

	/**
	 * Register one or more Parquet buffers as cold data sources.
	 *
	 * Each buffer is registered with DuckDB and can subsequently be
	 * queried alongside hot data via {@link query}.
	 *
	 * @param parquetBuffers - Array of named Parquet file buffers to register
	 * @returns A Result indicating success or a LakeSyncError on failure
	 */
	async registerColdData(
		parquetBuffers: Array<{ name: string; data: Uint8Array }>,
	): Promise<Result<void, LakeSyncError>> {
		try {
			for (const buf of parquetBuffers) {
				const regResult = await this._config.duckdb.registerParquetBuffer(buf.name, buf.data);
				if (!regResult.ok) {
					return regResult;
				}
				this._coldSources.push(buf.name);
			}
			return Ok(undefined);
		} catch (err) {
			const cause = err instanceof Error ? err : new Error(String(err));
			return Err(
				new LakeSyncError(`Failed to register cold data: ${cause.message}`, "ANALYST_ERROR", cause),
			);
		}
	}

	/**
	 * Execute a SQL query that unions hot in-memory rows with cold Parquet data.
	 *
	 * The caller's SQL is wrapped around a UNION ALL of cold and hot sources.
	 * The unioned data is available as `_union` in the SQL statement.
	 *
	 * If `hotRows` is empty or not provided, only cold data is queried.
	 * If no cold sources are registered and `hotRows` is provided, only hot data is queried.
	 *
	 * @param sql - SQL to apply on top of the unioned data (use `_union` as the table name)
	 * @param hotRows - Optional array of in-memory row objects to include in the union
	 * @returns A Result containing the query results or a LakeSyncError
	 */
	async query(
		sql: string,
		hotRows?: Record<string, unknown>[],
	): Promise<Result<Record<string, unknown>[], LakeSyncError>> {
		try {
			const hasHot = hotRows !== undefined && hotRows.length > 0;
			const hasCold = this._coldSources.length > 0;

			if (!hasHot && !hasCold) {
				return Ok([]);
			}

			// Build the inner union query parts
			const unionParts: string[] = [];

			// Add cold sources (each Parquet file is a separate SELECT)
			if (hasCold) {
				for (const source of this._coldSources) {
					unionParts.push(`SELECT * FROM '${source}'`);
				}
			}

			// Add hot source via JSON buffer registered with DuckDB
			if (hasHot) {
				const hotBufferName = `_hot_${this._config.tableName}_${this._hotCounter++}.json`;
				const jsonBytes = new TextEncoder().encode(JSON.stringify(hotRows));

				const regResult = await this._config.duckdb.registerParquetBuffer(hotBufferName, jsonBytes);
				if (!regResult.ok) {
					return Err(regResult.error);
				}

				unionParts.push(`SELECT * FROM read_json_auto('${hotBufferName}')`);
			}

			// Combine all parts with UNION ALL and expose as CTE named `_union`
			// Use UNION ALL BY NAME so columns are matched by name, not position.
			// This handles differing column orders between Parquet and JSON sources.
			const unionSql = unionParts.join(" UNION ALL BY NAME ");
			const finalSql = `WITH _union AS (${unionSql}) ${sql}`;

			const result = await this._config.duckdb.query<Record<string, unknown>>(finalSql);
			if (!result.ok) {
				return Err(result.error);
			}

			return Ok(result.value);
		} catch (err) {
			const cause = err instanceof Error ? err : new Error(String(err));
			return Err(new LakeSyncError(`Union query failed: ${cause.message}`, "ANALYST_ERROR", cause));
		}
	}

	/**
	 * Query only cold (Parquet) data without any hot rows.
	 *
	 * @param sql - SQL to execute against cold data (use `_union` as the table name)
	 * @returns A Result containing the query results or a LakeSyncError
	 */
	async queryColdOnly(sql: string): Promise<Result<Record<string, unknown>[], LakeSyncError>> {
		return this.query(sql);
	}
}
