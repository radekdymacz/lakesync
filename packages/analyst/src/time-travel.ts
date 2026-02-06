import type { HLCTimestamp } from "@lakesync/core";
import { Err, LakeSyncError, Ok, type Result } from "@lakesync/core";
import type { DuckDBClient } from "./duckdb";

/** System columns present in every delta Parquet file. */
const SYSTEM_COLUMNS = new Set(["op", "table", "rowId", "clientId", "hlc", "deltaId"]);

/**
 * Configuration for the TimeTraveller.
 */
export interface TimeTravelConfig {
	/** The DuckDB client used for executing time-travel queries. */
	duckdb: DuckDBClient;
}

/**
 * Provides time-travel query capabilities over delta Parquet files.
 *
 * Allows querying the materialised state of data as it existed at a specific
 * HLC timestamp, or inspecting raw deltas within a time range. Uses DuckDB
 * SQL with window functions to perform column-level LWW materialisation
 * entirely in-engine.
 *
 * Delta Parquet files contain flattened rows with system columns (`op`, `table`,
 * `rowId`, `clientId`, `hlc`, `deltaId`) and user-defined columns (e.g. `title`,
 * `completed`). The materialisation reconstructs per-row state by selecting the
 * latest value for each column based on HLC ordering, then excluding deleted rows.
 *
 * @example
 * ```ts
 * const traveller = new TimeTraveller({ duckdb: client });
 * await traveller.registerDeltas([{ name: "batch-1.parquet", data: bytes }]);
 *
 * const result = await traveller.queryAsOf(hlcTimestamp, "SELECT * FROM _state WHERE completed = true");
 * if (result.ok) console.log(result.value);
 * ```
 */
export class TimeTraveller {
	private readonly _config: TimeTravelConfig;
	private readonly _sources: string[] = [];

	constructor(config: TimeTravelConfig) {
		this._config = config;
	}

	/**
	 * Register one or more Parquet buffers containing delta data.
	 *
	 * Each buffer is registered with DuckDB and can subsequently be
	 * queried via the time-travel methods.
	 *
	 * @param parquetBuffers - Array of named Parquet file buffers to register
	 * @returns A Result indicating success or a LakeSyncError on failure
	 */
	async registerDeltas(
		parquetBuffers: Array<{ name: string; data: Uint8Array }>,
	): Promise<Result<void, LakeSyncError>> {
		try {
			for (const buf of parquetBuffers) {
				const regResult = await this._config.duckdb.registerParquetBuffer(buf.name, buf.data);
				if (!regResult.ok) {
					return regResult;
				}
				this._sources.push(buf.name);
			}
			return Ok(undefined);
		} catch (err) {
			const cause = err instanceof Error ? err : new Error(String(err));
			return Err(
				new LakeSyncError(
					`Failed to register delta data: ${cause.message}`,
					"ANALYST_ERROR",
					cause,
				),
			);
		}
	}

	/**
	 * Query the materialised state as of the given HLC timestamp.
	 *
	 * Filters all deltas where `hlc <= asOfHlc`, then materialises the latest
	 * state per (table, rowId) using column-level LWW (highest HLC wins per
	 * column). The user's SQL is applied on top of the materialised view,
	 * which is exposed as the CTE `_state`.
	 *
	 * Deleted rows (where the latest operation is DELETE) are excluded from
	 * the materialised view.
	 *
	 * @param asOfHlc - The HLC timestamp representing the point in time to query
	 * @param sql - SQL to apply on the materialised view (use `_state` as the table name)
	 * @returns A Result containing the query results or a LakeSyncError
	 */
	async queryAsOf(
		asOfHlc: HLCTimestamp,
		sql: string,
	): Promise<Result<Record<string, unknown>[], LakeSyncError>> {
		try {
			if (this._sources.length === 0) {
				return Ok([]);
			}

			const userColumns = await this._discoverUserColumns();
			if (!userColumns.ok) {
				return Err(userColumns.error);
			}

			const materialiseSql = this._buildMaterialiseSql(userColumns.value, BigInt(asOfHlc));
			const finalSql = `WITH _state AS (${materialiseSql}) ${sql}`;

			const result = await this._config.duckdb.query<Record<string, unknown>>(finalSql);
			if (!result.ok) {
				return Err(result.error);
			}
			return Ok(result.value);
		} catch (err) {
			const cause = err instanceof Error ? err : new Error(String(err));
			return Err(
				new LakeSyncError(`Time-travel queryAsOf failed: ${cause.message}`, "ANALYST_ERROR", cause),
			);
		}
	}

	/**
	 * Query raw deltas within a time range.
	 *
	 * Filters deltas where `fromHlc < hlc <= toHlc` and returns them as raw
	 * (unmaterialised) rows. Useful for audit trails and changelog views.
	 *
	 * The user's SQL is applied on top of the filtered deltas, which are
	 * exposed as the CTE `_deltas`.
	 *
	 * @param fromHlc - The exclusive lower bound HLC timestamp
	 * @param toHlc - The inclusive upper bound HLC timestamp
	 * @param sql - SQL to apply on the filtered deltas (use `_deltas` as the table name)
	 * @returns A Result containing the query results or a LakeSyncError
	 */
	async queryBetween(
		fromHlc: HLCTimestamp,
		toHlc: HLCTimestamp,
		sql: string,
	): Promise<Result<Record<string, unknown>[], LakeSyncError>> {
		try {
			if (this._sources.length === 0) {
				return Ok([]);
			}

			const unionSql = this._buildUnionSql();
			const fromBigint = BigInt(fromHlc);
			const toBigint = BigInt(toHlc);

			const filteredSql = `
				SELECT * FROM (${unionSql}) AS _all
				WHERE CAST(hlc AS BIGINT) > ${fromBigint}
				  AND CAST(hlc AS BIGINT) <= ${toBigint}
			`;

			const finalSql = `WITH _deltas AS (${filteredSql}) ${sql}`;

			const result = await this._config.duckdb.query<Record<string, unknown>>(finalSql);
			if (!result.ok) {
				return Err(result.error);
			}
			return Ok(result.value);
		} catch (err) {
			const cause = err instanceof Error ? err : new Error(String(err));
			return Err(
				new LakeSyncError(
					`Time-travel queryBetween failed: ${cause.message}`,
					"ANALYST_ERROR",
					cause,
				),
			);
		}
	}

	/**
	 * Materialise the full state at a point in time, returning all rows.
	 *
	 * Equivalent to `queryAsOf(asOfHlc, "SELECT * FROM _state")` but provided
	 * as a convenience method.
	 *
	 * @param asOfHlc - The HLC timestamp representing the point in time to materialise
	 * @returns A Result containing all materialised rows or a LakeSyncError
	 */
	async materialiseAsOf(
		asOfHlc: HLCTimestamp,
	): Promise<Result<Record<string, unknown>[], LakeSyncError>> {
		return this.queryAsOf(asOfHlc, "SELECT * FROM _state");
	}

	/**
	 * Build a UNION ALL SQL expression covering all registered Parquet sources.
	 */
	private _buildUnionSql(): string {
		return this._sources.map((s) => `SELECT * FROM '${s}'`).join(" UNION ALL BY NAME ");
	}

	/**
	 * Discover user-defined column names from the registered Parquet data.
	 *
	 * Reads the column names from the first registered source and filters
	 * out system columns to identify user-defined columns.
	 */
	private async _discoverUserColumns(): Promise<Result<string[], LakeSyncError>> {
		if (this._sources.length === 0) {
			return Ok([]);
		}

		const result = await this._config.duckdb.query<{ column_name: string }>(
			`SELECT column_name FROM (DESCRIBE SELECT * FROM '${this._sources[0]}')`,
		);
		if (!result.ok) {
			return Err(result.error);
		}

		const userCols = result.value
			.map((r) => r.column_name)
			.filter((name) => !SYSTEM_COLUMNS.has(name));

		return Ok(userCols);
	}

	/**
	 * Build the materialisation SQL that reconstructs per-row state using
	 * column-level LWW semantics.
	 *
	 * Strategy:
	 * 1. Filter deltas by HLC <= asOfHlc
	 * 2. For each (table, rowId), determine the latest operation (by max HLC)
	 * 3. For each user column in each row, pick the value from the delta with
	 *    the highest HLC (where that column is not null)
	 * 4. Exclude rows where the latest operation is DELETE
	 *
	 * @param userColumns - Names of user-defined columns
	 * @param asOfHlc - The HLC timestamp cutoff as a bigint
	 * @returns SQL string producing the materialised view
	 */
	private _buildMaterialiseSql(userColumns: string[], asOfHlc: bigint): string {
		const unionSql = this._buildUnionSql();

		// If there are no user columns, we still need to return rowId and table
		if (userColumns.length === 0) {
			return `
				SELECT "table", "rowId"
				FROM (
					SELECT *,
						ROW_NUMBER() OVER (
							PARTITION BY "table", "rowId"
							ORDER BY CAST(hlc AS BIGINT) DESC
						) AS _rn
					FROM (${unionSql}) AS _all
					WHERE CAST(hlc AS BIGINT) <= ${asOfHlc}
				) AS _ranked
				WHERE _rn = 1 AND op != 'DELETE'
			`;
		}

		// Build per-column LAST_VALUE expressions.
		// For each user column, we want the value from the row with the highest HLC
		// where that column is NOT NULL. We use LAST_VALUE with IGNORE NULLS over
		// an HLC-ordered window partition.
		const columnSelects = userColumns.map((col) => {
			return `LAST_VALUE("${col}" IGNORE NULLS) OVER (
				PARTITION BY "table", "rowId"
				ORDER BY CAST(hlc AS BIGINT) ASC
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			) AS "${col}"`;
		});

		// We need the latest op per (table, rowId) to filter out DELETEs.
		// Use LAST_VALUE on op ordered by HLC to get the most recent operation.
		const latestOpExpr = `LAST_VALUE(op) OVER (
			PARTITION BY "table", "rowId"
			ORDER BY CAST(hlc AS BIGINT) ASC
			ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		) AS _latest_op`;

		// Use ROW_NUMBER to deduplicate — we only need one output row per (table, rowId).
		const rowNumExpr = `ROW_NUMBER() OVER (
			PARTITION BY "table", "rowId"
			ORDER BY CAST(hlc AS BIGINT) DESC
		) AS _rn`;

		return `
			SELECT "table", "rowId", ${userColumns.map((c) => `"${c}"`).join(", ")}
			FROM (
				SELECT
					"table",
					"rowId",
					${columnSelects.join(",\n\t\t\t\t\t")},
					${latestOpExpr},
					${rowNumExpr}
				FROM (${unionSql}) AS _all
				WHERE CAST(hlc AS BIGINT) <= ${asOfHlc}
			) AS _materialised
			WHERE _rn = 1 AND _latest_op != 'DELETE'
		`;
	}
}
