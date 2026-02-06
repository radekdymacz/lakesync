import { Err, LakeSyncError, Ok, type Result } from "@lakesync/core";

/**
 * Configuration options for the DuckDB-Wasm client.
 */
export interface DuckDBClientConfig {
	/** Whether to enable console logging from DuckDB. Defaults to false. */
	logger?: boolean;
	/** Maximum number of threads for DuckDB. Defaults to 1. */
	threads?: number;
}

/**
 * Wrapper around DuckDB-Wasm that provides a simplified, Result-based API
 * for executing SQL queries and registering Parquet data sources.
 *
 * Works in both Node.js/Bun (using the blocking bindings) and browser
 * environments (using the async worker-based bindings).
 *
 * @example
 * ```ts
 * const client = new DuckDBClient({ logger: false });
 * const initResult = await client.init();
 * if (!initResult.ok) { console.error(initResult.error); return; }
 *
 * const result = await client.query<{ answer: number }>("SELECT 42 AS answer");
 * if (result.ok) console.log(result.value); // [{ answer: 42 }]
 *
 * await client.close();
 * ```
 */
export class DuckDBClient {
	private readonly _config: DuckDBClientConfig;
	/* eslint-disable @typescript-eslint/no-explicit-any */
	private _db: {
		registerFileBuffer(name: string, buffer: Uint8Array): void;
		registerFileURL(name: string, url: string, proto: number, directIO: boolean): void;
	} | null = null;
	private _conn: {
		query<T>(text: string): { toArray(): T[] };
		close(): void;
	} | null = null;
	private _closed = false;

	constructor(config?: DuckDBClientConfig) {
		this._config = config ?? {};
	}

	/**
	 * Initialise the DuckDB-Wasm instance and open a connection.
	 *
	 * Uses the blocking Node.js bindings when running in Node/Bun,
	 * which avoids the need for Worker threads.
	 *
	 * @returns A Result indicating success or failure with a LakeSyncError
	 */
	async init(): Promise<Result<void, LakeSyncError>> {
		try {
			// Dynamic import of the blocking Node bindings
			const duckdb = await import("@duckdb/duckdb-wasm/blocking");
			const path = await import("node:path");
			const { createRequire } = await import("node:module");
			// Resolve WASM bundle paths relative to the duckdb-wasm package.
			// We resolve via the exported blocking entry point and walk up to the
			// package root, since `./package.json` is not in the exports map.
			const require = createRequire(import.meta.url);
			const blockingEntry = require.resolve("@duckdb/duckdb-wasm/dist/duckdb-node-blocking.cjs");
			const duckdbPkgPath = path.dirname(path.dirname(blockingEntry));

			const bundles: {
				mvp: { mainModule: string; mainWorker: string };
				eh?: { mainModule: string; mainWorker: string };
			} = {
				mvp: {
					mainModule: path.join(duckdbPkgPath, "dist", "duckdb-mvp.wasm"),
					mainWorker: path.join(duckdbPkgPath, "dist", "duckdb-node-mvp.worker.cjs"),
				},
				eh: {
					mainModule: path.join(duckdbPkgPath, "dist", "duckdb-eh.wasm"),
					mainWorker: path.join(duckdbPkgPath, "dist", "duckdb-node-eh.worker.cjs"),
				},
			};

			const logger = this._config.logger ? new duckdb.ConsoleLogger() : new duckdb.VoidLogger();

			const db = await duckdb.createDuckDB(bundles, logger, duckdb.NODE_RUNTIME);
			await db.instantiate(() => {});

			if (this._config.threads !== undefined) {
				db.open({ maximumThreads: this._config.threads });
			}

			const conn = db.connect();

			// Store references using structural typing (avoids importing concrete types)
			this._db = db as unknown as typeof this._db;
			this._conn = conn as unknown as typeof this._conn;
			this._closed = false;

			return Ok(undefined);
		} catch (err) {
			const cause = err instanceof Error ? err : new Error(String(err));
			return Err(
				new LakeSyncError(
					`Failed to initialise DuckDB-Wasm: ${cause.message}`,
					"ANALYST_ERROR",
					cause,
				),
			);
		}
	}

	/**
	 * Execute a SQL query and return the results as an array of objects.
	 *
	 * @param sql - The SQL statement to execute
	 * @param _params - Reserved for future use (parameterised queries)
	 * @returns A Result containing the query results or a LakeSyncError
	 */
	async query<T>(sql: string, _params?: unknown[]): Promise<Result<T[], LakeSyncError>> {
		try {
			if (this._closed || !this._conn) {
				return Err(
					new LakeSyncError(
						"Cannot query: DuckDB connection is closed or not initialised",
						"ANALYST_ERROR",
					),
				);
			}

			const arrowTable = this._conn.query<T>(sql);
			const rows = arrowTable.toArray();

			// Convert Arrow StructRow proxies to plain JS objects
			const plainRows = rows.map((row) => ({ ...row })) as T[];
			return Ok(plainRows);
		} catch (err) {
			const cause = err instanceof Error ? err : new Error(String(err));
			return Err(
				new LakeSyncError(`DuckDB query failed: ${cause.message}`, "ANALYST_ERROR", cause),
			);
		}
	}

	/**
	 * Register an in-memory Parquet file as a named table that can be
	 * queried using `SELECT * FROM '<name>'`.
	 *
	 * @param name - The virtual file name (e.g. "deltas.parquet")
	 * @param data - The Parquet file contents as a Uint8Array
	 * @returns A Result indicating success or failure
	 */
	async registerParquetBuffer(
		name: string,
		data: Uint8Array,
	): Promise<Result<void, LakeSyncError>> {
		try {
			if (this._closed || !this._db) {
				return Err(
					new LakeSyncError(
						"Cannot register Parquet buffer: DuckDB is closed or not initialised",
						"ANALYST_ERROR",
					),
				);
			}

			this._db.registerFileBuffer(name, data);
			return Ok(undefined);
		} catch (err) {
			const cause = err instanceof Error ? err : new Error(String(err));
			return Err(
				new LakeSyncError(
					`Failed to register Parquet buffer "${name}": ${cause.message}`,
					"ANALYST_ERROR",
					cause,
				),
			);
		}
	}

	/**
	 * Register a remote Parquet file by URL so it can be queried using
	 * `SELECT * FROM '<name>'`.
	 *
	 * @param name - The virtual file name (e.g. "remote.parquet")
	 * @param url - The URL pointing to the Parquet file
	 * @returns A Result indicating success or failure
	 */
	async registerParquetUrl(name: string, url: string): Promise<Result<void, LakeSyncError>> {
		try {
			if (this._closed || !this._db) {
				return Err(
					new LakeSyncError(
						"Cannot register Parquet URL: DuckDB is closed or not initialised",
						"ANALYST_ERROR",
					),
				);
			}

			// DuckDBDataProtocol.HTTP = 1
			const HTTP_PROTOCOL = 1;
			this._db.registerFileURL(name, url, HTTP_PROTOCOL, false);
			return Ok(undefined);
		} catch (err) {
			const cause = err instanceof Error ? err : new Error(String(err));
			return Err(
				new LakeSyncError(
					`Failed to register Parquet URL "${name}": ${cause.message}`,
					"ANALYST_ERROR",
					cause,
				),
			);
		}
	}

	/**
	 * Tear down the DuckDB connection and database instance.
	 *
	 * After calling close(), any subsequent query or registration calls
	 * will return an error Result.
	 */
	async close(): Promise<void> {
		if (this._conn) {
			try {
				this._conn.close();
			} catch {
				// Ignore errors during cleanup
			}
			this._conn = null;
		}
		this._db = null;
		this._closed = true;
	}
}
