import { AdapterError, type ConnectorConfig, Err, Ok, type Result, toError } from "@lakesync/core";
import { BigQueryAdapter } from "./bigquery";
import type { DatabaseAdapter } from "./db-types";
import { MySQLAdapter } from "./mysql";
import { PostgresAdapter } from "./postgres";

/**
 * Instantiate a {@link DatabaseAdapter} from a {@link ConnectorConfig}.
 *
 * Switches on `config.type` and creates the matching adapter using
 * the type-specific connection configuration. Returns an {@link AdapterError}
 * if the type-specific config is missing or the adapter constructor throws.
 *
 * @param config - Validated connector configuration.
 * @returns The instantiated adapter or an error.
 */
export function createDatabaseAdapter(
	config: ConnectorConfig,
): Result<DatabaseAdapter, AdapterError> {
	try {
		switch (config.type) {
			case "postgres": {
				if (!config.postgres) {
					return Err(new AdapterError("Postgres connector config missing postgres field"));
				}
				return Ok(
					new PostgresAdapter({
						connectionString: config.postgres.connectionString,
					}),
				);
			}
			case "mysql": {
				if (!config.mysql) {
					return Err(new AdapterError("MySQL connector config missing mysql field"));
				}
				return Ok(
					new MySQLAdapter({
						connectionString: config.mysql.connectionString,
					}),
				);
			}
			case "bigquery": {
				if (!config.bigquery) {
					return Err(new AdapterError("BigQuery connector config missing bigquery field"));
				}
				return Ok(
					new BigQueryAdapter({
						projectId: config.bigquery.projectId,
						dataset: config.bigquery.dataset,
						keyFilename: config.bigquery.keyFilename,
						location: config.bigquery.location,
					}),
				);
			}
			default:
				return Err(new AdapterError(`Unsupported connector type: ${config.type}`));
		}
	} catch (err: unknown) {
		return Err(new AdapterError(`Failed to create adapter: ${toError(err).message}`));
	}
}
