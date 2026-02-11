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
 * if the type is unsupported or the adapter constructor throws.
 *
 * @param config - Validated connector configuration.
 * @returns The instantiated adapter or an error.
 */
export function createDatabaseAdapter(
	config: ConnectorConfig,
): Result<DatabaseAdapter, AdapterError> {
	try {
		switch (config.type) {
			case "postgres":
				return Ok(
					new PostgresAdapter({
						connectionString: config.postgres.connectionString,
					}),
				);
			case "mysql":
				return Ok(
					new MySQLAdapter({
						connectionString: config.mysql.connectionString,
					}),
				);
			case "bigquery":
				return Ok(
					new BigQueryAdapter({
						projectId: config.bigquery.projectId,
						dataset: config.bigquery.dataset,
						keyFilename: config.bigquery.keyFilename,
						location: config.bigquery.location,
					}),
				);
			case "jira":
			case "salesforce":
				return Err(
					new AdapterError(`Connector type "${config.type}" does not use a DatabaseAdapter`),
				);
		}
	} catch (err: unknown) {
		return Err(new AdapterError(`Failed to create adapter: ${toError(err).message}`));
	}
}
