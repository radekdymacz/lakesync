/** Supported connector types. */
export const CONNECTOR_TYPES = ["postgres", "mysql", "bigquery"] as const;

/** Union of supported connector type strings. */
export type ConnectorType = (typeof CONNECTOR_TYPES)[number];

/** Connection configuration for a PostgreSQL source. */
export interface PostgresConnectorConfig {
	/** PostgreSQL connection string (e.g. "postgres://user:pass@host/db"). */
	connectionString: string;
}

/** Connection configuration for a MySQL source. */
export interface MySQLConnectorConfig {
	/** MySQL connection string (e.g. "mysql://user:pass@host/db"). */
	connectionString: string;
}

/** Connection configuration for a BigQuery source. */
export interface BigQueryConnectorConfig {
	/** GCP project ID. */
	projectId: string;
	/** BigQuery dataset name. */
	dataset: string;
	/** Path to service account JSON key file. Falls back to ADC when omitted. */
	keyFilename?: string;
	/** Dataset location (default "US"). */
	location?: string;
}

/** Ingest table configuration — defines a single table to poll. */
export interface ConnectorIngestTable {
	/** Target table name in LakeSync. */
	table: string;
	/** SQL query to poll (must return rowId + data columns). */
	query: string;
	/** Primary key column name (default "id"). */
	rowIdColumn?: string;
	/** Change detection strategy. */
	strategy: { type: "cursor"; cursorColumn: string; lookbackMs?: number } | { type: "diff" };
}

/** Optional ingest polling configuration attached to a connector. */
export interface ConnectorIngestConfig {
	/** Tables to poll for changes. */
	tables: ConnectorIngestTable[];
	/** Poll interval in milliseconds (default 10 000). */
	intervalMs?: number;
}

/**
 * Configuration for a dynamically registered connector (data source).
 *
 * Each connector maps to a named {@link DatabaseAdapter} in the gateway,
 * optionally with an ingest poller that pushes detected changes into
 * the sync buffer.
 */
export interface ConnectorConfig {
	/** Unique connector name (used as source adapter key). */
	name: string;
	/** Connector type — determines which adapter implementation to instantiate. */
	type: ConnectorType;
	/** PostgreSQL connection configuration (required when type is "postgres"). */
	postgres?: PostgresConnectorConfig;
	/** MySQL connection configuration (required when type is "mysql"). */
	mysql?: MySQLConnectorConfig;
	/** BigQuery connection configuration (required when type is "bigquery"). */
	bigquery?: BigQueryConnectorConfig;
	/** Optional ingest polling configuration. */
	ingest?: ConnectorIngestConfig;
}
