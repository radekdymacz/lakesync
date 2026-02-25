import type { PushTarget } from "../polling/chunked-pusher";

/** Lifecycle handle for a running connector instance. */
export interface ConnectorLifecycle {
	/** Start the connector (polling, streaming, etc.). */
	start(): Promise<void>;
	/** Stop the connector and release resources. */
	stop(): Promise<void>;
	/** Whether the connector is currently running. */
	readonly isRunning: boolean;
}

/**
 * Factory function that creates a running connector from configuration.
 *
 * Returns a {@link ConnectorLifecycle} handle for start/stop control.
 * The factory receives a {@link PushTarget} so any connector can push
 * deltas into the gateway without coupling to `SyncGateway` directly.
 */
export type ConnectorFactory = (
	config: ConnectorConfig,
	target: PushTarget,
) => Promise<ConnectorLifecycle>;

/** Supported connector types. */
export const CONNECTOR_TYPES = ["postgres", "mysql", "bigquery", "jira", "salesforce"] as const;

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

/** Connection configuration for a Salesforce CRM source. */
export interface SalesforceConnectorConfig {
	/** Salesforce instance URL (e.g. "https://mycompany.salesforce.com"). */
	instanceUrl: string;
	/** Connected App consumer key. */
	clientId: string;
	/** Connected App consumer secret. */
	clientSecret: string;
	/** Salesforce username. */
	username: string;
	/** Salesforce password + security token concatenated. */
	password: string;
	/** REST API version (default "v62.0"). */
	apiVersion?: string;
	/** Use test.salesforce.com for auth (default false). */
	isSandbox?: boolean;
	/** Optional WHERE clause fragment appended to all SOQL queries. */
	soqlFilter?: string;
	/** Whether to include Account objects (default true). */
	includeAccounts?: boolean;
	/** Whether to include Contact objects (default true). */
	includeContacts?: boolean;
	/** Whether to include Opportunity objects (default true). */
	includeOpportunities?: boolean;
	/** Whether to include Lead objects (default true). */
	includeLeads?: boolean;
}

/** Connection configuration for a Jira Cloud source. */
export interface JiraConnectorConfig {
	/** Jira Cloud domain (e.g. "mycompany" for mycompany.atlassian.net). */
	domain: string;
	/** Email address for Basic auth. */
	email: string;
	/** API token paired with the email. */
	apiToken: string;
	/** Optional JQL filter to scope issue polling. */
	jql?: string;
	/** Whether to include comments (default true). */
	includeComments?: boolean;
	/** Whether to include projects (default true). */
	includeProjects?: boolean;
}

/** Optional ingest polling configuration attached to a connector. */
export interface ConnectorIngestConfig {
	/** Tables to poll for changes. */
	tables: ConnectorIngestTable[];
	/** Poll interval in milliseconds (default 10 000). */
	intervalMs?: number;
	/** Deltas per push chunk (default 500). */
	chunkSize?: number;
	/** Approximate memory budget in bytes — triggers flush at 70%. */
	memoryBudgetBytes?: number;
}

/** Base configuration shared by all connectors. */
export interface ConnectorConfigBase {
	/** Connector type identifier. */
	type: string;
	/** Unique connector name. */
	name: string;
	/** Optional ingest polling configuration. */
	ingest?: ConnectorIngestConfig;
}

/** Typed connector config for PostgreSQL. */
export interface PostgresConnectorConfigFull extends ConnectorConfigBase {
	type: "postgres";
	postgres: PostgresConnectorConfig;
}

/** Typed connector config for MySQL. */
export interface MySQLConnectorConfigFull extends ConnectorConfigBase {
	type: "mysql";
	mysql: MySQLConnectorConfig;
}

/** Typed connector config for BigQuery. */
export interface BigQueryConnectorConfigFull extends ConnectorConfigBase {
	type: "bigquery";
	bigquery: BigQueryConnectorConfig;
}

/** Typed connector config for Jira Cloud. */
export interface JiraConnectorConfigFull extends ConnectorConfigBase {
	type: "jira";
	jira: JiraConnectorConfig;
}

/** Typed connector config for Salesforce. */
export interface SalesforceConnectorConfigFull extends ConnectorConfigBase {
	type: "salesforce";
	salesforce: SalesforceConnectorConfig;
}

/**
 * Configuration for a dynamically registered connector (data source).
 *
 * Union of known connector configs plus an open base for extensibility.
 * Existing switch statements still work for known types; unknown types
 * can pass through via the open base.
 */
export type ConnectorConfig =
	| PostgresConnectorConfigFull
	| MySQLConnectorConfigFull
	| BigQueryConnectorConfigFull
	| JiraConnectorConfigFull
	| SalesforceConnectorConfigFull
	| (ConnectorConfigBase & Record<string, unknown>);
