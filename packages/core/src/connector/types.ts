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

/**
 * Configuration for a dynamically registered connector (data source).
 *
 * Discriminated union keyed on `type` — each variant carries exactly
 * its own connection config. No optional fields from other types.
 */
export type ConnectorConfig =
	| {
			type: "postgres";
			name: string;
			postgres: PostgresConnectorConfig;
			ingest?: ConnectorIngestConfig;
	  }
	| { type: "mysql"; name: string; mysql: MySQLConnectorConfig; ingest?: ConnectorIngestConfig }
	| {
			type: "bigquery";
			name: string;
			bigquery: BigQueryConnectorConfig;
			ingest?: ConnectorIngestConfig;
	  }
	| { type: "jira"; name: string; jira: JiraConnectorConfig; ingest?: ConnectorIngestConfig }
	| {
			type: "salesforce";
			name: string;
			salesforce: SalesforceConnectorConfig;
			ingest?: ConnectorIngestConfig;
	  };
