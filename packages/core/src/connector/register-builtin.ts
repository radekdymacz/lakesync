// ---------------------------------------------------------------------------
// Built-in connector descriptor registration (side-effect module)
// ---------------------------------------------------------------------------

import { registerConnectorDescriptor } from "./registry";
import {
	API_INGEST_SCHEMA,
	BIGQUERY_CONFIG_SCHEMA,
	DATABASE_INGEST_SCHEMA,
	JIRA_CONFIG_SCHEMA,
	MYSQL_CONFIG_SCHEMA,
	POSTGRES_CONFIG_SCHEMA,
	SALESFORCE_CONFIG_SCHEMA,
} from "./schemas/index";

registerConnectorDescriptor({
	type: "bigquery",
	displayName: "BigQuery",
	description: "Google BigQuery data warehouse connector.",
	category: "database",
	configSchema: BIGQUERY_CONFIG_SCHEMA,
	ingestSchema: DATABASE_INGEST_SCHEMA,
	outputTables: null,
});

registerConnectorDescriptor({
	type: "jira",
	displayName: "Jira",
	description: "Atlassian Jira Cloud issue tracker connector.",
	category: "api",
	configSchema: JIRA_CONFIG_SCHEMA,
	ingestSchema: API_INGEST_SCHEMA,
	outputTables: null,
});

registerConnectorDescriptor({
	type: "mysql",
	displayName: "MySQL",
	description: "MySQL relational database connector.",
	category: "database",
	configSchema: MYSQL_CONFIG_SCHEMA,
	ingestSchema: DATABASE_INGEST_SCHEMA,
	outputTables: null,
});

registerConnectorDescriptor({
	type: "postgres",
	displayName: "PostgreSQL",
	description: "PostgreSQL relational database connector.",
	category: "database",
	configSchema: POSTGRES_CONFIG_SCHEMA,
	ingestSchema: DATABASE_INGEST_SCHEMA,
	outputTables: null,
});

registerConnectorDescriptor({
	type: "salesforce",
	displayName: "Salesforce",
	description: "Salesforce CRM connector for accounts, contacts, opportunities, and leads.",
	category: "api",
	configSchema: SALESFORCE_CONFIG_SCHEMA,
	ingestSchema: API_INGEST_SCHEMA,
	outputTables: null,
});
