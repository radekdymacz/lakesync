export type { ActionDescriptor, ActionHandler, AuthContext } from "./action-handler";
export { isActionHandler } from "./action-handler";
export { ConnectorValidationError } from "./errors";
export type { ConnectorCategory, ConnectorDescriptor, ConnectorRegistry } from "./registry";
export {
	createConnectorRegistry,
	getConnectorDescriptor,
	listConnectorDescriptors,
	registerConnectorDescriptor,
	registerOutputSchemas,
} from "./registry";
export type {
	BigQueryConnectorConfig,
	BigQueryConnectorConfigFull,
	ConnectorConfig,
	ConnectorConfigBase,
	ConnectorIngestConfig,
	ConnectorIngestTable,
	ConnectorType,
	JiraConnectorConfig,
	JiraConnectorConfigFull,
	MySQLConnectorConfig,
	MySQLConnectorConfigFull,
	PostgresConnectorConfig,
	PostgresConnectorConfigFull,
	SalesforceConnectorConfig,
	SalesforceConnectorConfigFull,
} from "./types";
export { CONNECTOR_TYPES } from "./types";
export { validateConnectorConfig } from "./validate";

// Auto-register built-in connector descriptors.
import "./register-builtin";
