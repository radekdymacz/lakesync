export type { ActionDescriptor, ActionHandler, AuthContext } from "./action-handler";
export { isActionHandler } from "./action-handler";
export { ConnectorValidationError } from "./errors";
export type { ConnectorCategory, ConnectorDescriptor } from "./registry";
export {
	getConnectorDescriptor,
	listConnectorDescriptors,
	registerConnectorDescriptor,
	registerOutputSchemas,
} from "./registry";
export type {
	BigQueryConnectorConfig,
	ConnectorConfig,
	ConnectorIngestConfig,
	ConnectorIngestTable,
	ConnectorType,
	JiraConnectorConfig,
	MySQLConnectorConfig,
	PostgresConnectorConfig,
	SalesforceConnectorConfig,
} from "./types";
export { CONNECTOR_TYPES } from "./types";
export { validateConnectorConfig } from "./validate";

// Auto-register built-in connector descriptors.
import "./register-builtin";
