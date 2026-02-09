import { LakeSyncError } from "../result/errors";

/** Connector configuration validation error. */
export class ConnectorValidationError extends LakeSyncError {
	constructor(message: string, cause?: Error) {
		super(message, "CONNECTOR_VALIDATION", cause);
	}
}
