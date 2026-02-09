import { LakeSyncError } from "@lakesync/core";

/** HTTP error from the Salesforce REST API. */
export class SalesforceApiError extends LakeSyncError {
	/** HTTP status code returned by Salesforce. */
	readonly statusCode: number;
	/** Raw response body from Salesforce. */
	readonly responseBody: string;

	constructor(statusCode: number, responseBody: string, cause?: Error) {
		super(`Salesforce API error (${statusCode}): ${responseBody}`, "SALESFORCE_API_ERROR", cause);
		this.statusCode = statusCode;
		this.responseBody = responseBody;
	}
}

/** Authentication failure from the Salesforce OAuth token endpoint. */
export class SalesforceAuthError extends LakeSyncError {
	constructor(message: string, cause?: Error) {
		super(message, "SALESFORCE_AUTH_ERROR", cause);
	}
}
