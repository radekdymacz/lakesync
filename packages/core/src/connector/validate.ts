import { Err, Ok, type Result } from "../result/result";
import { ConnectorValidationError } from "./errors";
import { CONNECTOR_TYPES, type ConnectorConfig, type ConnectorType } from "./types";

const VALID_STRATEGIES = new Set(["cursor", "diff"]);

// ---------------------------------------------------------------------------
// Per-type validators
// ---------------------------------------------------------------------------

function validatePostgresConfig(
	obj: Record<string, unknown>,
): Result<void, ConnectorValidationError> {
	const pg = obj.postgres;
	if (typeof pg !== "object" || pg === null) {
		return Err(
			new ConnectorValidationError('Connector type "postgres" requires a postgres config object'),
		);
	}
	const pgObj = pg as Record<string, unknown>;
	if (typeof pgObj.connectionString !== "string" || pgObj.connectionString.length === 0) {
		return Err(
			new ConnectorValidationError("Postgres connector requires a non-empty connectionString"),
		);
	}
	return Ok(undefined);
}

function validateMySQLConfig(obj: Record<string, unknown>): Result<void, ConnectorValidationError> {
	const my = obj.mysql;
	if (typeof my !== "object" || my === null) {
		return Err(
			new ConnectorValidationError('Connector type "mysql" requires a mysql config object'),
		);
	}
	const myObj = my as Record<string, unknown>;
	if (typeof myObj.connectionString !== "string" || myObj.connectionString.length === 0) {
		return Err(
			new ConnectorValidationError("MySQL connector requires a non-empty connectionString"),
		);
	}
	return Ok(undefined);
}

function validateBigQueryConfig(
	obj: Record<string, unknown>,
): Result<void, ConnectorValidationError> {
	const bq = obj.bigquery;
	if (typeof bq !== "object" || bq === null) {
		return Err(
			new ConnectorValidationError('Connector type "bigquery" requires a bigquery config object'),
		);
	}
	const bqObj = bq as Record<string, unknown>;
	if (typeof bqObj.projectId !== "string" || bqObj.projectId.length === 0) {
		return Err(new ConnectorValidationError("BigQuery connector requires a non-empty projectId"));
	}
	if (typeof bqObj.dataset !== "string" || bqObj.dataset.length === 0) {
		return Err(new ConnectorValidationError("BigQuery connector requires a non-empty dataset"));
	}
	return Ok(undefined);
}

function validateJiraConfig(obj: Record<string, unknown>): Result<void, ConnectorValidationError> {
	const jira = obj.jira;
	if (typeof jira !== "object" || jira === null) {
		return Err(new ConnectorValidationError('Connector type "jira" requires a jira config object'));
	}
	const jiraObj = jira as Record<string, unknown>;
	if (typeof jiraObj.domain !== "string" || jiraObj.domain.length === 0) {
		return Err(new ConnectorValidationError("Jira connector requires a non-empty domain"));
	}
	if (typeof jiraObj.email !== "string" || jiraObj.email.length === 0) {
		return Err(new ConnectorValidationError("Jira connector requires a non-empty email"));
	}
	if (typeof jiraObj.apiToken !== "string" || jiraObj.apiToken.length === 0) {
		return Err(new ConnectorValidationError("Jira connector requires a non-empty apiToken"));
	}
	return Ok(undefined);
}

function validateSalesforceConfig(
	obj: Record<string, unknown>,
): Result<void, ConnectorValidationError> {
	const sf = obj.salesforce;
	if (typeof sf !== "object" || sf === null) {
		return Err(
			new ConnectorValidationError(
				'Connector type "salesforce" requires a salesforce config object',
			),
		);
	}
	const sfObj = sf as Record<string, unknown>;
	if (typeof sfObj.instanceUrl !== "string" || sfObj.instanceUrl.length === 0) {
		return Err(
			new ConnectorValidationError("Salesforce connector requires a non-empty instanceUrl"),
		);
	}
	if (typeof sfObj.clientId !== "string" || sfObj.clientId.length === 0) {
		return Err(new ConnectorValidationError("Salesforce connector requires a non-empty clientId"));
	}
	if (typeof sfObj.clientSecret !== "string" || sfObj.clientSecret.length === 0) {
		return Err(
			new ConnectorValidationError("Salesforce connector requires a non-empty clientSecret"),
		);
	}
	if (typeof sfObj.username !== "string" || sfObj.username.length === 0) {
		return Err(new ConnectorValidationError("Salesforce connector requires a non-empty username"));
	}
	if (typeof sfObj.password !== "string" || sfObj.password.length === 0) {
		return Err(new ConnectorValidationError("Salesforce connector requires a non-empty password"));
	}
	return Ok(undefined);
}

// ---------------------------------------------------------------------------
// Ingest validation
// ---------------------------------------------------------------------------

function validateIngestConfig(
	obj: Record<string, unknown>,
	connectorType: string,
): Result<void, ConnectorValidationError> {
	if (obj.ingest === undefined) return Ok(undefined);

	if (typeof obj.ingest !== "object" || obj.ingest === null) {
		return Err(new ConnectorValidationError("Ingest config must be an object"));
	}

	const ingest = obj.ingest as Record<string, unknown>;

	// API-based connectors define tables internally — only validate intervalMs
	if (connectorType === "jira" || connectorType === "salesforce") {
		if (ingest.intervalMs !== undefined) {
			if (typeof ingest.intervalMs !== "number" || ingest.intervalMs < 1) {
				return Err(new ConnectorValidationError("Ingest intervalMs must be a positive number"));
			}
		}
		return Ok(undefined);
	}

	if (!Array.isArray(ingest.tables) || ingest.tables.length === 0) {
		return Err(new ConnectorValidationError("Ingest config must have a non-empty tables array"));
	}

	for (let i = 0; i < ingest.tables.length; i++) {
		const table = ingest.tables[i] as Record<string, unknown>;

		if (typeof table !== "object" || table === null) {
			return Err(new ConnectorValidationError(`Ingest table at index ${i} must be an object`));
		}

		if (typeof table.table !== "string" || (table.table as string).length === 0) {
			return Err(
				new ConnectorValidationError(`Ingest table at index ${i} must have a non-empty table name`),
			);
		}

		if (typeof table.query !== "string" || (table.query as string).length === 0) {
			return Err(
				new ConnectorValidationError(`Ingest table at index ${i} must have a non-empty query`),
			);
		}

		if (typeof table.strategy !== "object" || table.strategy === null) {
			return Err(
				new ConnectorValidationError(`Ingest table at index ${i} must have a strategy object`),
			);
		}

		const strategy = table.strategy as Record<string, unknown>;
		if (!VALID_STRATEGIES.has(strategy.type as string)) {
			return Err(
				new ConnectorValidationError(
					`Ingest table at index ${i} strategy type must be "cursor" or "diff"`,
				),
			);
		}

		if (strategy.type === "cursor") {
			if (
				typeof strategy.cursorColumn !== "string" ||
				(strategy.cursorColumn as string).length === 0
			) {
				return Err(
					new ConnectorValidationError(
						`Ingest table at index ${i} cursor strategy requires a non-empty cursorColumn`,
					),
				);
			}
		}
	}

	if (ingest.intervalMs !== undefined) {
		if (typeof ingest.intervalMs !== "number" || ingest.intervalMs < 1) {
			return Err(new ConnectorValidationError("Ingest intervalMs must be a positive number"));
		}
	}

	return Ok(undefined);
}

// ---------------------------------------------------------------------------
// Main validator
// ---------------------------------------------------------------------------

/**
 * Validate a connector configuration for structural correctness.
 *
 * Checks:
 * - `name` is a non-empty string
 * - `type` is one of the supported connector types
 * - Type-specific config object is present and valid
 * - Optional ingest config has valid table definitions
 *
 * @param input - Raw input to validate.
 * @returns The validated {@link ConnectorConfig} or a validation error.
 */
export function validateConnectorConfig(
	input: unknown,
): Result<ConnectorConfig, ConnectorValidationError> {
	if (typeof input !== "object" || input === null) {
		return Err(new ConnectorValidationError("Connector config must be an object"));
	}

	const obj = input as Record<string, unknown>;

	// --- name ---
	if (typeof obj.name !== "string" || obj.name.length === 0) {
		return Err(new ConnectorValidationError("Connector name must be a non-empty string"));
	}

	// --- type ---
	if (typeof obj.type !== "string" || obj.type.length === 0) {
		return Err(new ConnectorValidationError("Connector type must be a non-empty string"));
	}

	const connectorType = obj.type;

	// --- type-specific config for known types ---
	if ((CONNECTOR_TYPES as readonly string[]).includes(connectorType)) {
		const knownType = connectorType as ConnectorType;
		let typeResult: Result<void, ConnectorValidationError>;
		switch (knownType) {
			case "postgres":
				typeResult = validatePostgresConfig(obj);
				break;
			case "mysql":
				typeResult = validateMySQLConfig(obj);
				break;
			case "bigquery":
				typeResult = validateBigQueryConfig(obj);
				break;
			case "jira":
				typeResult = validateJiraConfig(obj);
				break;
			case "salesforce":
				typeResult = validateSalesforceConfig(obj);
				break;
		}
		if (!typeResult.ok) return typeResult;
	}

	// --- optional ingest config ---
	const ingestResult = validateIngestConfig(obj, connectorType);
	if (!ingestResult.ok) return ingestResult;

	return Ok(input as ConnectorConfig);
}
