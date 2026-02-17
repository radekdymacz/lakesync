import type {
	ActionPush,
	ApiErrorCode,
	HLCTimestamp,
	ResolvedClaims,
	RowDelta,
	SyncPull,
	SyncPush,
	SyncRulesConfig,
	SyncRulesContext,
	TableSchema,
} from "@lakesync/core";
import {
	API_ERROR_CODES,
	assertValidIdentifier,
	bigintReviver,
	Err,
	isValidIdentifier,
	Ok,
	type Result,
	type SchemaError,
} from "@lakesync/core";
import {
	DEFAULT_PULL_LIMIT,
	MAX_DELTAS_PER_PUSH,
	MAX_PULL_LIMIT,
	VALID_COLUMN_TYPES,
} from "./constants";

/** Validation error with HTTP status code and structured error code. */
export interface RequestError {
	status: number;
	message: string;
	code: ApiErrorCode;
}

/** Parse a JSON string, returning Err on invalid JSON. */
export function parseJson<T>(
	raw: string,
	reviver?: (key: string, value: unknown) => unknown,
): Result<T, RequestError> {
	try {
		return Ok(JSON.parse(raw, reviver) as T);
	} catch {
		return Err({ status: 400, message: "Invalid JSON body", code: API_ERROR_CODES.VALIDATION_ERROR });
	}
}

/**
 * Validate and parse a push request body.
 * Handles JSON parsing with bigint revival.
 */
export function validatePushBody(
	raw: string,
	headerClientId?: string | null,
): Result<SyncPush, RequestError> {
	const parsed = parseJson<SyncPush>(raw, bigintReviver);
	if (!parsed.ok) return parsed;
	const body = parsed.value;

	if (!body.clientId || !Array.isArray(body.deltas)) {
		return Err({ status: 400, message: "Missing required fields: clientId, deltas", code: API_ERROR_CODES.VALIDATION_ERROR });
	}

	if (headerClientId && body.clientId !== headerClientId) {
		return Err({
			status: 403,
			message: "Client ID mismatch: push clientId does not match authenticated identity",
			code: API_ERROR_CODES.FORBIDDEN,
		});
	}

	if (body.deltas.length > MAX_DELTAS_PER_PUSH) {
		return Err({ status: 400, message: "Too many deltas in a single push (max 10,000)", code: API_ERROR_CODES.VALIDATION_ERROR });
	}

	return Ok(body);
}

/**
 * Parse and validate pull query parameters.
 */
export function parsePullParams(params: {
	since: string | null;
	clientId: string | null;
	limit: string | null;
	source: string | null;
}): Result<SyncPull, RequestError> {
	if (!params.since || !params.clientId) {
		return Err({ status: 400, message: "Missing required query params: since, clientId", code: API_ERROR_CODES.VALIDATION_ERROR });
	}

	let sinceHlc: HLCTimestamp;
	try {
		sinceHlc = BigInt(params.since) as HLCTimestamp;
	} catch {
		return Err({
			status: 400,
			message: "Invalid 'since' parameter \u2014 must be a decimal integer",
			code: API_ERROR_CODES.VALIDATION_ERROR,
		});
	}

	const rawLimit = params.limit ? Number.parseInt(params.limit, 10) : DEFAULT_PULL_LIMIT;
	if (Number.isNaN(rawLimit) || rawLimit < 1) {
		return Err({
			status: 400,
			message: "Invalid 'limit' parameter \u2014 must be a positive integer",
			code: API_ERROR_CODES.VALIDATION_ERROR,
		});
	}
	const maxDeltas = Math.min(rawLimit, MAX_PULL_LIMIT);

	const msg: SyncPull = {
		clientId: params.clientId,
		sinceHlc,
		maxDeltas,
		...(params.source ? { source: params.source } : {}),
	};

	return Ok(msg);
}

/**
 * Validate and parse an action request body.
 */
export function validateActionBody(
	raw: string,
	headerClientId?: string | null,
): Result<ActionPush, RequestError> {
	const parsed = parseJson<ActionPush>(raw, bigintReviver);
	if (!parsed.ok) return parsed;
	const body = parsed.value;

	if (!body.clientId || !Array.isArray(body.actions)) {
		return Err({ status: 400, message: "Missing required fields: clientId, actions", code: API_ERROR_CODES.VALIDATION_ERROR });
	}

	if (headerClientId && body.clientId !== headerClientId) {
		return Err({
			status: 403,
			message: "Client ID mismatch: action clientId does not match authenticated identity",
			code: API_ERROR_CODES.FORBIDDEN,
		});
	}

	return Ok(body);
}

/**
 * Validate a table schema body.
 */
export function validateSchemaBody(raw: string): Result<TableSchema, RequestError> {
	const parsed = parseJson<TableSchema>(raw);
	if (!parsed.ok) return parsed;
	const schema = parsed.value;

	if (!schema.table || !Array.isArray(schema.columns)) {
		return Err({ status: 400, message: "Missing required fields: table, columns", code: API_ERROR_CODES.SCHEMA_ERROR });
	}

	// Validate table name is a safe SQL identifier
	if (!isValidIdentifier(schema.table)) {
		return Err({
			status: 400,
			message: `Invalid table name: "${schema.table}". Identifiers must start with a letter or underscore, contain only alphanumeric characters and underscores, and be at most 64 characters long.`,
			code: API_ERROR_CODES.SCHEMA_ERROR,
		});
	}

	for (const col of schema.columns) {
		if (typeof col.name !== "string" || col.name.length === 0) {
			return Err({ status: 400, message: "Each column must have a non-empty 'name' string", code: API_ERROR_CODES.SCHEMA_ERROR });
		}
		// Validate column name is a safe SQL identifier
		if (!isValidIdentifier(col.name)) {
			return Err({
				status: 400,
				message: `Invalid column name: "${col.name}". Identifiers must start with a letter or underscore, contain only alphanumeric characters and underscores, and be at most 64 characters long.`,
				code: API_ERROR_CODES.SCHEMA_ERROR,
			});
		}
		if (!VALID_COLUMN_TYPES.has(col.type)) {
			return Err({
				status: 400,
				message: `Invalid column type "${col.type}" for column "${col.name}". Allowed: string, number, boolean, json, null`,
				code: API_ERROR_CODES.SCHEMA_ERROR,
			});
		}
	}

	const columnNames = new Set(schema.columns.map((c) => c.name));

	// Validate primaryKey
	if (schema.primaryKey !== undefined) {
		if (!Array.isArray(schema.primaryKey) || schema.primaryKey.length === 0) {
			return Err({ status: 400, message: "primaryKey must be a non-empty array of strings", code: API_ERROR_CODES.SCHEMA_ERROR });
		}
		for (const pk of schema.primaryKey) {
			if (typeof pk !== "string") {
				return Err({ status: 400, message: "primaryKey must be a non-empty array of strings", code: API_ERROR_CODES.SCHEMA_ERROR });
			}
			if (pk !== "row_id" && !columnNames.has(pk)) {
				return Err({
					status: 400,
					message: `primaryKey column "${pk}" must be "row_id" or exist in columns`,
					code: API_ERROR_CODES.SCHEMA_ERROR,
				});
			}
		}
	}

	// Validate softDelete
	if (schema.softDelete !== undefined && typeof schema.softDelete !== "boolean") {
		return Err({ status: 400, message: "softDelete must be a boolean", code: API_ERROR_CODES.SCHEMA_ERROR });
	}

	// Validate externalIdColumn
	if (schema.externalIdColumn !== undefined) {
		if (typeof schema.externalIdColumn !== "string" || schema.externalIdColumn.length === 0) {
			return Err({ status: 400, message: "externalIdColumn must be a non-empty string", code: API_ERROR_CODES.SCHEMA_ERROR });
		}
		if (!columnNames.has(schema.externalIdColumn)) {
			return Err({
				status: 400,
				message: `externalIdColumn "${schema.externalIdColumn}" must exist in columns`,
				code: API_ERROR_CODES.SCHEMA_ERROR,
			});
		}
	}

	return Ok(schema);
}

/**
 * Validate that a push delta's table name is a safe SQL identifier.
 *
 * Intended for use as a {@link DeltaValidator} in the gateway
 * {@link ValidationPipeline} — defence in depth against SQL injection
 * via crafted table names.
 */
export function validateDeltaTableName(
	delta: RowDelta,
): Result<void, SchemaError> {
	return assertValidIdentifier(delta.table);
}

/**
 * Map a gateway push error code to an HTTP status code.
 */
export function pushErrorToStatus(code: string): number {
	switch (code) {
		case "CLOCK_DRIFT":
			return 409;
		case "SCHEMA_MISMATCH":
			return 422;
		case "BACKPRESSURE":
			return 503;
		default:
			return 500;
	}
}

/**
 * Map a gateway push error code to an API error code.
 */
export function pushErrorToApiCode(code: string): ApiErrorCode {
	switch (code) {
		case "CLOCK_DRIFT":
			return API_ERROR_CODES.CLOCK_DRIFT;
		case "SCHEMA_MISMATCH":
			return API_ERROR_CODES.SCHEMA_ERROR;
		case "BACKPRESSURE":
			return API_ERROR_CODES.BACKPRESSURE_ERROR;
		default:
			return API_ERROR_CODES.INTERNAL_ERROR;
	}
}

/**
 * Build a SyncRulesContext from rules and claims.
 * Returns undefined when no rules or empty buckets.
 */
export function buildSyncRulesContext(
	rules: SyncRulesConfig | undefined,
	claims: ResolvedClaims,
): SyncRulesContext | undefined {
	if (!rules || rules.buckets.length === 0) {
		return undefined;
	}
	return { claims, rules };
}
