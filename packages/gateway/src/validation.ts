import type {
	ActionPush,
	HLCTimestamp,
	ResolvedClaims,
	SyncPull,
	SyncPush,
	SyncRulesConfig,
	SyncRulesContext,
	TableSchema,
} from "@lakesync/core";
import { bigintReviver, Err, Ok, type Result } from "@lakesync/core";
import {
	DEFAULT_PULL_LIMIT,
	MAX_DELTAS_PER_PUSH,
	MAX_PULL_LIMIT,
	VALID_COLUMN_TYPES,
} from "./constants";

/** Validation error with HTTP status code. */
export interface RequestError {
	status: number;
	message: string;
}

/**
 * Validate and parse a push request body.
 * Handles JSON parsing with bigint revival.
 */
export function validatePushBody(
	raw: string,
	headerClientId?: string | null,
): Result<SyncPush, RequestError> {
	let body: SyncPush;
	try {
		body = JSON.parse(raw, bigintReviver) as SyncPush;
	} catch {
		return Err({ status: 400, message: "Invalid JSON body" });
	}

	if (!body.clientId || !Array.isArray(body.deltas)) {
		return Err({ status: 400, message: "Missing required fields: clientId, deltas" });
	}

	if (headerClientId && body.clientId !== headerClientId) {
		return Err({
			status: 403,
			message: "Client ID mismatch: push clientId does not match authenticated identity",
		});
	}

	if (body.deltas.length > MAX_DELTAS_PER_PUSH) {
		return Err({ status: 400, message: "Too many deltas in a single push (max 10,000)" });
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
		return Err({ status: 400, message: "Missing required query params: since, clientId" });
	}

	let sinceHlc: HLCTimestamp;
	try {
		sinceHlc = BigInt(params.since) as HLCTimestamp;
	} catch {
		return Err({
			status: 400,
			message: "Invalid 'since' parameter \u2014 must be a decimal integer",
		});
	}

	const rawLimit = params.limit ? Number.parseInt(params.limit, 10) : DEFAULT_PULL_LIMIT;
	if (Number.isNaN(rawLimit) || rawLimit < 1) {
		return Err({
			status: 400,
			message: "Invalid 'limit' parameter \u2014 must be a positive integer",
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
	let body: ActionPush;
	try {
		body = JSON.parse(raw, bigintReviver) as ActionPush;
	} catch {
		return Err({ status: 400, message: "Invalid JSON body" });
	}

	if (!body.clientId || !Array.isArray(body.actions)) {
		return Err({ status: 400, message: "Missing required fields: clientId, actions" });
	}

	if (headerClientId && body.clientId !== headerClientId) {
		return Err({
			status: 403,
			message: "Client ID mismatch: action clientId does not match authenticated identity",
		});
	}

	return Ok(body);
}

/**
 * Validate a table schema body.
 */
export function validateSchemaBody(raw: string): Result<TableSchema, RequestError> {
	let schema: TableSchema;
	try {
		schema = JSON.parse(raw) as TableSchema;
	} catch {
		return Err({ status: 400, message: "Invalid JSON body" });
	}

	if (!schema.table || !Array.isArray(schema.columns)) {
		return Err({ status: 400, message: "Missing required fields: table, columns" });
	}

	for (const col of schema.columns) {
		if (typeof col.name !== "string" || col.name.length === 0) {
			return Err({ status: 400, message: "Each column must have a non-empty 'name' string" });
		}
		if (!VALID_COLUMN_TYPES.has(col.type)) {
			return Err({
				status: 400,
				message: `Invalid column type "${col.type}" for column "${col.name}". Allowed: string, number, boolean, json, null`,
			});
		}
	}

	const columnNames = new Set(schema.columns.map((c) => c.name));

	// Validate primaryKey
	if (schema.primaryKey !== undefined) {
		if (!Array.isArray(schema.primaryKey) || schema.primaryKey.length === 0) {
			return Err({ status: 400, message: "primaryKey must be a non-empty array of strings" });
		}
		for (const pk of schema.primaryKey) {
			if (typeof pk !== "string") {
				return Err({ status: 400, message: "primaryKey must be a non-empty array of strings" });
			}
			if (pk !== "row_id" && !columnNames.has(pk)) {
				return Err({
					status: 400,
					message: `primaryKey column "${pk}" must be "row_id" or exist in columns`,
				});
			}
		}
	}

	// Validate softDelete
	if (schema.softDelete !== undefined && typeof schema.softDelete !== "boolean") {
		return Err({ status: 400, message: "softDelete must be a boolean" });
	}

	// Validate externalIdColumn
	if (schema.externalIdColumn !== undefined) {
		if (typeof schema.externalIdColumn !== "string" || schema.externalIdColumn.length === 0) {
			return Err({ status: 400, message: "externalIdColumn must be a non-empty string" });
		}
		if (!columnNames.has(schema.externalIdColumn)) {
			return Err({
				status: 400,
				message: `externalIdColumn "${schema.externalIdColumn}" must exist in columns`,
			});
		}
	}

	return Ok(schema);
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
