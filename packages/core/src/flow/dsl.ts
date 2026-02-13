import { Err, Ok, type Result } from "../result/result";
import {
	FLOW_MATERIALISE_TYPES,
	FLOW_SOURCE_TYPES,
	FLOW_STORE_TYPES,
	type FlowConfig,
	FlowError,
	type FlowMaterialiseConfig,
	type FlowSourceConfig,
	type FlowStoreConfig,
} from "./types";

// ---------------------------------------------------------------------------
// parseFlows — parse a JSON string into validated FlowConfig[]
// ---------------------------------------------------------------------------

/**
 * Parse a JSON string containing one or more flow configurations.
 *
 * Accepts either a single object or an array. Each config is individually
 * validated.
 */
export function parseFlows(input: string): Result<FlowConfig[], FlowError> {
	let parsed: unknown;
	try {
		parsed = JSON.parse(input);
	} catch {
		return Err(new FlowError("Invalid JSON input", "INVALID_CONFIG"));
	}

	const items = Array.isArray(parsed) ? parsed : [parsed];
	const configs: FlowConfig[] = [];

	for (const item of items) {
		const result = parseFlowConfig(item);
		if (!result.ok) return result as Result<never, FlowError>;
		configs.push(result.value);
	}

	return Ok(configs);
}

// ---------------------------------------------------------------------------
// parseFlowConfig — parse and validate a single unknown value
// ---------------------------------------------------------------------------

/**
 * Parse a single unknown value into a validated {@link FlowConfig}.
 *
 * Performs structural type checking and delegates to {@link validateFlowConfig}
 * for semantic validation.
 */
export function parseFlowConfig(input: unknown): Result<FlowConfig, FlowError> {
	if (input === null || typeof input !== "object" || Array.isArray(input)) {
		return Err(new FlowError("Flow config must be a non-null object", "INVALID_CONFIG"));
	}

	const obj = input as Record<string, unknown>;

	// --- name ---
	if (typeof obj.name !== "string") {
		return Err(new FlowError("Flow config 'name' must be a string", "INVALID_CONFIG"));
	}

	// --- source ---
	if (obj.source === undefined || obj.source === null || typeof obj.source !== "object") {
		return Err(new FlowError("Flow config 'source' must be an object", "INVALID_CONFIG"));
	}

	const sourceResult = parseSource(obj.source as Record<string, unknown>);
	if (!sourceResult.ok) return sourceResult as Result<never, FlowError>;

	// --- store (optional) ---
	let store: FlowStoreConfig | undefined;
	if (obj.store !== undefined) {
		if (obj.store === null || typeof obj.store !== "object") {
			return Err(new FlowError("Flow config 'store' must be an object", "INVALID_CONFIG"));
		}
		const storeResult = parseStore(obj.store as Record<string, unknown>);
		if (!storeResult.ok) return storeResult as Result<never, FlowError>;
		store = storeResult.value;
	}

	// --- materialise (optional) ---
	let materialise: FlowMaterialiseConfig[] | undefined;
	if (obj.materialise !== undefined) {
		if (!Array.isArray(obj.materialise)) {
			return Err(new FlowError("Flow config 'materialise' must be an array", "INVALID_CONFIG"));
		}
		materialise = [];
		for (const m of obj.materialise) {
			if (m === null || typeof m !== "object") {
				return Err(new FlowError("Materialise entry must be an object", "INVALID_CONFIG"));
			}
			const matResult = parseMaterialise(m as Record<string, unknown>);
			if (!matResult.ok) return matResult as Result<never, FlowError>;
			materialise.push(matResult.value);
		}
	}

	// --- direction (optional) ---
	let direction: "one-way" | "bidirectional" | undefined;
	if (obj.direction !== undefined) {
		if (obj.direction !== "one-way" && obj.direction !== "bidirectional") {
			return Err(
				new FlowError(
					'Flow config \'direction\' must be "one-way" or "bidirectional"',
					"INVALID_CONFIG",
				),
			);
		}
		direction = obj.direction;
	}

	const config: FlowConfig = {
		name: obj.name,
		source: sourceResult.value,
		...(store !== undefined && { store }),
		...(materialise !== undefined && { materialise }),
		...(obj.rules !== undefined && { rules: obj.rules as FlowConfig["rules"] }),
		...(direction !== undefined && { direction }),
	};

	return validateFlowConfig(config).ok
		? Ok(config)
		: (validateFlowConfig(config) as Result<never, FlowError>);
}

// ---------------------------------------------------------------------------
// validateFlowConfig — semantic validation of a FlowConfig
// ---------------------------------------------------------------------------

/**
 * Validate a {@link FlowConfig} for semantic correctness.
 *
 * Rules:
 * - `name` must be non-empty
 * - `source.type` must be one of the valid source types
 * - bidirectional direction is only valid with push source
 * - At least one of `store` or `materialise` must be present
 */
export function validateFlowConfig(config: FlowConfig): Result<void, FlowError> {
	// Name must be non-empty
	if (config.name.trim().length === 0) {
		return Err(new FlowError("Flow name must not be empty", "INVALID_CONFIG"));
	}

	// Source type must be valid (already checked during parse, but supports direct calls)
	if (!FLOW_SOURCE_TYPES.includes(config.source.type as (typeof FLOW_SOURCE_TYPES)[number])) {
		return Err(new FlowError(`Invalid source type: "${config.source.type}"`, "INVALID_CONFIG"));
	}

	// Bidirectional only valid with push source
	if (config.direction === "bidirectional" && config.source.type !== "push") {
		return Err(
			new FlowError(
				`Bidirectional direction is only valid with push source, got "${config.source.type}"`,
				"INVALID_CONFIG",
			),
		);
	}

	// At least one of store or materialise must be present
	if (
		config.store === undefined &&
		(config.materialise === undefined || config.materialise.length === 0)
	) {
		return Err(
			new FlowError("Flow must have at least one of 'store' or 'materialise'", "INVALID_CONFIG"),
		);
	}

	return Ok(undefined);
}

// ---------------------------------------------------------------------------
// Internal parsers
// ---------------------------------------------------------------------------

function parseSource(obj: Record<string, unknown>): Result<FlowSourceConfig, FlowError> {
	if (typeof obj.type !== "string") {
		return Err(new FlowError("Source 'type' must be a string", "INVALID_CONFIG"));
	}

	if (!FLOW_SOURCE_TYPES.includes(obj.type as (typeof FLOW_SOURCE_TYPES)[number])) {
		return Err(new FlowError(`Invalid source type: "${obj.type}"`, "INVALID_CONFIG"));
	}

	switch (obj.type) {
		case "cdc":
		case "poll":
			if (typeof obj.adapter !== "string") {
				return Err(
					new FlowError(`Source type "${obj.type}" requires 'adapter' string`, "INVALID_CONFIG"),
				);
			}
			return Ok(obj as unknown as FlowSourceConfig);
		case "push":
			if (typeof obj.gatewayId !== "string") {
				return Err(
					new FlowError("Source type \"push\" requires 'gatewayId' string", "INVALID_CONFIG"),
				);
			}
			return Ok(obj as unknown as FlowSourceConfig);
		case "watch":
			if (typeof obj.adapter !== "string") {
				return Err(
					new FlowError("Source type \"watch\" requires 'adapter' string", "INVALID_CONFIG"),
				);
			}
			return Ok(obj as unknown as FlowSourceConfig);
		default:
			return Err(new FlowError(`Invalid source type: "${obj.type}"`, "INVALID_CONFIG"));
	}
}

function parseStore(obj: Record<string, unknown>): Result<FlowStoreConfig, FlowError> {
	if (typeof obj.type !== "string") {
		return Err(new FlowError("Store 'type' must be a string", "INVALID_CONFIG"));
	}

	if (!FLOW_STORE_TYPES.includes(obj.type as (typeof FLOW_STORE_TYPES)[number])) {
		return Err(new FlowError(`Invalid store type: "${obj.type}"`, "INVALID_CONFIG"));
	}

	switch (obj.type) {
		case "database":
		case "lake":
			if (typeof obj.adapter !== "string") {
				return Err(
					new FlowError(`Store type "${obj.type}" requires 'adapter' string`, "INVALID_CONFIG"),
				);
			}
			return Ok(obj as unknown as FlowStoreConfig);
		case "memory":
			return Ok({ type: "memory" });
		default:
			return Err(new FlowError(`Invalid store type: "${obj.type}"`, "INVALID_CONFIG"));
	}
}

function parseMaterialise(obj: Record<string, unknown>): Result<FlowMaterialiseConfig, FlowError> {
	if (typeof obj.type !== "string") {
		return Err(new FlowError("Materialise 'type' must be a string", "INVALID_CONFIG"));
	}

	if (!FLOW_MATERIALISE_TYPES.includes(obj.type as (typeof FLOW_MATERIALISE_TYPES)[number])) {
		return Err(new FlowError(`Invalid materialise type: "${obj.type}"`, "INVALID_CONFIG"));
	}

	switch (obj.type) {
		case "sql":
			if (typeof obj.adapter !== "string") {
				return Err(
					new FlowError("Materialise type \"sql\" requires 'adapter' string", "INVALID_CONFIG"),
				);
			}
			if (typeof obj.schemas !== "string") {
				return Err(
					new FlowError("Materialise type \"sql\" requires 'schemas' string", "INVALID_CONFIG"),
				);
			}
			return Ok(obj as unknown as FlowMaterialiseConfig);
		case "parquet":
			if (typeof obj.adapter !== "string") {
				return Err(
					new FlowError("Materialise type \"parquet\" requires 'adapter' string", "INVALID_CONFIG"),
				);
			}
			return Ok(obj as unknown as FlowMaterialiseConfig);
		case "client":
			if (typeof obj.gatewayId !== "string") {
				return Err(
					new FlowError(
						"Materialise type \"client\" requires 'gatewayId' string",
						"INVALID_CONFIG",
					),
				);
			}
			return Ok(obj as unknown as FlowMaterialiseConfig);
		default:
			return Err(new FlowError(`Invalid materialise type: "${obj.type}"`, "INVALID_CONFIG"));
	}
}
