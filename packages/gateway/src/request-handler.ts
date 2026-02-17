import type { HLCTimestamp, ResolvedClaims, RowDelta, SyncRulesConfig } from "@lakesync/core";
import {
	API_ERROR_CODES,
	listConnectorDescriptors,
	validateConnectorConfig,
	validateSyncRules,
} from "@lakesync/core";
import type { ConfigStore } from "./config-store";
import type { SyncGateway } from "./gateway";
import {
	buildSyncRulesContext,
	parseJson,
	parsePullParams,
	pushErrorToApiCode,
	pushErrorToStatus,
	validateActionBody,
	validatePushBody,
	validateSchemaBody,
} from "./validation";

/** Result from a request handler, ready for platform-specific serialisation. */
export interface HandlerResult {
	status: number;
	body: unknown;
}

/**
 * Handle a push request.
 *
 * @param gateway - The SyncGateway instance.
 * @param raw - The raw request body string.
 * @param headerClientId - Client ID from auth header (for mismatch check).
 * @param opts - Optional callbacks for persistence and broadcast.
 */
export function handlePushRequest(
	gateway: SyncGateway,
	raw: string,
	headerClientId?: string | null,
	opts?: {
		/** Persist deltas before processing (WAL-style). */
		persistBatch?: (deltas: RowDelta[]) => void;
		/** Clear persisted deltas after successful push. */
		clearPersistence?: () => void;
		/** Broadcast deltas to connected clients. */
		broadcastFn?: (
			deltas: RowDelta[],
			serverHlc: HLCTimestamp,
			excludeClientId: string,
		) => void | Promise<void>;
	},
): HandlerResult {
	const validation = validatePushBody(raw, headerClientId);
	if (!validation.ok) {
		return {
			status: validation.error.status,
			body: { error: validation.error.message, code: validation.error.code },
		};
	}

	const body = validation.value;

	// Persist before processing (WAL-style)
	opts?.persistBatch?.(body.deltas);

	const result = gateway.handlePush(body);
	if (!result.ok) {
		return {
			status: pushErrorToStatus(result.error.code),
			body: { error: result.error.message, code: pushErrorToApiCode(result.error.code) },
		};
	}

	// Clear persisted deltas on success
	opts?.clearPersistence?.();

	// Broadcast to connected clients (fire and forget)
	if (opts?.broadcastFn && result.value.deltas.length > 0) {
		opts.broadcastFn(result.value.deltas, result.value.serverHlc, body.clientId);
	}

	return { status: 200, body: result.value };
}

/**
 * Handle a pull request.
 */
export async function handlePullRequest(
	gateway: SyncGateway,
	params: {
		since: string | null;
		clientId: string | null;
		limit: string | null;
		source: string | null;
	},
	claims?: ResolvedClaims,
	syncRules?: SyncRulesConfig,
): Promise<HandlerResult> {
	const validation = parsePullParams(params);
	if (!validation.ok) {
		return {
			status: validation.error.status,
			body: { error: validation.error.message, code: validation.error.code },
		};
	}

	const msg = validation.value;
	const context = buildSyncRulesContext(syncRules, claims ?? {});

	const result = msg.source
		? await gateway.handlePull(
				msg as import("@lakesync/core").SyncPull & { source: string },
				context,
			)
		: gateway.handlePull(msg, context);

	if (!result.ok) {
		const err = result.error;
		if (err.code === "ADAPTER_NOT_FOUND") {
			return { status: 404, body: { error: err.message, code: API_ERROR_CODES.NOT_FOUND } };
		}
		return { status: 500, body: { error: err.message, code: API_ERROR_CODES.INTERNAL_ERROR } };
	}

	return { status: 200, body: result.value };
}

/**
 * Handle an action request.
 */
export async function handleActionRequest(
	gateway: SyncGateway,
	raw: string,
	headerClientId?: string | null,
	claims?: ResolvedClaims,
): Promise<HandlerResult> {
	const validation = validateActionBody(raw, headerClientId);
	if (!validation.ok) {
		return {
			status: validation.error.status,
			body: { error: validation.error.message, code: validation.error.code },
		};
	}

	const context = claims ? { claims } : undefined;
	const result = await gateway.handleAction(validation.value, context);

	if (!result.ok) {
		return {
			status: 400,
			body: { error: result.error.message, code: API_ERROR_CODES.VALIDATION_ERROR },
		};
	}

	return { status: 200, body: result.value };
}

/**
 * Handle a flush request.
 */
export async function handleFlushRequest(
	gateway: SyncGateway,
	opts?: { clearPersistence?: () => void },
): Promise<HandlerResult> {
	const result = await gateway.flush();
	if (!result.ok) {
		return {
			status: 500,
			body: { error: result.error.message, code: API_ERROR_CODES.FLUSH_ERROR },
		};
	}

	opts?.clearPersistence?.();
	return { status: 200, body: { flushed: true } };
}

/**
 * Handle saving a table schema.
 */
export async function handleSaveSchema(
	raw: string,
	store: ConfigStore,
	gatewayId: string,
): Promise<HandlerResult> {
	const validation = validateSchemaBody(raw);
	if (!validation.ok) {
		return {
			status: validation.error.status,
			body: { error: validation.error.message, code: validation.error.code },
		};
	}

	await store.setSchema(gatewayId, validation.value);
	return { status: 200, body: { saved: true } };
}

/**
 * Handle saving sync rules.
 */
export async function handleSaveSyncRules(
	raw: string,
	store: ConfigStore,
	gatewayId: string,
): Promise<HandlerResult> {
	const parsed = parseJson<unknown>(raw);
	if (!parsed.ok) {
		return {
			status: parsed.error.status,
			body: { error: parsed.error.message, code: parsed.error.code },
		};
	}
	const config = parsed.value;

	const validation = validateSyncRules(config);
	if (!validation.ok) {
		return {
			status: 400,
			body: { error: validation.error.message, code: API_ERROR_CODES.VALIDATION_ERROR },
		};
	}

	await store.setSyncRules(gatewayId, config as SyncRulesConfig);
	return { status: 200, body: { saved: true } };
}

/**
 * Handle registering a connector.
 */
export async function handleRegisterConnector(
	raw: string,
	store: ConfigStore,
): Promise<HandlerResult> {
	const parsed = parseJson<unknown>(raw);
	if (!parsed.ok) {
		return {
			status: parsed.error.status,
			body: { error: parsed.error.message, code: parsed.error.code },
		};
	}
	const body = parsed.value;

	const validation = validateConnectorConfig(body);
	if (!validation.ok) {
		return {
			status: 400,
			body: { error: validation.error.message, code: API_ERROR_CODES.VALIDATION_ERROR },
		};
	}

	const config = validation.value;
	const connectors = await store.getConnectors();

	if (connectors[config.name]) {
		return {
			status: 409,
			body: {
				error: `Connector "${config.name}" already exists`,
				code: API_ERROR_CODES.VALIDATION_ERROR,
			},
		};
	}

	connectors[config.name] = config;
	await store.setConnectors(connectors);

	return { status: 200, body: { registered: true, name: config.name } };
}

/**
 * Handle unregistering a connector.
 */
export async function handleUnregisterConnector(
	name: string,
	store: ConfigStore,
): Promise<HandlerResult> {
	const connectors = await store.getConnectors();

	if (!connectors[name]) {
		return {
			status: 404,
			body: { error: `Connector "${name}" not found`, code: API_ERROR_CODES.NOT_FOUND },
		};
	}

	delete connectors[name];
	await store.setConnectors(connectors);

	return { status: 200, body: { unregistered: true, name } };
}

/**
 * Handle listing connectors.
 */
export async function handleListConnectors(store: ConfigStore): Promise<HandlerResult> {
	const connectors = await store.getConnectors();
	const list = Object.values(connectors).map((c) => ({
		name: c.name,
		type: c.type,
		hasIngest: c.ingest !== undefined,
	}));

	return { status: 200, body: list };
}

/**
 * Handle listing available connector types (static metadata).
 */
export function handleListConnectorTypes(): HandlerResult {
	return { status: 200, body: listConnectorDescriptors() };
}

/**
 * Handle metrics request.
 */
export function handleMetrics(
	gateway: SyncGateway,
	extra?: Record<string, unknown>,
): HandlerResult {
	const stats = gateway.bufferStats;
	return { status: 200, body: { ...stats, ...extra } };
}
