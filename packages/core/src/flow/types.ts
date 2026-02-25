import type { SyncRulesConfig } from "../sync-rules";

// ---------------------------------------------------------------------------
// Flow source — where changes originate
// ---------------------------------------------------------------------------

/** CDC (change data capture) source — streams changes from a database adapter. */
export interface FlowSourceCdc {
	type: "cdc";
	/** Named adapter reference. */
	adapter: string;
	/** Optional table filter. Empty = all tables. */
	tables?: string[];
	/** Poll interval in milliseconds (default determined by engine). */
	pollIntervalMs?: number;
}

/** Poll source — periodically queries a database adapter for new rows. */
export interface FlowSourcePoll {
	type: "poll";
	/** Named adapter reference. */
	adapter: string;
	/** Optional table filter. Empty = all tables. */
	tables?: string[];
	/** Poll interval in milliseconds (default determined by engine). */
	intervalMs?: number;
}

/** Push source — receives deltas pushed through a gateway. */
export interface FlowSourcePush {
	type: "push";
	/** Gateway ID that accepts pushes. */
	gatewayId: string;
}

/** Watch source — watches an object-store prefix for new files. */
export interface FlowSourceWatch {
	type: "watch";
	/** Named adapter reference. */
	adapter: string;
	/** Optional prefix to watch. */
	prefix?: string;
}

/** Source configuration — where changes originate. */
export type FlowSourceConfig = FlowSourceCdc | FlowSourcePoll | FlowSourcePush | FlowSourceWatch;

/** Valid source type discriminators. */
export const FLOW_SOURCE_TYPES = ["cdc", "poll", "push", "watch"] as const;

/** Union of valid source type strings. */
export type FlowSourceType = (typeof FLOW_SOURCE_TYPES)[number];

// ---------------------------------------------------------------------------
// Flow store — where the delta changelog is persisted
// ---------------------------------------------------------------------------

/** Database store — persists deltas in a SQL database. */
export interface FlowStoreDatabase {
	type: "database";
	/** Named adapter reference. */
	adapter: string;
}

/** Lake store — persists deltas as files in an object store. */
export interface FlowStoreLake {
	type: "lake";
	/** Named adapter reference. */
	adapter: string;
	/** File format for persisted deltas. */
	format?: "parquet" | "json";
}

/** Memory store — keeps deltas in memory only (useful for testing/ephemeral). */
export interface FlowStoreMemory {
	type: "memory";
}

/** Store configuration — where the delta changelog is persisted. */
export type FlowStoreConfig = FlowStoreDatabase | FlowStoreLake | FlowStoreMemory;

/** Valid store type discriminators. */
export const FLOW_STORE_TYPES = ["database", "lake", "memory"] as const;

/** Union of valid store type strings. */
export type FlowStoreType = (typeof FLOW_STORE_TYPES)[number];

// ---------------------------------------------------------------------------
// Flow materialise — where current state is written
// ---------------------------------------------------------------------------

/** SQL materialise — upserts current state into a SQL destination table. */
export interface FlowMaterialiseSql {
	type: "sql";
	/** Named adapter reference. */
	adapter: string;
	/** Schema selector for materialisation. */
	schemas: string;
}

/** Parquet materialise — writes current state as Parquet files. */
export interface FlowMaterialiseParquet {
	type: "parquet";
	/** Named adapter reference. */
	adapter: string;
	/** Output path prefix. */
	path?: string;
}

/** Client materialise — pushes current state to connected clients via gateway. */
export interface FlowMaterialiseClient {
	type: "client";
	/** Gateway ID serving connected clients. */
	gatewayId: string;
}

/** Materialise configuration — where current state is written. */
export type FlowMaterialiseConfig =
	| FlowMaterialiseSql
	| FlowMaterialiseParquet
	| FlowMaterialiseClient;

/** Valid materialise type discriminators. */
export const FLOW_MATERIALISE_TYPES = ["sql", "parquet", "client"] as const;

/** Union of valid materialise type strings. */
export type FlowMaterialiseType = (typeof FLOW_MATERIALISE_TYPES)[number];

// ---------------------------------------------------------------------------
// Flow direction
// ---------------------------------------------------------------------------

/** Direction of data flow. */
export type FlowDirection = "one-way" | "bidirectional";

// ---------------------------------------------------------------------------
// FlowConfig — the core declarative unit
// ---------------------------------------------------------------------------

/**
 * Configuration for a single sync flow.
 *
 * Declares where data comes from (source), where the changelog is stored (store),
 * and where current state is materialised (materialise). Optional sync rules
 * filter which deltas flow through.
 */
export interface FlowConfig {
	/** Unique name for this flow. */
	name: string;

	/** Source configuration — where changes come from. */
	source: FlowSourceConfig;

	/** Store configuration — where deltas are persisted (the changelog). */
	store?: FlowStoreConfig;

	/** Materialise configurations — where current state is written. */
	materialise?: FlowMaterialiseConfig[];

	/** Optional sync rules for filtering deltas. */
	rules?: SyncRulesConfig;

	/** Flow direction. Default: "one-way". */
	direction?: FlowDirection;
}

// ---------------------------------------------------------------------------
// Flow runtime status
// ---------------------------------------------------------------------------

/** Runtime state of a flow. */
export type FlowState = "idle" | "running" | "error" | "stopped";

/**
 * Immutable snapshot of a flow's runtime state.
 *
 * Mutations swap the entire snapshot atomically — the same pattern used by
 * {@link DeltaBuffer} and {@link SchemaManager} elsewhere in the codebase.
 */
export interface FlowSnapshot {
	readonly state: FlowState;
	readonly deltasProcessed: number;
	readonly lastError?: string;
	readonly lastActivityAt?: Date;
	readonly handle?: FlowHandle;
}

/** Runtime status of a single flow. */
export interface FlowStatus {
	/** Flow name. */
	name: string;
	/** Current state. */
	state: FlowState;
	/** Total deltas processed since start. */
	deltasProcessed: number;
	/** Last error message, if any. */
	lastError?: string;
	/** Timestamp of last activity (push, pull, materialise). */
	lastActivityAt?: Date;
}

// ---------------------------------------------------------------------------
// FlowError
// ---------------------------------------------------------------------------

/** Error codes for flow operations. */
export type FlowErrorCode =
	| "FLOW_NOT_FOUND"
	| "FLOW_ALREADY_EXISTS"
	| "FLOW_START_FAILED"
	| "INVALID_CONFIG";

/** Error class for flow operations. */
export class FlowError extends Error {
	override readonly name = "FlowError";

	constructor(
		message: string,
		/** Typed error code. */
		public readonly code: FlowErrorCode,
	) {
		super(message);
	}
}

// ---------------------------------------------------------------------------
// FlowEngine — lifecycle management interface
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// FlowRuntime — injected wiring for flow execution
// ---------------------------------------------------------------------------

/**
 * Handle returned by {@link FlowRuntime.start} representing a running flow.
 *
 * The engine calls `stop()` when the flow is stopped or the engine shuts down.
 */
export interface FlowHandle {
	/** Stop the running flow and release resources. */
	stop(): Promise<void>;
}

/**
 * Runtime wiring for flow execution.
 *
 * The flow engine is defined in `@lakesync/core` which cannot depend on
 * `@lakesync/gateway` or `@lakesync/adapter`. Instead, consumers inject
 * a `FlowRuntime` that knows how to resolve adapter names, create gateways,
 * and wire source → store → materialise pipelines.
 *
 * When no runtime is provided, the engine manages state transitions only
 * (useful for testing and configuration validation).
 */
export interface FlowRuntime {
	/**
	 * Start a flow based on its configuration.
	 *
	 * The runtime is responsible for:
	 * - Resolving named adapters from the config
	 * - Creating gateways for the store path
	 * - Wiring materialisation targets
	 * - Setting up polling/CDC/push listeners
	 *
	 * Returns a handle that can stop the flow.
	 */
	start(config: FlowConfig): Promise<FlowHandle>;
}

/** Dependencies injected into the flow engine. */
export interface FlowEngineDeps {
	/**
	 * Optional callback invoked when a flow transitions state.
	 * Useful for logging and monitoring.
	 */
	onFlowStateChange?: (name: string, from: FlowState, to: FlowState) => void;

	/**
	 * Optional runtime for wiring flow execution.
	 *
	 * When provided, `startFlow()` delegates to `runtime.start()` to
	 * create the adapter → gateway → materialise pipeline. When absent,
	 * the engine only manages state transitions (dry-run mode).
	 */
	runtime?: FlowRuntime;
}

/**
 * Engine that manages the lifecycle of multiple declarative flows.
 *
 * Flows are isolated — one failing does not affect others.
 */
export interface FlowEngine {
	/** Register a flow. Does not start it. */
	addFlow(config: FlowConfig): import("../result/result").Result<void, FlowError>;

	/** Start a specific flow by name. */
	startFlow(name: string): Promise<import("../result/result").Result<void, FlowError>>;

	/** Stop a specific flow by name. */
	stopFlow(name: string): Promise<import("../result/result").Result<void, FlowError>>;

	/** Start all registered flows. */
	startAll(): Promise<import("../result/result").Result<void, FlowError>>;

	/** Stop all flows gracefully. */
	stopAll(): Promise<void>;

	/** Get status of all registered flows. */
	getStatus(): FlowStatus[];
}
