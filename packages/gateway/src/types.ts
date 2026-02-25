import type { NessieCatalogueClient } from "@lakesync/catalogue";
import {
	type ActionHandler,
	type DatabaseAdapter,
	defaultLogger,
	type HLCTimestamp,
	type LakeAdapter,
	type Logger,
	type Materialisable,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import type { FlushQueue } from "./flush-queue";
import type { SchemaManager } from "./schema-manager";

/** Result returned by {@link SyncGateway.handlePush}. */
export interface HandlePushResult {
	/** Server HLC after processing the push. */
	serverHlc: HLCTimestamp;
	/** Number of deltas accepted (including idempotent re-pushes). */
	accepted: number;
	/** Deltas actually ingested (excludes idempotent re-pushes). */
	deltas: RowDelta[];
}

/** Configuration for buffer thresholds (subset of GatewayConfig). */
export interface BufferConfig {
	/** Maximum buffer size in bytes before triggering flush. */
	maxBufferBytes: number;
	/** Maximum buffer age in milliseconds before triggering flush. */
	maxBufferAgeMs: number;
	/** Adaptive buffer configuration for wide-column deltas. */
	adaptiveBufferConfig?: {
		/** Average delta byte threshold above which flush triggers earlier. */
		wideColumnThreshold: number;
		/** Factor to reduce effective maxBufferBytes (0-1). */
		reductionFactor: number;
	};
	/** Maximum buffer bytes before rejecting pushes (default: 2 × maxBufferBytes). */
	maxBackpressureBytes?: number;
	/** Maximum buffer bytes per table before auto-flushing that table. */
	perTableBudgetBytes?: number;
}

/** Configuration for flush target (lake adapter output). */
export interface FlushTargetConfig {
	/** Flush output format. Defaults to "parquet". */
	flushFormat?: "json" | "parquet";
	/** Table schema — required for Parquet flush. */
	tableSchema?: TableSchema;
	/** Optional Nessie catalogue client for Iceberg snapshot registration. */
	catalogue?: NessieCatalogueClient;
}

/** Configuration for post-flush materialisation. */
export interface MaterialiseConfig {
	/** Table schemas for materialisation after flush. */
	schemas?: ReadonlyArray<TableSchema>;
	/** Additional materialisers invoked after successful flush (non-fatal). */
	materialisers?: ReadonlyArray<Materialisable>;
	/**
	 * Optional flush queue for post-flush materialisation.
	 *
	 * When not provided, a `MemoryFlushQueue` is created automatically
	 * from the adapter (if materialisable) and `materialisers`.
	 */
	flushQueue?: FlushQueue;
	/** Optional callback invoked when materialisation fails. Useful for metrics/alerting. */
	onMaterialisationFailure?: (table: string, deltaCount: number, error: Error) => void;
}

/** Configuration for the sync gateway */
export interface GatewayConfig extends BufferConfig {
	/** Unique gateway identifier */
	gatewayId: string;
	/** Optional schema manager for delta validation. */
	schemaManager?: SchemaManager;
	/** Optional storage adapter — LakeAdapter (S3/R2) or DatabaseAdapter (Postgres/MySQL). */
	adapter?: LakeAdapter | DatabaseAdapter;
	/** Named source adapters for adapter-sourced pulls. */
	sourceAdapters?: Record<string, DatabaseAdapter>;
	/** Named action handlers for imperative action execution. */
	actionHandlers?: Record<string, ActionHandler>;
	/** Optional logger callback. Defaults to `console[level]`. */
	logger?: Logger;

	/** Grouped flush target configuration. Takes precedence over flat fields. */
	flush?: FlushTargetConfig;
	/** Grouped materialisation configuration. Takes precedence over flat fields. */
	materialise?: MaterialiseConfig;

	// --- Flat fields (deprecated — prefer `flush` and `materialise` sub-configs) ---

	/**
	 * Flush output format. Defaults to "parquet".
	 * @deprecated Use `flush.flushFormat` instead.
	 */
	flushFormat?: "json" | "parquet";
	/**
	 * Table schema — required for Parquet flush.
	 * @deprecated Use `flush.tableSchema` instead.
	 */
	tableSchema?: TableSchema;
	/**
	 * Optional Nessie catalogue client for Iceberg snapshot registration.
	 * @deprecated Use `flush.catalogue` instead.
	 */
	catalogue?: NessieCatalogueClient;
	/**
	 * Table schemas for materialisation after flush.
	 * @deprecated Use `materialise.schemas` instead.
	 */
	schemas?: ReadonlyArray<TableSchema>;
	/**
	 * Additional materialisers invoked after successful flush (non-fatal).
	 * @deprecated Use `materialise.materialisers` instead.
	 */
	materialisers?: ReadonlyArray<Materialisable>;
	/**
	 * Optional flush queue for post-flush materialisation.
	 * @deprecated Use `materialise.flushQueue` instead.
	 */
	flushQueue?: FlushQueue;
	/**
	 * Optional callback invoked when materialisation fails.
	 * @deprecated Use `materialise.onMaterialisationFailure` instead.
	 */
	onMaterialisationFailure?: (table: string, deltaCount: number, error: Error) => void;
}

/** Resolved (normalised) gateway configuration with grouped sub-configs. */
export interface ResolvedGatewayConfig extends BufferConfig {
	gatewayId: string;
	schemaManager?: SchemaManager;
	adapter: LakeAdapter | DatabaseAdapter | null;
	sourceAdapters: Record<string, DatabaseAdapter>;
	actionHandlers?: Record<string, ActionHandler>;
	logger: Logger;
	flush: FlushTargetConfig;
	materialise: MaterialiseConfig;
}

/**
 * Normalise a `GatewayConfig` into a `ResolvedGatewayConfig`.
 *
 * Merges deprecated flat fields into the grouped sub-configs.
 * Grouped sub-config fields take precedence over flat fields.
 */
export function normaliseGatewayConfig(
	config: GatewayConfig,
	adapter?: LakeAdapter | DatabaseAdapter,
): ResolvedGatewayConfig {
	return {
		gatewayId: config.gatewayId,
		maxBufferBytes: config.maxBufferBytes,
		maxBufferAgeMs: config.maxBufferAgeMs,
		adaptiveBufferConfig: config.adaptiveBufferConfig,
		maxBackpressureBytes: config.maxBackpressureBytes,
		perTableBudgetBytes: config.perTableBudgetBytes,
		schemaManager: config.schemaManager,
		adapter: config.adapter ?? adapter ?? null,
		sourceAdapters: config.sourceAdapters ?? {},
		actionHandlers: config.actionHandlers,
		logger: config.logger ?? defaultLogger,
		flush: {
			flushFormat: config.flush?.flushFormat ?? config.flushFormat,
			tableSchema: config.flush?.tableSchema ?? config.tableSchema,
			catalogue: config.flush?.catalogue ?? config.catalogue,
		},
		materialise: {
			schemas: config.materialise?.schemas ?? config.schemas,
			materialisers: config.materialise?.materialisers ?? config.materialisers,
			flushQueue: config.materialise?.flushQueue ?? config.flushQueue,
			onMaterialisationFailure:
				config.materialise?.onMaterialisationFailure ?? config.onMaterialisationFailure,
		},
	};
}

/** Gateway runtime state */
export interface GatewayState {
	/** Current server HLC */
	hlc: HLCTimestamp;
	/** Whether a flush is currently in progress */
	flushing: boolean;
}

/** Versioned envelope for flushed data */
export interface FlushEnvelope {
	/** Envelope format version */
	version: 1;
	/** Gateway that produced this flush */
	gatewayId: string;
	/** ISO 8601 creation timestamp */
	createdAt: string;
	/** Range of HLC timestamps in this flush */
	hlcRange: { min: HLCTimestamp; max: HLCTimestamp };
	/** Number of deltas in this flush */
	deltaCount: number;
	/** Estimated byte size */
	byteSize: number;
	/** The flushed deltas */
	deltas: RowDelta[];
}
