import type { DatabaseAdapter, LakeAdapter } from "@lakesync/adapter";
import type { NessieCatalogueClient } from "@lakesync/catalogue";
import type { ActionHandler, HLCTimestamp, RowDelta, TableSchema } from "@lakesync/core";
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

/** Configuration for the sync gateway */
export interface GatewayConfig extends BufferConfig {
	/** Unique gateway identifier */
	gatewayId: string;
	/** Flush output format. Defaults to "parquet". */
	flushFormat?: "json" | "parquet";
	/** Table schema — required for Parquet flush. */
	tableSchema?: TableSchema;
	/** Optional Nessie catalogue client for Iceberg snapshot registration. */
	catalogue?: NessieCatalogueClient;
	/** Optional schema manager for delta validation. */
	schemaManager?: SchemaManager;
	/** Optional storage adapter — LakeAdapter (S3/R2) or DatabaseAdapter (Postgres/MySQL). */
	adapter?: LakeAdapter | DatabaseAdapter;
	/** Named source adapters for adapter-sourced pulls. */
	sourceAdapters?: Record<string, DatabaseAdapter>;
	/** Named action handlers for imperative action execution. */
	actionHandlers?: Record<string, ActionHandler>;
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
