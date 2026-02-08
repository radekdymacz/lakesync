import type { DatabaseAdapter, LakeAdapter } from "@lakesync/adapter";
import type { NessieCatalogueClient } from "@lakesync/catalogue";
import type { HLCTimestamp, RowDelta, TableSchema } from "@lakesync/core";
import type { SchemaManager } from "./schema-manager";

/** Configuration for the sync gateway */
export interface GatewayConfig {
	/** Unique gateway identifier */
	gatewayId: string;
	/** Maximum buffer size in bytes before triggering flush */
	maxBufferBytes: number;
	/** Maximum buffer age in milliseconds before triggering flush */
	maxBufferAgeMs: number;
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
