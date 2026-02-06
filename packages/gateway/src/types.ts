import type { HLCTimestamp, RowDelta } from "@lakesync/core";

/** Configuration for the sync gateway */
export interface GatewayConfig {
	/** Unique gateway identifier */
	gatewayId: string;
	/** Maximum buffer size in bytes before triggering flush */
	maxBufferBytes: number;
	/** Maximum buffer age in milliseconds before triggering flush */
	maxBufferAgeMs: number;
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
