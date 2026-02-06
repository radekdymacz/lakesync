import type { HLCTimestamp } from "../hlc/types";

/** Delta operation type */
export type DeltaOp = "INSERT" | "UPDATE" | "DELETE";

/** A single column-level change */
export interface ColumnDelta {
	/** Column name */
	column: string;
	/** Serialisable JSON value — NEVER undefined, use null instead */
	value: unknown;
}

/** A row-level delta containing column-level changes */
export interface RowDelta {
	/** Operation type */
	op: DeltaOp;
	/** Table name */
	table: string;
	/** Row identifier */
	rowId: string;
	/** Client identifier — used for LWW tiebreak and audit */
	clientId: string;
	/** Changed columns — empty for DELETE */
	columns: ColumnDelta[];
	/** HLC timestamp (branded bigint) */
	hlc: HLCTimestamp;
	/** Deterministic identifier: hash(clientId + hlc + table + rowId + columns) */
	deltaId: string;
}

/** Minimal schema for Phase 1. Column allow-list + type hints. */
export interface TableSchema {
	table: string;
	columns: Array<{
		name: string;
		type: "string" | "number" | "boolean" | "json" | "null";
	}>;
}

/** Composite key utility — avoids string concatenation bugs */
export type RowKey = string & { readonly __brand: "RowKey" };

/** Create a composite row key from table and row ID */
export function rowKey(table: string, rowId: string): RowKey {
	return `${table}:${rowId}` as RowKey;
}

/** SyncPush input message — sent by clients to push local deltas to the gateway */
export interface SyncPush {
	/** Client that sent the push */
	clientId: string;
	/** Deltas to push */
	deltas: RowDelta[];
	/** Client's last-seen HLC */
	lastSeenHlc: HLCTimestamp;
}

/** SyncPull input message — sent by clients to pull remote deltas from the gateway */
export interface SyncPull {
	/** Client that sent the pull */
	clientId: string;
	/** Return deltas with HLC strictly after this value */
	sinceHlc: HLCTimestamp;
	/** Maximum number of deltas to return */
	maxDeltas: number;
}

/** SyncResponse output — returned by the gateway after push or pull */
export interface SyncResponse {
	/** Deltas matching the pull criteria */
	deltas: RowDelta[];
	/** Current server HLC */
	serverHlc: HLCTimestamp;
	/** Whether there are more deltas to fetch */
	hasMore: boolean;
}
