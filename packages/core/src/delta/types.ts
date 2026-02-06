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
