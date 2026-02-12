export type { DatabaseAdapter, DatabaseAdapterConfig } from "@lakesync/core";
export { isDatabaseAdapter } from "@lakesync/core";

import type { TableSchema } from "@lakesync/core";

/**
 * Map a LakeSync column type to a BigQuery column definition.
 */
const BIGQUERY_TYPE_MAP: Record<TableSchema["columns"][number]["type"], string> = {
	string: "STRING",
	number: "FLOAT64",
	boolean: "BOOL",
	json: "JSON",
	null: "STRING",
};

export function lakeSyncTypeToBigQuery(type: TableSchema["columns"][number]["type"]): string {
	return BIGQUERY_TYPE_MAP[type];
}
