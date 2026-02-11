import type { HLCTimestamp, RowDelta } from "@lakesync/core";

/** Helper to create a RowDelta for testing. */
export function makeDelta(overrides: Partial<RowDelta> = {}): RowDelta {
	return {
		deltaId: "delta-1",
		table: "todos",
		rowId: "row-1",
		clientId: "client-a",
		columns: [{ column: "title", value: "Buy milk" }],
		hlc: BigInt(1000) as HLCTimestamp,
		op: "INSERT",
		...overrides,
	};
}

/** Convenience helper to create an HLCTimestamp from a number. */
export function hlc(n: number): HLCTimestamp {
	return BigInt(n) as HLCTimestamp;
}
