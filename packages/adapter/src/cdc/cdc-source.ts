import {
	type AdapterError,
	type ColumnDelta,
	HLC,
	type HLCTimestamp,
	Ok,
	type OnDeltas,
	type Result,
	type RowDelta,
	type Source,
	type SourceCursor,
	type TableSchema,
} from "@lakesync/core";
import stableStringify from "fast-json-stable-stringify";
import type { CdcCursor, CdcDialect, CdcRawChange } from "./dialect";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Configuration for a generic CDC source. */
export interface CdcSourceConfig {
	/** The CDC dialect to use. */
	dialect: CdcDialect;
	/** Tables to capture. Empty or omitted means all tables. */
	tables?: string[];
	/** Poll interval in milliseconds. Default: `1000`. */
	pollIntervalMs?: number;
	/** HLC instance for timestamp assignment. One is created if not provided. */
	hlc?: HLC;
}

// ---------------------------------------------------------------------------
// CdcSource
// ---------------------------------------------------------------------------

/**
 * Generic CDC source that reads changes via a pluggable {@link CdcDialect}.
 *
 * The source is completely database-agnostic: all database-specific logic
 * (connection management, slot/binlog setup, change parsing) lives in the
 * dialect. The CdcSource handles:
 * - Polling loop with overlap guard
 * - Converting {@link CdcRawChange} to {@link RowDelta}
 * - Cursor tracking
 * - Deterministic delta ID generation
 */
export class CdcSource implements Source {
	readonly name: string;

	private readonly dialect: CdcDialect;
	private readonly tables: string[] | null;
	private readonly pollIntervalMs: number;
	private readonly hlc: HLC;

	private timer: ReturnType<typeof setInterval> | null = null;
	private polling = false;
	private cursor: CdcCursor;

	constructor(config: CdcSourceConfig) {
		this.dialect = config.dialect;
		this.tables = config.tables && config.tables.length > 0 ? config.tables : null;
		this.pollIntervalMs = config.pollIntervalMs ?? 1_000;
		this.hlc = config.hlc ?? new HLC();
		this.cursor = this.dialect.defaultCursor();
		this.name = `cdc:${this.dialect.name}`;
	}

	/** Client identifier used for deltas produced by this source. */
	get clientId(): string {
		return `cdc:${this.dialect.name}`;
	}

	// -----------------------------------------------------------------------
	// Source interface
	// -----------------------------------------------------------------------

	async start(onDeltas: OnDeltas): Promise<Result<void, AdapterError>> {
		const connectResult = await this.dialect.connect();
		if (!connectResult.ok) return connectResult;

		const captureResult = await this.dialect.ensureCapture(this.tables);
		if (!captureResult.ok) return captureResult;

		this.timer = setInterval(() => {
			void this.pollCycle(onDeltas);
		}, this.pollIntervalMs);

		return Ok(undefined);
	}

	async stop(): Promise<void> {
		if (this.timer !== null) {
			clearInterval(this.timer);
			this.timer = null;
		}
		await this.dialect.close();
	}

	getCursor(): SourceCursor {
		return { ...this.cursor };
	}

	setCursor(cursor: SourceCursor): void {
		this.cursor = { ...cursor };
	}

	async discoverSchemas(): Promise<Result<TableSchema[], AdapterError>> {
		return this.dialect.discoverSchemas(this.tables);
	}

	// -----------------------------------------------------------------------
	// Polling
	// -----------------------------------------------------------------------

	/** Single poll cycle — guarded against overlapping calls. */
	private async pollCycle(onDeltas: OnDeltas): Promise<void> {
		if (this.polling) return;
		this.polling = true;
		try {
			const result = await this.dialect.fetchChanges(this.cursor);
			if (!result.ok) return;

			const batch = result.value;
			if (batch.changes.length > 0) {
				const tablesFilter = this.tables ? new Set(this.tables) : null;
				const deltas = await convertChangesToDeltas(
					batch.changes,
					this.hlc.now(),
					this.clientId,
					tablesFilter,
				);
				if (deltas.length > 0) {
					await onDeltas(deltas);
				}
			}
			this.cursor = batch.cursor;
		} catch {
			// Errors during polling are silently swallowed to keep the loop alive.
		} finally {
			this.polling = false;
		}
	}
}

// ---------------------------------------------------------------------------
// Change-to-delta conversion — exported for unit testing
// ---------------------------------------------------------------------------

/**
 * Convert an array of raw CDC changes into RowDeltas.
 *
 * Exported for direct unit testing without a database connection.
 *
 * @param changes - The raw change entries from the dialect.
 * @param hlc - HLC timestamp to assign to all deltas in the batch.
 * @param clientId - Client identifier for the deltas.
 * @param tables - Optional set of tables to filter by. Null means all tables.
 * @returns Array of RowDeltas.
 */
export async function convertChangesToDeltas(
	changes: CdcRawChange[],
	hlc: HLCTimestamp,
	clientId: string,
	tables: Set<string> | null,
): Promise<RowDelta[]> {
	const deltas: RowDelta[] = [];

	for (const change of changes) {
		if (tables && !tables.has(change.table)) continue;

		const op = mapKindToOp(change.kind);
		const deltaId = await generateDeltaId({
			clientId,
			hlc,
			table: change.table,
			rowId: change.rowId,
			columns: change.columns,
		});

		deltas.push({
			op,
			table: change.table,
			rowId: change.rowId,
			clientId,
			columns: change.columns,
			hlc,
			deltaId,
		});
	}

	return deltas;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Map CDC `kind` to LakeSync `DeltaOp`. */
function mapKindToOp(kind: CdcRawChange["kind"]): RowDelta["op"] {
	switch (kind) {
		case "insert":
			return "INSERT";
		case "update":
			return "UPDATE";
		case "delete":
			return "DELETE";
	}
}

/**
 * Generate a deterministic delta ID using SHA-256.
 *
 * Uses the same stable-stringify approach as `extractDelta` in core.
 */
async function generateDeltaId(params: {
	clientId: string;
	hlc: HLCTimestamp;
	table: string;
	rowId: string;
	columns: ColumnDelta[];
}): Promise<string> {
	const payload = stableStringify({
		clientId: params.clientId,
		hlc: params.hlc.toString(),
		table: params.table,
		rowId: params.rowId,
		columns: params.columns,
	});

	const data = new TextEncoder().encode(payload);
	const hashBuffer = await crypto.subtle.digest("SHA-256", data);
	const bytes = new Uint8Array(hashBuffer);

	let hex = "";
	for (const b of bytes) {
		hex += b.toString(16).padStart(2, "0");
	}
	return hex;
}
