// ---------------------------------------------------------------------------
// SourcePoller — polls external databases and pushes deltas to SyncGateway
// ---------------------------------------------------------------------------

import type { HLCTimestamp, RowDelta, SyncPush } from "@lakesync/core";
import { extractDelta, HLC } from "@lakesync/core";
import type { SyncGateway } from "@lakesync/gateway";
import type { IngestSourceConfig, IngestTableConfig } from "./types";

const DEFAULT_INTERVAL_MS = 10_000;
const DEFAULT_LOOKBACK_MS = 5_000;
const LARGE_SNAPSHOT_WARN = 1_000;

/** Per-table state for cursor strategy. */
interface CursorState {
	lastCursor: unknown;
}

/** Per-table state for diff strategy. */
interface DiffState {
	snapshot: Map<string, Record<string, unknown>>;
}

/**
 * Polls an external data source and pushes detected changes into a
 * {@link SyncGateway} via `handlePush()`.
 *
 * Supports two change detection strategies:
 * - **cursor**: fast incremental polling using a monotonically increasing column
 * - **diff**: full-table comparison detecting inserts, updates, and deletes
 */
export class SourcePoller {
	private readonly config: IngestSourceConfig;
	private readonly gateway: SyncGateway;
	private readonly hlc: HLC;
	private readonly clientId: string;

	private timer: ReturnType<typeof setTimeout> | null = null;
	private running = false;

	/** Cursor state per table (keyed by table name). */
	private cursorStates = new Map<string, CursorState>();
	/** Diff snapshot per table (keyed by table name). */
	private diffStates = new Map<string, DiffState>();

	/** Optional callback invoked after each poll with the current cursor state. */
	public onCursorUpdate?: (state: Record<string, unknown>) => void;

	constructor(config: IngestSourceConfig, gateway: SyncGateway) {
		this.config = config;
		this.gateway = gateway;
		this.hlc = new HLC();
		this.clientId = `ingest:${config.name}`;
	}

	/** Start the polling loop. */
	start(): void {
		if (this.running) return;
		this.running = true;
		this.schedulePoll();
	}

	/** Stop the polling loop. */
	stop(): void {
		this.running = false;
		if (this.timer) {
			clearTimeout(this.timer);
			this.timer = null;
		}
	}

	/** Whether the poller is currently running. */
	get isRunning(): boolean {
		return this.running;
	}

	// -----------------------------------------------------------------------
	// Poll scheduling (recursive setTimeout — no overlap)
	// -----------------------------------------------------------------------

	private schedulePoll(): void {
		if (!this.running) return;

		this.timer = setTimeout(async () => {
			try {
				await this.poll();
			} catch {
				// Swallow errors — a failed poll must never crash the server
			}
			this.schedulePoll();
		}, this.config.intervalMs ?? DEFAULT_INTERVAL_MS);
	}

	/** Export cursor state as a JSON-serialisable object for external persistence. */
	getCursorState(): Record<string, unknown> {
		const cursors: Record<string, unknown> = {};
		for (const [table, state] of this.cursorStates) {
			cursors[table] = state.lastCursor;
		}
		return { cursorStates: cursors };
	}

	/** Restore cursor state from a previously exported snapshot. */
	setCursorState(state: Record<string, unknown>): void {
		const cursors = state.cursorStates as Record<string, unknown> | undefined;
		if (!cursors) return;
		for (const [table, cursor] of Object.entries(cursors)) {
			this.cursorStates.set(table, { lastCursor: cursor });
		}
	}

	/** Execute a single poll cycle across all configured tables. */
	async poll(): Promise<void> {
		const allDeltas: RowDelta[] = [];

		for (const table of this.config.tables) {
			const deltas =
				table.strategy.type === "cursor"
					? await this.pollCursor(table)
					: await this.pollDiff(table);

			for (const d of deltas) {
				allDeltas.push(d);
			}
		}

		if (allDeltas.length === 0) {
			if (this.onCursorUpdate) {
				this.onCursorUpdate(this.getCursorState());
			}
			return;
		}

		const push: SyncPush = {
			clientId: this.clientId,
			deltas: allDeltas,
			lastSeenHlc: 0n as HLCTimestamp,
		};

		this.gateway.handlePush(push);

		if (this.onCursorUpdate) {
			this.onCursorUpdate(this.getCursorState());
		}
	}

	// -----------------------------------------------------------------------
	// Cursor strategy
	// -----------------------------------------------------------------------

	private async pollCursor(tableConfig: IngestTableConfig): Promise<RowDelta[]> {
		const strategy = tableConfig.strategy;
		if (strategy.type !== "cursor") return [];

		const rowIdCol = tableConfig.rowIdColumn ?? "id";
		const state = this.cursorStates.get(tableConfig.table);
		const lookbackMs = strategy.lookbackMs ?? DEFAULT_LOOKBACK_MS;

		let rows: Record<string, unknown>[];

		if (state?.lastCursor != null) {
			// Subsequent poll: apply look-back overlap for late-committing transactions
			let effectiveCursor = state.lastCursor;
			if (typeof effectiveCursor === "number") {
				effectiveCursor = effectiveCursor - lookbackMs;
			} else if (effectiveCursor instanceof Date) {
				effectiveCursor = new Date(effectiveCursor.getTime() - lookbackMs);
			} else if (typeof effectiveCursor === "string") {
				// Attempt ISO date string
				const parsed = Date.parse(effectiveCursor);
				if (!Number.isNaN(parsed)) {
					effectiveCursor = new Date(parsed - lookbackMs).toISOString();
				}
			}

			const sql = `SELECT * FROM (${tableConfig.query}) AS _src WHERE ${strategy.cursorColumn} > $1 ORDER BY ${strategy.cursorColumn} ASC`;
			rows = await this.config.queryFn(sql, [effectiveCursor]);
		} else {
			// First poll: fetch everything
			const sql = `SELECT * FROM (${tableConfig.query}) AS _src ORDER BY ${strategy.cursorColumn} ASC`;
			rows = await this.config.queryFn(sql);
		}

		if (rows.length === 0) return [];

		// Update cursor to max value
		const lastRow = rows[rows.length - 1]!;
		const newCursor = lastRow[strategy.cursorColumn];
		this.cursorStates.set(tableConfig.table, { lastCursor: newCursor });

		// Convert rows to deltas
		// Cursor strategy cannot determine previous state — every row is an INSERT/upsert
		const deltas: RowDelta[] = [];
		for (const row of rows) {
			const rowId = String(row[rowIdCol]);
			const after = { ...row };
			delete after[rowIdCol];

			const delta = await extractDelta(null, after, {
				table: tableConfig.table,
				rowId,
				clientId: this.clientId,
				hlc: this.hlc.now(),
			});

			if (delta) {
				deltas.push(delta);
			}
		}

		return deltas;
	}

	// -----------------------------------------------------------------------
	// Diff strategy
	// -----------------------------------------------------------------------

	private async pollDiff(tableConfig: IngestTableConfig): Promise<RowDelta[]> {
		const rowIdCol = tableConfig.rowIdColumn ?? "id";
		const rows = await this.config.queryFn(tableConfig.query);

		const currentMap = new Map<string, Record<string, unknown>>();
		for (const row of rows) {
			const rowId = String(row[rowIdCol]);
			currentMap.set(rowId, row);
		}

		if (currentMap.size > LARGE_SNAPSHOT_WARN) {
			console.warn(
				`[lakesync:ingest] Diff snapshot for "${tableConfig.table}" has ${currentMap.size} rows (>1k). Consider using cursor strategy.`,
			);
		}

		const state = this.diffStates.get(tableConfig.table);
		const previousMap = state?.snapshot ?? new Map<string, Record<string, unknown>>();

		const deltas: RowDelta[] = [];

		// Detect inserts and updates
		for (const [rowId, currentRow] of currentMap) {
			const previousRow = previousMap.get(rowId);

			// Build column maps without rowId column
			const after = { ...currentRow };
			delete after[rowIdCol];

			let before: Record<string, unknown> | null = null;
			if (previousRow) {
				before = { ...previousRow };
				delete before[rowIdCol];
			}

			const delta = await extractDelta(before, after, {
				table: tableConfig.table,
				rowId,
				clientId: this.clientId,
				hlc: this.hlc.now(),
			});

			if (delta) {
				deltas.push(delta);
			}
		}

		// Detect deletes: rows in previous snapshot missing from current
		for (const [rowId, previousRow] of previousMap) {
			if (!currentMap.has(rowId)) {
				const before = { ...previousRow };
				delete before[rowIdCol];

				const delta = await extractDelta(before, null, {
					table: tableConfig.table,
					rowId,
					clientId: this.clientId,
					hlc: this.hlc.now(),
				});

				if (delta) {
					deltas.push(delta);
				}
			}
		}

		// Replace snapshot
		this.diffStates.set(tableConfig.table, { snapshot: currentMap });

		return deltas;
	}
}
