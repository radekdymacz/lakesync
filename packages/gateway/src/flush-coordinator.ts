import {
	type DatabaseAdapter,
	Err,
	FlushError,
	isDatabaseAdapter,
	isMaterialisable,
	type LakeAdapter,
	type Materialisable,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import type { DeltaBuffer } from "./buffer";
import type { FlushConfig } from "./flush";
import { flushEntries } from "./flush";

/** Dependencies for flush operations. */
export interface FlushCoordinatorDeps {
	/** Gateway configuration for flush. */
	config: FlushConfig;
	/** Table schemas for materialisation after flush. */
	schemas?: ReadonlyArray<TableSchema>;
	/**
	 * Additional materialisers to invoke after successful flush.
	 *
	 * These are called in addition to the flush adapter itself (when it
	 * implements `Materialisable`). Failures are non-fatal — warned but
	 * never fail the flush.
	 */
	materialisers?: ReadonlyArray<Materialisable>;
}

/**
 * Coordinates flush operations from the buffer to the adapter.
 *
 * Owns the flushing state to prevent concurrent flushes and handles
 * entry restoration on failure.
 */
export class FlushCoordinator {
	private flushing = false;

	/** Whether a flush is currently in progress. */
	get isFlushing(): boolean {
		return this.flushing;
	}

	/**
	 * Flush all entries from the buffer to the adapter.
	 *
	 * Drains the buffer first, then writes to the adapter. On failure,
	 * entries are restored to the buffer. On success, materialisers are
	 * invoked as a non-fatal post-step.
	 */
	async flush(
		buffer: DeltaBuffer,
		adapter: LakeAdapter | DatabaseAdapter | null,
		deps: FlushCoordinatorDeps,
	): Promise<Result<void, FlushError>> {
		if (this.flushing) {
			return Err(new FlushError("Flush already in progress"));
		}
		if (buffer.logSize === 0) {
			return Ok(undefined);
		}
		if (!adapter) {
			return Err(new FlushError("No adapter configured"));
		}

		this.flushing = true;

		const byteSize = isDatabaseAdapter(adapter) ? 0 : buffer.byteSize;
		const entries = buffer.drain();
		if (entries.length === 0) {
			this.flushing = false;
			return Ok(undefined);
		}

		try {
			const result = await flushEntries(entries, byteSize, {
				adapter,
				config: deps.config,
				restoreEntries: (e) => this.restoreEntries(buffer, e),
				schemas: deps.schemas,
			});

			if (result.ok) {
				await this.runMaterialisers(entries, adapter, deps);
			}

			return result;
		} finally {
			this.flushing = false;
		}
	}

	/**
	 * Flush a single table's deltas from the buffer.
	 *
	 * Drains only the specified table's deltas and flushes them,
	 * leaving other tables in the buffer.
	 */
	async flushTable(
		table: string,
		buffer: DeltaBuffer,
		adapter: LakeAdapter | DatabaseAdapter | null,
		deps: FlushCoordinatorDeps,
	): Promise<Result<void, FlushError>> {
		if (this.flushing) {
			return Err(new FlushError("Flush already in progress"));
		}
		if (!adapter) {
			return Err(new FlushError("No adapter configured"));
		}

		const entries = buffer.drainTable(table);
		if (entries.length === 0) {
			return Ok(undefined);
		}

		this.flushing = true;

		try {
			const result = await flushEntries(
				entries,
				0,
				{
					adapter,
					config: deps.config,
					restoreEntries: (e) => this.restoreEntries(buffer, e),
					schemas: deps.schemas,
				},
				table,
			);

			if (result.ok) {
				await this.runMaterialisers(entries, adapter, deps);
			}

			return result;
		} finally {
			this.flushing = false;
		}
	}

	/**
	 * Run all materialisers after successful flush (non-fatal).
	 *
	 * Invokes the flush adapter's own `materialise()` (when it implements
	 * `Materialisable`), then any additional materialisers from deps.
	 */
	private async runMaterialisers(
		entries: RowDelta[],
		adapter: LakeAdapter | DatabaseAdapter,
		deps: FlushCoordinatorDeps,
	): Promise<void> {
		const schemas = deps.schemas;
		if (!schemas || schemas.length === 0) return;

		const targets: Materialisable[] = [];

		// Include the flush adapter itself when it supports materialisation
		if (isMaterialisable(adapter)) {
			targets.push(adapter);
		}

		// Include any additional materialisers from deps
		if (deps.materialisers) {
			targets.push(...deps.materialisers);
		}

		for (const target of targets) {
			try {
				const matResult = await target.materialise(entries, schemas);
				if (!matResult.ok) {
					const error = new Error(matResult.error.message);
					console.warn(
						`[lakesync] Materialisation failed (${entries.length} deltas): ${matResult.error.message}`,
					);
					notifyMaterialisationFailure(entries, error, deps.config);
				}
			} catch (error: unknown) {
				const err = error instanceof Error ? error : new Error(String(error));
				console.warn(
					`[lakesync] Materialisation error (${entries.length} deltas): ${err.message}`,
				);
				notifyMaterialisationFailure(entries, err, deps.config);
			}
		}
	}

	/** Restore drained entries back to the buffer for retry. */
	private restoreEntries(buffer: DeltaBuffer, entries: RowDelta[]): void {
		for (const entry of entries) {
			buffer.append(entry);
		}
	}
}

/**
 * Notify the onMaterialisationFailure callback if configured.
 * Extracts unique table names from deltas for per-table reporting.
 */
function notifyMaterialisationFailure(
	entries: RowDelta[],
	error: Error,
	config: FlushConfig,
): void {
	if (!config.onMaterialisationFailure) return;
	const tables = new Set(entries.map((e) => e.table));
	for (const table of tables) {
		const count = entries.filter((e) => e.table === table).length;
		config.onMaterialisationFailure(table, count, error);
	}
}
