import {
	type DatabaseAdapter,
	Err,
	FlushError,
	isDatabaseAdapter,
	type LakeAdapter,
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
	 * entries are restored to the buffer.
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
			return await flushEntries(entries, byteSize, {
				adapter,
				config: deps.config,
				restoreEntries: (e) => this.restoreEntries(buffer, e),
				schemas: deps.schemas,
			});
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
			return await flushEntries(
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
		} finally {
			this.flushing = false;
		}
	}

	/** Restore drained entries back to the buffer for retry. */
	private restoreEntries(buffer: DeltaBuffer, entries: RowDelta[]): void {
		for (const entry of entries) {
			buffer.append(entry);
		}
	}
}
