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
import type { FlushQueue } from "./flush-queue";

/** Dependencies for flush operations. */
export interface FlushCoordinatorDeps {
	/** Gateway configuration for flush. */
	config: FlushConfig;
	/** Table schemas for materialisation after flush. */
	schemas?: ReadonlyArray<TableSchema>;
	/** Optional flush queue for post-flush materialisation. */
	flushQueue?: FlushQueue;
}

/**
 * Coordinates flush operations from the buffer to the adapter.
 *
 * Owns the flushing state to prevent concurrent flushes and handles
 * entry restoration on failure. After a successful flush, publishes
 * entries to the configured `FlushQueue` for downstream materialisation.
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
	 * entries are restored to the buffer. On success, publishes to the
	 * flush queue as a non-fatal post-step.
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

			if (result.ok && deps.flushQueue) {
				await this.publishToQueue(entries, deps);
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

			if (result.ok && deps.flushQueue) {
				await this.publishToQueue(entries, deps);
			}

			return result;
		} finally {
			this.flushing = false;
		}
	}

	/**
	 * Publish flushed entries to the queue for materialisation (non-fatal).
	 *
	 * Failures are warned but never fail the flush.
	 */
	private async publishToQueue(entries: RowDelta[], deps: FlushCoordinatorDeps): Promise<void> {
		if (!deps.flushQueue || !deps.schemas || deps.schemas.length === 0) return;

		try {
			const result = await deps.flushQueue.publish(entries, {
				gatewayId: deps.config.gatewayId,
				schemas: deps.schemas,
			});
			if (!result.ok) {
				console.warn(
					`[lakesync] FlushQueue publish failed (${entries.length} deltas): ${result.error.message}`,
				);
			}
		} catch (error: unknown) {
			const err = error instanceof Error ? error : new Error(String(error));
			console.warn(
				`[lakesync] FlushQueue publish error (${entries.length} deltas): ${err.message}`,
			);
		}
	}

	/** Restore drained entries back to the buffer for retry. */
	private restoreEntries(buffer: DeltaBuffer, entries: RowDelta[]): void {
		for (const entry of entries) {
			buffer.append(entry);
		}
	}
}
