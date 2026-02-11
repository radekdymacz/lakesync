import {
	type AdapterError,
	type HLCTimestamp,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";

import type { DatabaseAdapter } from "./db-types";
import { isMaterialisable } from "./materialise";

/** Configuration for the FanOutAdapter. */
export interface FanOutAdapterConfig {
	/** The primary adapter that handles all reads and authoritative writes. */
	primary: DatabaseAdapter;
	/** Secondary adapters that receive replicated writes on a best-effort basis. */
	secondaries: DatabaseAdapter[];
}

/**
 * Writes to a primary adapter synchronously and replicates to secondary
 * adapters asynchronously. Reads always go to the primary.
 *
 * Secondary failures are silently caught and never affect the return value.
 * Use case: write to Postgres (fast, operational), replicate to BigQuery (analytics).
 *
 * **Materialisation:** This adapter exposes a `materialise()` method for
 * duck-type compatibility with `isMaterialisable()`. When the primary adapter
 * is itself materialisable, materialisation is delegated to it; otherwise
 * the method is a graceful no-op returning `Ok`. Materialisable secondaries
 * receive fire-and-forget replication.
 */
export class FanOutAdapter implements DatabaseAdapter {
	private readonly primary: DatabaseAdapter;
	private readonly secondaries: ReadonlyArray<DatabaseAdapter>;

	constructor(config: FanOutAdapterConfig) {
		this.primary = config.primary;
		this.secondaries = config.secondaries;
	}

	/** Insert deltas into the primary, then replicate to secondaries (fire-and-forget). */
	async insertDeltas(deltas: RowDelta[]): Promise<Result<void, AdapterError>> {
		const result = await this.primary.insertDeltas(deltas);
		if (!result.ok) {
			return result;
		}

		for (const secondary of this.secondaries) {
			secondary.insertDeltas(deltas).catch(() => {});
		}

		return Ok(undefined);
	}

	/** Query deltas from the primary adapter only. */
	async queryDeltasSince(
		hlc: HLCTimestamp,
		tables?: string[],
	): Promise<Result<RowDelta[], AdapterError>> {
		return this.primary.queryDeltasSince(hlc, tables);
	}

	/** Get the latest state from the primary adapter only. */
	async getLatestState(
		table: string,
		rowId: string,
	): Promise<Result<Record<string, unknown> | null, AdapterError>> {
		return this.primary.getLatestState(table, rowId);
	}

	/** Ensure schema on the primary first, then best-effort on secondaries. */
	async ensureSchema(schema: TableSchema): Promise<Result<void, AdapterError>> {
		const result = await this.primary.ensureSchema(schema);
		if (!result.ok) {
			return result;
		}

		for (const secondary of this.secondaries) {
			secondary.ensureSchema(schema).catch(() => {});
		}

		return Ok(undefined);
	}

	/** Materialise via primary, then replicate to materialisable secondaries (fire-and-forget). */
	async materialise(
		deltas: RowDelta[],
		schemas: ReadonlyArray<TableSchema>,
	): Promise<Result<void, AdapterError>> {
		if (isMaterialisable(this.primary)) {
			const result = await this.primary.materialise(deltas, schemas);
			if (!result.ok) {
				return result;
			}
		}

		for (const secondary of this.secondaries) {
			if (isMaterialisable(secondary)) {
				secondary.materialise(deltas, schemas).catch(() => {});
			}
		}

		return Ok(undefined);
	}

	/** Close primary and all secondary adapters. */
	async close(): Promise<void> {
		await this.primary.close();
		for (const secondary of this.secondaries) {
			await secondary.close();
		}
	}
}
