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

/** Configuration for age-based tiered storage. */
export interface LifecycleAdapterConfig {
	/** Hot tier — recent data, fast queries. */
	hot: {
		/** The adapter storing recent deltas. */
		adapter: DatabaseAdapter;
		/** Maximum age in milliseconds before data is considered cold. */
		maxAgeMs: number;
	};
	/** Cold tier — older data, cheap storage. */
	cold: {
		/** The adapter storing archived deltas. */
		adapter: DatabaseAdapter;
	};
}

/**
 * Routes database operations across hot and cold tiers based on delta age.
 *
 * Writes always go to the hot adapter. Reads fan out to both tiers when
 * the requested HLC is older than the configured `maxAgeMs` threshold.
 *
 * Use {@link migrateToTier} as a background job to copy aged-out deltas
 * from hot to cold.
 *
 * **Materialisation:** This adapter exposes a `materialise()` method for
 * duck-type compatibility with `isMaterialisable()`. Materialisation is
 * delegated to the hot tier only — cold tier stores archived deltas, not
 * destination tables. When the hot adapter is not materialisable, the
 * method is a graceful no-op returning `Ok`.
 */
export class LifecycleAdapter implements DatabaseAdapter {
	private readonly hot: DatabaseAdapter;
	private readonly cold: DatabaseAdapter;
	private readonly maxAgeMs: number;

	constructor(config: LifecycleAdapterConfig) {
		this.hot = config.hot.adapter;
		this.cold = config.cold.adapter;
		this.maxAgeMs = config.hot.maxAgeMs;
	}

	/** Insert deltas into the hot adapter — new data is always hot. */
	async insertDeltas(deltas: RowDelta[]): Promise<Result<void, AdapterError>> {
		return this.hot.insertDeltas(deltas);
	}

	/**
	 * Query deltas since the given HLC.
	 *
	 * If `sinceHlc` is older than `now - maxAgeMs`, queries both hot and cold
	 * adapters and merges the results sorted by HLC. Otherwise queries hot only.
	 */
	async queryDeltasSince(
		hlc: HLCTimestamp,
		tables?: string[],
	): Promise<Result<RowDelta[], AdapterError>> {
		const sinceWallMs = Number(hlc >> 16n);
		const thresholdMs = Date.now() - this.maxAgeMs;

		if (sinceWallMs < thresholdMs) {
			// Query spans into cold territory — fan out to both tiers
			const [hotResult, coldResult] = await Promise.all([
				this.hot.queryDeltasSince(hlc, tables),
				this.cold.queryDeltasSince(hlc, tables),
			]);

			if (!hotResult.ok) return hotResult;
			if (!coldResult.ok) return coldResult;

			const merged = [...hotResult.value, ...coldResult.value];
			merged.sort((a, b) => (a.hlc < b.hlc ? -1 : a.hlc > b.hlc ? 1 : 0));

			return Ok(merged);
		}

		// Recent query — hot tier only
		return this.hot.queryDeltasSince(hlc, tables);
	}

	/** Get latest state — try hot first, fall back to cold if hot returns null. */
	async getLatestState(
		table: string,
		rowId: string,
	): Promise<Result<Record<string, unknown> | null, AdapterError>> {
		const hotResult = await this.hot.getLatestState(table, rowId);
		if (!hotResult.ok) return hotResult;

		if (hotResult.value !== null) {
			return hotResult;
		}

		return this.cold.getLatestState(table, rowId);
	}

	/** Ensure schema exists on both hot and cold adapters. */
	async ensureSchema(schema: TableSchema): Promise<Result<void, AdapterError>> {
		const hotResult = await this.hot.ensureSchema(schema);
		if (!hotResult.ok) return hotResult;

		return this.cold.ensureSchema(schema);
	}

	/** Materialise via hot tier only — cold tier stores archived deltas, not destination tables. */
	async materialise(
		deltas: RowDelta[],
		schemas: ReadonlyArray<TableSchema>,
	): Promise<Result<void, AdapterError>> {
		if (isMaterialisable(this.hot)) {
			return this.hot.materialise(deltas, schemas);
		}
		return Ok(undefined);
	}

	/** Close both hot and cold adapters. */
	async close(): Promise<void> {
		await this.hot.close();
		await this.cold.close();
	}
}

/**
 * Migrate aged-out deltas from the hot adapter to the cold adapter.
 *
 * Queries the hot adapter for all deltas since HLC 0, filters those with
 * wall time older than `Date.now() - maxAgeMs`, and inserts them into the
 * cold adapter. Insertion is idempotent via deltaId uniqueness.
 *
 * Does NOT delete from hot — that is a separate cleanup concern.
 *
 * @param hot - The hot-tier adapter to read old deltas from.
 * @param cold - The cold-tier adapter to write old deltas to.
 * @param maxAgeMs - Age threshold in milliseconds.
 * @returns The count of migrated deltas, or an AdapterError.
 */
export async function migrateToTier(
	hot: DatabaseAdapter,
	cold: DatabaseAdapter,
	maxAgeMs: number,
): Promise<Result<{ migrated: number }, AdapterError>> {
	const thresholdMs = Date.now() - maxAgeMs;
	const thresholdHlc = (BigInt(0) << 16n) as HLCTimestamp;

	const result = await hot.queryDeltasSince(thresholdHlc);
	if (!result.ok) return result;

	const oldDeltas = result.value.filter((delta) => {
		const wallMs = Number(delta.hlc >> 16n);
		return wallMs < thresholdMs;
	});

	if (oldDeltas.length === 0) {
		return Ok({ migrated: 0 });
	}

	const insertResult = await cold.insertDeltas(oldDeltas);
	if (!insertResult.ok) return insertResult;

	return Ok({ migrated: oldDeltas.length });
}
