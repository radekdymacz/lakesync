import {
	isMaterialisable,
	type Materialisable,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";

/** Configuration for the materialisation processor. */
export interface MaterialisationProcessorConfig {
	/** Materialisation targets to invoke. */
	materialisers: ReadonlyArray<Materialisable>;
	/** Optional callback invoked per-table when materialisation fails. */
	onFailure?: (table: string, deltaCount: number, error: Error) => void;
}

/**
 * Run materialisation targets against flushed deltas.
 *
 * Extracted from `FlushCoordinator.runMaterialisers()` so it can be
 * invoked by any consumer (inline, queue worker, polling consumer).
 * Semantics unchanged: iterates targets, catches failures, calls
 * `onFailure` per-table, never throws.
 */
export async function processMaterialisation(
	entries: ReadonlyArray<RowDelta>,
	schemas: ReadonlyArray<TableSchema>,
	config: MaterialisationProcessorConfig,
): Promise<void> {
	if (schemas.length === 0) return;
	if (config.materialisers.length === 0) return;

	for (const target of config.materialisers) {
		try {
			const matResult = await target.materialise([...entries], schemas);
			if (!matResult.ok) {
				const error = new Error(matResult.error.message);
				console.warn(
					`[lakesync] Materialisation failed (${entries.length} deltas): ${matResult.error.message}`,
				);
				notifyFailure(entries, error, config.onFailure);
			}
		} catch (error: unknown) {
			const err = error instanceof Error ? error : new Error(String(error));
			console.warn(`[lakesync] Materialisation error (${entries.length} deltas): ${err.message}`);
			notifyFailure(entries, err, config.onFailure);
		}
	}
}

/**
 * Build the full list of materialisation targets from an adapter and
 * explicit materialisers. Used by `MemoryFlushQueue` and the gateway
 * constructor to assemble targets.
 */
export function collectMaterialisers(
	adapter: unknown,
	extra?: ReadonlyArray<Materialisable>,
): Materialisable[] {
	const targets: Materialisable[] = [];
	if (isMaterialisable(adapter)) {
		targets.push(adapter);
	}
	if (extra) {
		targets.push(...extra);
	}
	return targets;
}

/** Notify the onFailure callback for each affected table. */
function notifyFailure(
	entries: ReadonlyArray<RowDelta>,
	error: Error,
	onFailure?: (table: string, deltaCount: number, error: Error) => void,
): void {
	if (!onFailure) return;
	const tables = new Set(entries.map((e) => e.table));
	for (const table of tables) {
		const count = entries.filter((e) => e.table === table).length;
		onFailure(table, count, error);
	}
}
