/**
 * Backward-compatibility layer for the Postgres CDC source.
 *
 * The original monolithic `PostgresCdcSource` has been refactored into:
 * - {@link import("./dialect").CdcDialect} — generic interface
 * - {@link import("./cdc-source").CdcSource} — generic polling source
 * - {@link import("./postgres-dialect").PostgresCdcDialect} — Postgres implementation
 *
 * This module re-exports the old public API so existing consumers continue
 * to work without changes, and provides a `createPostgresCdcSource` factory.
 */

import type { HLC, Source } from "@lakesync/core";
import { CdcSource } from "./cdc-source";
import { PostgresCdcDialect } from "./postgres-dialect";

// Re-export wal2json types from the dialect (backward compatibility)
export type {
	PostgresCdcDialectConfig,
	Wal2JsonChange,
	Wal2JsonPayload,
} from "./postgres-dialect";
export { PostgresCdcDialect, parseWal2JsonChanges } from "./postgres-dialect";

// ---------------------------------------------------------------------------
// Configuration (preserved for backward compatibility)
// ---------------------------------------------------------------------------

/** Configuration for the Postgres CDC source. */
export interface PostgresCdcConfig {
	/** Postgres connection string. */
	connectionString: string;
	/** Replication slot name (created if it does not exist). Default: `"lakesync_cdc"`. */
	slotName?: string;
	/** Tables to capture. Empty or omitted means all tables. */
	tables?: string[];
	/** Poll interval in milliseconds. Default: `1000`. */
	pollIntervalMs?: number;
	/** HLC instance for timestamp assignment. One is created if not provided. */
	hlc?: HLC;
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/**
 * Create a Postgres CDC source using the generic CdcSource + PostgresCdcDialect.
 *
 * @param config - Postgres CDC configuration.
 * @returns A generic CdcSource backed by a PostgresCdcDialect.
 */
export function createPostgresCdcSource(config: PostgresCdcConfig): CdcSource {
	const dialect = new PostgresCdcDialect({
		connectionString: config.connectionString,
		slotName: config.slotName,
	});

	return new CdcSource({
		dialect,
		tables: config.tables,
		pollIntervalMs: config.pollIntervalMs,
		hlc: config.hlc,
	});
}

// ---------------------------------------------------------------------------
// PostgresCdcSource (backward compatibility alias)
// ---------------------------------------------------------------------------

/**
 * Postgres CDC source — backward-compatibility wrapper.
 *
 * @deprecated Use `createPostgresCdcSource` or construct `CdcSource` + `PostgresCdcDialect` directly.
 */
export class PostgresCdcSource {
	private readonly inner: CdcSource;
	private readonly slotName: string;
	readonly name: string;

	constructor(config: PostgresCdcConfig) {
		this.slotName = config.slotName ?? "lakesync_cdc";
		this.inner = createPostgresCdcSource(config);
		this.name = `postgres-cdc:${this.slotName}`;
	}

	get clientId(): string {
		return `cdc:${this.slotName}`;
	}

	start: Source["start"] = (onDeltas) => this.inner.start(onDeltas);
	stop: Source["stop"] = () => this.inner.stop();
	getCursor: Source["getCursor"] = () => this.inner.getCursor();
	setCursor: Source["setCursor"] = (cursor) => this.inner.setCursor(cursor);
	discoverSchemas: NonNullable<Source["discoverSchemas"]> = () => this.inner.discoverSchemas!();
}
