import {
	type AdapterError,
	type HLCTimestamp,
	Ok,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";

import type { DatabaseAdapter } from "./db-types";

/** A routing rule that maps specific tables to a database adapter. */
export interface CompositeRoute {
	/** Tables handled by this adapter */
	tables: string[];
	/** The adapter for these tables */
	adapter: DatabaseAdapter;
}

/** Configuration for CompositeAdapter routing. */
export interface CompositeAdapterConfig {
	/** Table-to-adapter routing rules */
	routes: CompositeRoute[];
	/** Fallback adapter for tables not matching any route */
	defaultAdapter: DatabaseAdapter;
}

/**
 * Routes database operations to different adapters based on table name.
 * Implements DatabaseAdapter so it can be used as a drop-in replacement.
 */
export class CompositeAdapter implements DatabaseAdapter {
	private readonly routeMap: Map<string, DatabaseAdapter>;
	private readonly adapters: Set<DatabaseAdapter>;
	private readonly defaultAdapter: DatabaseAdapter;

	constructor(config: CompositeAdapterConfig) {
		this.routeMap = new Map();
		this.adapters = new Set();
		this.defaultAdapter = config.defaultAdapter;
		this.adapters.add(config.defaultAdapter);

		for (const route of config.routes) {
			this.adapters.add(route.adapter);
			for (const table of route.tables) {
				if (this.routeMap.has(table)) {
					throw new Error(`Duplicate table route: "${table}" appears in multiple routes`);
				}
				this.routeMap.set(table, route.adapter);
			}
		}
	}

	/** Insert deltas, routing each group to the correct adapter by table. */
	async insertDeltas(deltas: RowDelta[]): Promise<Result<void, AdapterError>> {
		const groups = new Map<DatabaseAdapter, RowDelta[]>();

		for (const delta of deltas) {
			const adapter = this.routeMap.get(delta.table) ?? this.defaultAdapter;
			let group = groups.get(adapter);
			if (!group) {
				group = [];
				groups.set(adapter, group);
			}
			group.push(delta);
		}

		for (const [adapter, group] of groups) {
			const result = await adapter.insertDeltas(group);
			if (!result.ok) {
				return result;
			}
		}

		return Ok(undefined);
	}

	/** Query deltas since a given HLC, fanning out to relevant adapters and merging results. */
	async queryDeltasSince(
		hlc: HLCTimestamp,
		tables?: string[],
	): Promise<Result<RowDelta[], AdapterError>> {
		const adapterSet = new Set<DatabaseAdapter>();
		const adapterTables = new Map<DatabaseAdapter, string[]>();

		if (tables && tables.length > 0) {
			for (const table of tables) {
				const adapter = this.routeMap.get(table) ?? this.defaultAdapter;
				adapterSet.add(adapter);
				let existing = adapterTables.get(adapter);
				if (!existing) {
					existing = [];
					adapterTables.set(adapter, existing);
				}
				existing.push(table);
			}
		} else {
			for (const adapter of this.adapters) {
				adapterSet.add(adapter);
			}
		}

		const merged: RowDelta[] = [];

		for (const adapter of adapterSet) {
			const filterTables = adapterTables.get(adapter);
			const result = await adapter.queryDeltasSince(hlc, filterTables);
			if (!result.ok) {
				return result;
			}
			merged.push(...result.value);
		}

		merged.sort((a, b) => (a.hlc < b.hlc ? -1 : a.hlc > b.hlc ? 1 : 0));

		return Ok(merged);
	}

	/** Get the latest state for a row, routing to the correct adapter. */
	async getLatestState(
		table: string,
		rowId: string,
	): Promise<Result<Record<string, unknown> | null, AdapterError>> {
		const adapter = this.routeMap.get(table) ?? this.defaultAdapter;
		return adapter.getLatestState(table, rowId);
	}

	/** Ensure schema exists, routing to the correct adapter for the table. */
	async ensureSchema(schema: TableSchema): Promise<Result<void, AdapterError>> {
		const adapter = this.routeMap.get(schema.table) ?? this.defaultAdapter;
		return adapter.ensureSchema(schema);
	}

	/** Close all unique adapters (routes + default, deduplicated). */
	async close(): Promise<void> {
		for (const adapter of this.adapters) {
			await adapter.close();
		}
	}
}
