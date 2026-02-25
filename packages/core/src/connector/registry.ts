// ---------------------------------------------------------------------------
// Connector Registry — machine-readable metadata for all connector types
// ---------------------------------------------------------------------------

import type { TableSchema } from "../delta/types";
import type { ConnectorFactory, ConnectorType } from "./types";

/** Connector category — determines the ingest model. */
export type ConnectorCategory = "database" | "api";

/**
 * Machine-readable descriptor for a connector type.
 *
 * Exposes enough metadata for a UI to render an "Add Connector" form
 * without hardcoding per-type config shapes.
 */
export interface ConnectorDescriptor {
	/** Connector type identifier (e.g. "postgres", "jira"). */
	type: ConnectorType;
	/** Human-readable display name (e.g. "PostgreSQL"). */
	displayName: string;
	/** Short description of the connector. */
	description: string;
	/** Ingest model category. */
	category: ConnectorCategory;
	/** JSON Schema (draft-07) describing the connector-specific config object. */
	configSchema: Record<string, unknown>;
	/** JSON Schema (draft-07) describing the ingest configuration. */
	ingestSchema: Record<string, unknown>;
	/** Output table schemas for API connectors. Null for database connectors. */
	outputTables: ReadonlyArray<TableSchema> | null;
}

// ---------------------------------------------------------------------------
// ConnectorRegistry — immutable value created from a list of descriptors
// ---------------------------------------------------------------------------

/** Immutable registry of connector descriptors. */
export interface ConnectorRegistry {
	/** Look up a single descriptor by type. */
	get(type: string): ConnectorDescriptor | undefined;
	/** List all descriptors, sorted alphabetically by type. */
	list(): ConnectorDescriptor[];
	/** Create a new registry with an additional or replaced descriptor. */
	with(descriptor: ConnectorDescriptor): ConnectorRegistry;
	/** Create a new registry with output schemas attached to a type. */
	withOutputSchemas(type: string, schemas: ReadonlyArray<TableSchema>): ConnectorRegistry;
}

/**
 * Create an immutable {@link ConnectorRegistry} from a list of descriptors.
 */
export function createConnectorRegistry(descriptors: ConnectorDescriptor[]): ConnectorRegistry {
	const map = new Map<string, ConnectorDescriptor>();
	for (const d of descriptors) {
		map.set(d.type, d);
	}
	return buildRegistry(map);
}

function buildRegistry(map: Map<string, ConnectorDescriptor>): ConnectorRegistry {
	return {
		get(type: string): ConnectorDescriptor | undefined {
			return map.get(type);
		},
		list(): ConnectorDescriptor[] {
			return [...map.values()].sort((a, b) => a.type.localeCompare(b.type));
		},
		with(descriptor: ConnectorDescriptor): ConnectorRegistry {
			const next = new Map(map);
			next.set(descriptor.type, descriptor);
			return buildRegistry(next);
		},
		withOutputSchemas(type: string, schemas: ReadonlyArray<TableSchema>): ConnectorRegistry {
			const existing = map.get(type);
			if (!existing) return buildRegistry(map);
			const next = new Map(map);
			next.set(type, { ...existing, outputTables: schemas });
			return buildRegistry(next);
		},
	};
}

// ---------------------------------------------------------------------------
// ConnectorFactoryRegistry — unified factory registry for all connector types
// ---------------------------------------------------------------------------

/**
 * Immutable registry mapping connector type strings to {@link ConnectorFactory}
 * functions. Replaces the dual `PollerRegistry` + `AdapterFactoryRegistry`
 * lookup with a single dispatch path.
 *
 * Uses the same `.with()` pattern as `AdapterFactoryRegistry`.
 */
export interface ConnectorFactoryRegistry {
	/** Look up a factory by connector type. */
	get(type: string): ConnectorFactory | undefined;
	/** Create a new registry with an additional or replaced factory. */
	with(type: string, factory: ConnectorFactory): ConnectorFactoryRegistry;
}

/**
 * Create an immutable {@link ConnectorFactoryRegistry} from an optional Map.
 */
export function createConnectorFactoryRegistry(
	factories: Map<string, ConnectorFactory> = new Map(),
): ConnectorFactoryRegistry {
	return buildFactoryRegistry(new Map(factories));
}

function buildFactoryRegistry(map: Map<string, ConnectorFactory>): ConnectorFactoryRegistry {
	return {
		get(type: string): ConnectorFactory | undefined {
			return map.get(type);
		},
		with(type: string, factory: ConnectorFactory): ConnectorFactoryRegistry {
			const next = new Map(map);
			next.set(type, factory);
			return buildFactoryRegistry(next);
		},
	};
}

// ---------------------------------------------------------------------------
// Module-level mutable registry (backwards compatibility)
// ---------------------------------------------------------------------------

const descriptors = new Map<string, ConnectorDescriptor>();

/**
 * Register (or replace) a connector descriptor in the global registry.
 *
 * Called at module load time by built-in registration and by connector
 * packages that need to attach output schemas.
 */
export function registerConnectorDescriptor(descriptor: ConnectorDescriptor): void {
	descriptors.set(descriptor.type, descriptor);
}

/**
 * Attach output table schemas to an already-registered connector type.
 *
 * No-op if the type has not been registered yet — this allows connector
 * packages to be imported in any order relative to the core registration.
 */
export function registerOutputSchemas(type: string, schemas: ReadonlyArray<TableSchema>): void {
	const existing = descriptors.get(type);
	if (!existing) return;
	descriptors.set(type, { ...existing, outputTables: schemas });
}

/**
 * Look up a single connector descriptor by type.
 *
 * Returns `undefined` when the type is not registered.
 */
export function getConnectorDescriptor(type: string): ConnectorDescriptor | undefined {
	return descriptors.get(type);
}

/**
 * List all registered connector descriptors, sorted alphabetically by type.
 */
export function listConnectorDescriptors(): ConnectorDescriptor[] {
	return [...descriptors.values()].sort((a, b) => a.type.localeCompare(b.type));
}
