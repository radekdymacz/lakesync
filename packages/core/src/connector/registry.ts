// ---------------------------------------------------------------------------
// Connector Registry — machine-readable metadata for all connector types
// ---------------------------------------------------------------------------

import type { TableSchema } from "../delta/types";
import type { ConnectorType } from "./types";

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
// Module-level registry
// ---------------------------------------------------------------------------

const descriptors = new Map<string, ConnectorDescriptor>();

/**
 * Register (or replace) a connector descriptor.
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
