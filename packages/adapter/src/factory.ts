import {
	AdapterError,
	type BigQueryConnectorConfigFull,
	type ConnectorConfig,
	type DatabaseAdapter,
	Err,
	type MySQLConnectorConfigFull,
	Ok,
	type PostgresConnectorConfigFull,
	type Result,
	toError,
} from "@lakesync/core";
import { BigQueryAdapter } from "./bigquery";
import { MySQLAdapter } from "./mysql";
import { PostgresAdapter } from "./postgres";

// ---------------------------------------------------------------------------
// AdapterFactoryRegistry — open, immutable registry for adapter construction
// ---------------------------------------------------------------------------

/** Factory function that creates a DatabaseAdapter from a ConnectorConfig. */
export type AdapterFactory = (config: ConnectorConfig) => DatabaseAdapter;

/** Immutable registry of adapter factories keyed by connector type. */
export interface AdapterFactoryRegistry {
	/** Look up a factory by type. */
	get(type: string): AdapterFactory | undefined;
	/** Create a new registry with an additional or replaced factory. */
	with(type: string, factory: AdapterFactory): AdapterFactoryRegistry;
}

/**
 * Create an immutable {@link AdapterFactoryRegistry} from a Map of factories.
 */
export function createAdapterFactoryRegistry(
	factories: Map<string, AdapterFactory> = new Map(),
): AdapterFactoryRegistry {
	return buildAdapterFactoryRegistry(new Map(factories));
}

function buildAdapterFactoryRegistry(map: Map<string, AdapterFactory>): AdapterFactoryRegistry {
	return {
		get(type: string): AdapterFactory | undefined {
			return map.get(type);
		},
		with(type: string, factory: AdapterFactory): AdapterFactoryRegistry {
			const next = new Map(map);
			next.set(type, factory);
			return buildAdapterFactoryRegistry(next);
		},
	};
}

/** Default registry with built-in database adapters (Postgres, MySQL, BigQuery). */
export function defaultAdapterFactoryRegistry(): AdapterFactoryRegistry {
	return createAdapterFactoryRegistry()
		.with("postgres", (c) => {
			const pg = (c as PostgresConnectorConfigFull).postgres;
			return new PostgresAdapter({ connectionString: pg.connectionString });
		})
		.with("mysql", (c) => {
			const my = (c as MySQLConnectorConfigFull).mysql;
			return new MySQLAdapter({ connectionString: my.connectionString });
		})
		.with("bigquery", (c) => {
			const bq = (c as BigQueryConnectorConfigFull).bigquery;
			return new BigQueryAdapter({
				projectId: bq.projectId,
				dataset: bq.dataset,
				keyFilename: bq.keyFilename,
				location: bq.location,
			});
		});
}

/**
 * Instantiate a {@link DatabaseAdapter} from a {@link ConnectorConfig}.
 *
 * Uses the provided registry (or the default built-in registry) to look up
 * a factory for the config's type. Returns an {@link AdapterError} if the
 * type is unsupported or the adapter constructor throws.
 *
 * @param config - Validated connector configuration.
 * @param registry - Optional adapter factory registry. Defaults to built-in adapters.
 * @returns The instantiated adapter or an error.
 */
export function createDatabaseAdapter(
	config: ConnectorConfig,
	registry?: AdapterFactoryRegistry,
): Result<DatabaseAdapter, AdapterError> {
	const reg = registry ?? defaultAdapterFactoryRegistry();
	const factory = reg.get(config.type);
	if (!factory) {
		return Err(new AdapterError(`No adapter factory for connector type "${config.type}"`));
	}
	try {
		return Ok(factory(config));
	} catch (err: unknown) {
		return Err(new AdapterError(`Failed to create adapter: ${toError(err).message}`));
	}
}
