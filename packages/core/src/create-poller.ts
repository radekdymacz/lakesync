// ---------------------------------------------------------------------------
// createPoller — registry-based factory for creating pollers from ConnectorConfig
// ---------------------------------------------------------------------------

import type { BaseSourcePoller, PushTarget } from "./base-poller";
import type { ConnectorConfig } from "./connector/types";

/** Factory function that creates a poller from a ConnectorConfig. */
export type PollerFactory = (config: ConnectorConfig, gateway: PushTarget) => BaseSourcePoller;

// ---------------------------------------------------------------------------
// PollerRegistry — explicit value holding poller factories
// ---------------------------------------------------------------------------

/** Immutable registry of poller factories keyed by connector type. */
export interface PollerRegistry {
	/** Look up a factory by type. */
	get(type: string): PollerFactory | undefined;
	/** Create a new registry with an additional or replaced factory. */
	with(type: string, factory: PollerFactory): PollerRegistry;
}

/**
 * Create an immutable {@link PollerRegistry} from a Map of factories.
 */
export function createPollerRegistry(
	factories: Map<string, PollerFactory> = new Map(),
): PollerRegistry {
	return buildPollerRegistry(new Map(factories));
}

function buildPollerRegistry(map: Map<string, PollerFactory>): PollerRegistry {
	return {
		get(type: string): PollerFactory | undefined {
			return map.get(type);
		},
		with(type: string, factory: PollerFactory): PollerRegistry {
			const next = new Map(map);
			next.set(type, factory);
			return buildPollerRegistry(next);
		},
	};
}

// ---------------------------------------------------------------------------
// Global mutable registry (backwards compatibility)
// ---------------------------------------------------------------------------

/** Registry of poller factory functions keyed by connector type. */
const pollerFactories = new Map<string, PollerFactory>();

/**
 * Register a poller factory for a connector type.
 * Connector packages call this at module load time so that
 * `createPoller()` can instantiate the correct poller.
 */
export function registerPollerFactory(type: string, factory: PollerFactory): void {
	pollerFactories.set(type, factory);
}

/**
 * Create a poller from a {@link ConnectorConfig}.
 *
 * @param config - Connector configuration.
 * @param gateway - Push target for the poller.
 * @param registry - Optional explicit registry. Defaults to the global registry.
 * @throws If no factory has been registered for the config's `type`.
 */
export function createPoller(
	config: ConnectorConfig,
	gateway: PushTarget,
	registry?: PollerRegistry,
): BaseSourcePoller {
	const factory = registry ? registry.get(config.type) : pollerFactories.get(config.type);
	if (!factory) {
		throw new Error(
			`No poller factory registered for connector type "${config.type}". ` +
				`Did you import the connector package (e.g. "@lakesync/connector-${config.type}")?`,
		);
	}
	return factory(config, gateway);
}
