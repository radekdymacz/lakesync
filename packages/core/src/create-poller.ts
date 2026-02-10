// ---------------------------------------------------------------------------
// createPoller — registry-based factory for creating pollers from ConnectorConfig
// ---------------------------------------------------------------------------

import type { BaseSourcePoller, PushTarget } from "./base-poller";
import type { ConnectorConfig } from "./connector/types";

/** Factory function that creates a poller from a ConnectorConfig. */
export type PollerFactory = (config: ConnectorConfig, gateway: PushTarget) => BaseSourcePoller;

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
 * @throws If no factory has been registered for the config's `type`.
 */
export function createPoller(config: ConnectorConfig, gateway: PushTarget): BaseSourcePoller {
	const factory = pollerFactories.get(config.type);
	if (!factory) {
		throw new Error(
			`No poller factory registered for connector type "${config.type}". ` +
				`Did you import the connector package (e.g. "@lakesync/connector-${config.type}")?`,
		);
	}
	return factory(config, gateway);
}
