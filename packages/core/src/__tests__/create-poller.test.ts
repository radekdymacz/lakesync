import { describe, expect, it } from "vitest";
import {
	BaseSourcePoller,
	type ConnectorConfig,
	createPoller,
	createPollerRegistry,
	type PushTarget,
} from "../index";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Minimal no-op PushTarget for testing. */
const noopGateway: PushTarget = {
	handlePush() {},
};

/** Concrete test poller returned by factories. */
class StubPoller extends BaseSourcePoller {
	readonly connectorName: string;
	constructor(name: string, gateway: PushTarget) {
		super({ name, intervalMs: 60_000, gateway });
		this.connectorName = name;
	}
	async poll(): Promise<void> {}
	getCursorState(): Record<string, unknown> {
		return {};
	}
	setCursorState(_state: Record<string, unknown>): void {}
}

/** Shared registry with jira + salesforce factories. */
const testRegistry = createPollerRegistry()
	.with("jira", (config, gateway) => new StubPoller(config.name, gateway))
	.with("salesforce", (config, gateway) => new StubPoller(config.name, gateway));

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("createPoller", () => {
	it("creates a poller for a registered jira connector", () => {
		const config: ConnectorConfig = {
			name: "my-jira",
			type: "jira",
			jira: { domain: "test", email: "a@b.com", apiToken: "tok" },
		};
		const poller = createPoller(config, noopGateway, testRegistry);
		expect(poller).toBeInstanceOf(BaseSourcePoller);
		expect((poller as StubPoller).connectorName).toBe("my-jira");
	});

	it("creates a poller for a registered salesforce connector", () => {
		const config: ConnectorConfig = {
			name: "my-sf",
			type: "salesforce",
			salesforce: {
				instanceUrl: "https://test.salesforce.com",
				clientId: "cid",
				clientSecret: "csec",
				username: "user",
				password: "pass",
			},
		};
		const poller = createPoller(config, noopGateway, testRegistry);
		expect(poller).toBeInstanceOf(BaseSourcePoller);
		expect((poller as StubPoller).connectorName).toBe("my-sf");
	});

	it("throws a descriptive error for an unregistered connector type", () => {
		const config: ConnectorConfig = {
			name: "unknown-src",
			type: "postgres",
			postgres: { connectionString: "postgres://localhost/db" },
		};
		const emptyRegistry = createPollerRegistry();
		expect(() => createPoller(config, noopGateway, emptyRegistry)).toThrow(
			/No poller factory registered for connector type "postgres"/,
		);
	});

	it("error message suggests importing the connector package", () => {
		const config: ConnectorConfig = {
			name: "missing",
			type: "bigquery",
			bigquery: { projectId: "p", dataset: "d" },
		};
		const emptyRegistry = createPollerRegistry();
		expect(() => createPoller(config, noopGateway, emptyRegistry)).toThrow(
			/Did you import the connector package/,
		);
	});
});

// ---------------------------------------------------------------------------
// PollerRegistry — explicit registry tests
// ---------------------------------------------------------------------------

describe("PollerRegistry", () => {
	it("creates a registry and uses it with createPoller", () => {
		const registry = createPollerRegistry().with("jira", (config, gateway) => {
			return new StubPoller(config.name, gateway);
		});
		const config: ConnectorConfig = {
			name: "explicit-jira",
			type: "jira",
			jira: { domain: "test", email: "a@b.com", apiToken: "tok" },
		};
		const poller = createPoller(config, noopGateway, registry);
		expect(poller).toBeInstanceOf(BaseSourcePoller);
		expect((poller as StubPoller).connectorName).toBe("explicit-jira");
	});

	it(".with() returns a new registry (immutability)", () => {
		const base = createPollerRegistry();
		const withJira = base.with("jira", (config, gateway) => new StubPoller(config.name, gateway));

		// Original registry should not have the factory
		expect(base.get("jira")).toBeUndefined();
		// New registry should have it
		expect(withJira.get("jira")).toBeDefined();
	});

	it("throws when type not in registry", () => {
		const emptyRegistry = createPollerRegistry();
		const config: ConnectorConfig = {
			name: "sf-miss",
			type: "salesforce",
			salesforce: {
				instanceUrl: "https://test.salesforce.com",
				clientId: "cid",
				clientSecret: "csec",
				username: "user",
				password: "pass",
			},
		};
		expect(() => createPoller(config, noopGateway, emptyRegistry)).toThrow(
			/No poller factory registered/,
		);
	});

	it("supports chaining multiple .with() calls", () => {
		const registry = createPollerRegistry()
			.with("jira", (config, gateway) => new StubPoller(config.name, gateway))
			.with("salesforce", (config, gateway) => new StubPoller(config.name, gateway));

		expect(registry.get("jira")).toBeDefined();
		expect(registry.get("salesforce")).toBeDefined();
	});

	it("initialises from a pre-populated Map", () => {
		const factories = new Map<string, (config: ConnectorConfig, gateway: PushTarget) => StubPoller>(
			[["jira", (config, gateway) => new StubPoller(config.name, gateway)]],
		);
		const registry = createPollerRegistry(factories);
		expect(registry.get("jira")).toBeDefined();

		// Mutating the original Map should not affect the registry
		factories.delete("jira");
		expect(registry.get("jira")).toBeDefined();
	});
});
