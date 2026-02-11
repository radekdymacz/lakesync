import { beforeEach, describe, expect, it } from "vitest";
import {
	BaseSourcePoller,
	type ConnectorConfig,
	createPoller,
	type PushTarget,
	registerPollerFactory,
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("createPoller", () => {
	beforeEach(() => {
		// Register stub factories that mirror the connector pattern.
		registerPollerFactory("jira", (config, gateway) => {
			return new StubPoller(config.name, gateway);
		});
		registerPollerFactory("salesforce", (config, gateway) => {
			return new StubPoller(config.name, gateway);
		});
	});

	it("creates a poller for a registered jira connector", () => {
		const config: ConnectorConfig = {
			name: "my-jira",
			type: "jira",
			jira: { domain: "test", email: "a@b.com", apiToken: "tok" },
		};
		const poller = createPoller(config, noopGateway);
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
		const poller = createPoller(config, noopGateway);
		expect(poller).toBeInstanceOf(BaseSourcePoller);
		expect((poller as StubPoller).connectorName).toBe("my-sf");
	});

	it("throws a descriptive error for an unregistered connector type", () => {
		const config: ConnectorConfig = {
			name: "unknown-src",
			type: "postgres",
			postgres: { connectionString: "postgres://localhost/db" },
		};
		// Postgres has no registered poller factory in this test
		// (it uses the database adapter path, not a source poller)
		expect(() => createPoller(config, noopGateway)).toThrow(
			/No poller factory registered for connector type "postgres"/,
		);
	});

	it("error message suggests importing the connector package", () => {
		const config: ConnectorConfig = {
			name: "missing",
			type: "bigquery",
			bigquery: { projectId: "p", dataset: "d" },
		};
		expect(() => createPoller(config, noopGateway)).toThrow(/Did you import the connector package/);
	});

	it("later registrations overwrite earlier ones", () => {
		let called = "";
		registerPollerFactory("jira", (config, gateway) => {
			called = "v2";
			return new StubPoller(config.name, gateway);
		});
		const config: ConnectorConfig = {
			name: "jira-v2",
			type: "jira",
			jira: { domain: "test", email: "a@b.com", apiToken: "tok" },
		};
		createPoller(config, noopGateway);
		expect(called).toBe("v2");
	});
});
