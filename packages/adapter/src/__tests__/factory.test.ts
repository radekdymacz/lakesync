import type { ConnectorConfig } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { BigQueryAdapter } from "../bigquery";
import {
	createAdapterFactoryRegistry,
	createDatabaseAdapter,
	defaultAdapterFactoryRegistry,
} from "../factory";
import { MySQLAdapter } from "../mysql";
import { PostgresAdapter } from "../postgres";

describe("createDatabaseAdapter", () => {
	it("returns Ok with PostgresAdapter for valid postgres config", () => {
		const config: ConnectorConfig = {
			name: "pg",
			type: "postgres",
			postgres: { connectionString: "postgres://localhost:5432/test" },
		};
		const result = createDatabaseAdapter(config);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toBeInstanceOf(PostgresAdapter);
		}
	});

	it("returns Ok with MySQLAdapter for valid mysql config", () => {
		const config: ConnectorConfig = {
			name: "my",
			type: "mysql",
			mysql: { connectionString: "mysql://localhost:3306/test" },
		};
		const result = createDatabaseAdapter(config);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toBeInstanceOf(MySQLAdapter);
		}
	});

	it("returns Ok with BigQueryAdapter for valid bigquery config", () => {
		const config: ConnectorConfig = {
			name: "bq",
			type: "bigquery",
			bigquery: { projectId: "my-project", dataset: "my_dataset" },
		};
		const result = createDatabaseAdapter(config);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toBeInstanceOf(BigQueryAdapter);
		}
	});

	it("returns Err for jira connector type (no adapter factory)", () => {
		const config: ConnectorConfig = {
			name: "jira",
			type: "jira",
			jira: { domain: "test", email: "a@b.com", apiToken: "tok" },
		};
		const result = createDatabaseAdapter(config);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("jira");
		}
	});

	it("returns Err for salesforce connector type (no adapter factory)", () => {
		const config: ConnectorConfig = {
			name: "sf",
			type: "salesforce",
			salesforce: {
				instanceUrl: "https://test.salesforce.com",
				clientId: "cid",
				clientSecret: "csec",
				username: "user",
				password: "pass",
			},
		};
		const result = createDatabaseAdapter(config);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("salesforce");
		}
	});

	it("accepts a custom registry", () => {
		const customRegistry = createAdapterFactoryRegistry().with(
			"postgres",
			(c) =>
				new PostgresAdapter({
					connectionString: (c as { postgres: { connectionString: string } }).postgres
						.connectionString,
				}),
		);
		const config: ConnectorConfig = {
			name: "custom-pg",
			type: "postgres",
			postgres: { connectionString: "postgres://localhost/custom" },
		};
		const result = createDatabaseAdapter(config, customRegistry);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toBeInstanceOf(PostgresAdapter);
		}
	});

	it("returns Err for unknown type when no factory registered", () => {
		const config: ConnectorConfig = {
			name: "custom",
			type: "custom-db",
		};
		const result = createDatabaseAdapter(config);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("custom-db");
		}
	});
});

describe("AdapterFactoryRegistry", () => {
	it("defaultAdapterFactoryRegistry includes postgres, mysql, bigquery", () => {
		const reg = defaultAdapterFactoryRegistry();
		expect(reg.get("postgres")).toBeDefined();
		expect(reg.get("mysql")).toBeDefined();
		expect(reg.get("bigquery")).toBeDefined();
		expect(reg.get("jira")).toBeUndefined();
	});

	it(".with() returns a new registry (immutability)", () => {
		const base = createAdapterFactoryRegistry();
		const withCustom = base.with("custom", () => ({}) as never);
		expect(base.get("custom")).toBeUndefined();
		expect(withCustom.get("custom")).toBeDefined();
	});
});
