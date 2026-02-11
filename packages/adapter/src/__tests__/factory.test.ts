import type { ConnectorConfig } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { BigQueryAdapter } from "../bigquery";
import { createDatabaseAdapter } from "../factory";
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

	it("returns Err for jira connector type", () => {
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

	it("returns Err for salesforce connector type", () => {
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
});
