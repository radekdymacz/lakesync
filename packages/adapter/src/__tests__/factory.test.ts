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

	it("returns Err when postgres config is missing postgres field", () => {
		const config = {
			name: "pg",
			type: "postgres",
		} as ConnectorConfig;
		const result = createDatabaseAdapter(config);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("postgres");
		}
	});

	it("returns Err when mysql config is missing mysql field", () => {
		const config = {
			name: "my",
			type: "mysql",
		} as ConnectorConfig;
		const result = createDatabaseAdapter(config);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("mysql");
		}
	});

	it("returns Err when bigquery config is missing bigquery field", () => {
		const config = {
			name: "bq",
			type: "bigquery",
		} as ConnectorConfig;
		const result = createDatabaseAdapter(config);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("bigquery");
		}
	});

	it("returns Err for unsupported connector type", () => {
		const config = {
			name: "redis",
			type: "redis" as ConnectorConfig["type"],
		} as ConnectorConfig;
		const result = createDatabaseAdapter(config);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Unsupported");
		}
	});
});
