import { describe, expect, it } from "vitest";
import { validateConnectorConfig } from "../validate";

const validPostgres = {
	name: "my-pg",
	type: "postgres",
	postgres: { connectionString: "postgres://localhost/test" },
};

const validMySQL = {
	name: "my-mysql",
	type: "mysql",
	mysql: { connectionString: "mysql://localhost/test" },
};

const validBigQuery = {
	name: "my-bq",
	type: "bigquery",
	bigquery: { projectId: "my-project", dataset: "my_dataset" },
};

const validWithCursorIngest = {
	name: "pg-ingest",
	type: "postgres",
	postgres: { connectionString: "postgres://localhost/test" },
	ingest: {
		tables: [
			{
				table: "users",
				query: "SELECT * FROM users",
				strategy: { type: "cursor", cursorColumn: "updated_at" },
			},
		],
		intervalMs: 5000,
	},
};

const validWithDiffIngest = {
	name: "pg-diff",
	type: "postgres",
	postgres: { connectionString: "postgres://localhost/test" },
	ingest: {
		tables: [
			{
				table: "orders",
				query: "SELECT * FROM orders",
				strategy: { type: "diff" },
			},
		],
	},
};

describe("validateConnectorConfig", () => {
	it("accepts valid postgres config", () => {
		const result = validateConnectorConfig(validPostgres);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.name).toBe("my-pg");
			expect(result.value.type).toBe("postgres");
		}
	});

	it("accepts valid mysql config", () => {
		const result = validateConnectorConfig(validMySQL);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.name).toBe("my-mysql");
			expect(result.value.type).toBe("mysql");
		}
	});

	it("accepts valid bigquery config", () => {
		const result = validateConnectorConfig(validBigQuery);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.name).toBe("my-bq");
			expect(result.value.type).toBe("bigquery");
		}
	});

	it("accepts config with ingest (cursor strategy)", () => {
		const result = validateConnectorConfig(validWithCursorIngest);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.ingest).toBeDefined();
			expect(result.value.ingest!.tables[0]!.strategy.type).toBe("cursor");
		}
	});

	it("accepts config with ingest (diff strategy)", () => {
		const result = validateConnectorConfig(validWithDiffIngest);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.ingest!.tables[0]!.strategy.type).toBe("diff");
		}
	});

	it("rejects non-object input", () => {
		expect(validateConnectorConfig(null).ok).toBe(false);
		expect(validateConnectorConfig(42).ok).toBe(false);
		expect(validateConnectorConfig("string").ok).toBe(false);
		expect(validateConnectorConfig(undefined).ok).toBe(false);
	});

	it("rejects missing name", () => {
		const result = validateConnectorConfig({
			type: "postgres",
			postgres: { connectionString: "x" },
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("name");
		}
	});

	it("rejects empty name", () => {
		const result = validateConnectorConfig({
			name: "",
			type: "postgres",
			postgres: { connectionString: "x" },
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("name");
		}
	});

	it("rejects invalid type", () => {
		const result = validateConnectorConfig({ name: "x", type: "redis" });
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("type");
		}
	});

	it("rejects postgres type without postgres config", () => {
		const result = validateConnectorConfig({ name: "x", type: "postgres" });
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("postgres");
		}
	});

	it("rejects mysql type without mysql config", () => {
		const result = validateConnectorConfig({ name: "x", type: "mysql" });
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("mysql");
		}
	});

	it("rejects bigquery type without bigquery config", () => {
		const result = validateConnectorConfig({ name: "x", type: "bigquery" });
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("bigquery");
		}
	});

	it("rejects postgres with empty connectionString", () => {
		const result = validateConnectorConfig({
			name: "x",
			type: "postgres",
			postgres: { connectionString: "" },
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("connectionString");
		}
	});

	it("rejects bigquery with missing projectId", () => {
		const result = validateConnectorConfig({
			name: "x",
			type: "bigquery",
			bigquery: { dataset: "ds" },
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("projectId");
		}
	});

	it("rejects bigquery with missing dataset", () => {
		const result = validateConnectorConfig({
			name: "x",
			type: "bigquery",
			bigquery: { projectId: "proj" },
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("dataset");
		}
	});

	it("rejects ingest with empty tables array", () => {
		const result = validateConnectorConfig({
			name: "x",
			type: "postgres",
			postgres: { connectionString: "postgres://localhost/test" },
			ingest: { tables: [] },
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("tables");
		}
	});

	it("rejects ingest table with missing query", () => {
		const result = validateConnectorConfig({
			name: "x",
			type: "postgres",
			postgres: { connectionString: "postgres://localhost/test" },
			ingest: {
				tables: [{ table: "users", query: "", strategy: { type: "diff" } }],
			},
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("query");
		}
	});

	it("rejects ingest cursor strategy without cursorColumn", () => {
		const result = validateConnectorConfig({
			name: "x",
			type: "postgres",
			postgres: { connectionString: "postgres://localhost/test" },
			ingest: {
				tables: [
					{
						table: "users",
						query: "SELECT * FROM users",
						strategy: { type: "cursor" },
					},
				],
			},
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("cursorColumn");
		}
	});

	it("rejects invalid ingest strategy type", () => {
		const result = validateConnectorConfig({
			name: "x",
			type: "postgres",
			postgres: { connectionString: "postgres://localhost/test" },
			ingest: {
				tables: [
					{
						table: "users",
						query: "SELECT * FROM users",
						strategy: { type: "snapshot" },
					},
				],
			},
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("strategy type");
		}
	});

	it("rejects negative intervalMs", () => {
		const result = validateConnectorConfig({
			name: "x",
			type: "postgres",
			postgres: { connectionString: "postgres://localhost/test" },
			ingest: {
				tables: [
					{
						table: "users",
						query: "SELECT * FROM users",
						strategy: { type: "diff" },
					},
				],
				intervalMs: -1,
			},
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("intervalMs");
		}
	});
});
