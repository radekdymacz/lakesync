import type { TableSchema } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { LocalDB } from "../local-db";
import { getSchema, migrateSchema, registerSchema } from "../schema-registry";

describe("SchemaRegistry", () => {
	const todoSchema: TableSchema = {
		table: "todos",
		columns: [
			{ name: "title", type: "string" },
			{ name: "completed", type: "boolean" },
			{ name: "priority", type: "number" },
		],
	};

	it("should register a schema, create the table, and retrieve it via getSchema", async () => {
		const dbResult = await LocalDB.open({ name: "test-register", backend: "memory" });
		expect(dbResult.ok).toBe(true);
		if (!dbResult.ok) return;
		const db = dbResult.value;

		try {
			// Register the schema
			const regResult = await registerSchema(db, todoSchema);
			expect(regResult.ok).toBe(true);

			// Verify the schema is retrievable
			const getResult = await getSchema(db, "todos");
			expect(getResult.ok).toBe(true);
			if (!getResult.ok) return;

			expect(getResult.value).not.toBeNull();
			expect(getResult.value?.table).toBe("todos");
			expect(getResult.value?.columns).toHaveLength(3);
			expect(getResult.value?.columns[0]).toEqual({ name: "title", type: "string" });
			expect(getResult.value?.columns[1]).toEqual({ name: "completed", type: "boolean" });
			expect(getResult.value?.columns[2]).toEqual({ name: "priority", type: "number" });

			// Verify the actual table was created by inserting a row
			const insertResult = await db.exec(
				"INSERT INTO todos (_rowId, title, completed, priority) VALUES (?, ?, ?, ?)",
				["row-1", "Buy milk", 0, 1.0],
			);
			expect(insertResult.ok).toBe(true);

			// Verify the row exists
			const rows = await db.query<{ _rowId: string; title: string }>(
				"SELECT _rowId, title FROM todos WHERE _rowId = ?",
				["row-1"],
			);
			expect(rows.ok).toBe(true);
			if (rows.ok) {
				expect(rows.value).toHaveLength(1);
				const firstRow = rows.value[0];
				expect(firstRow?.title).toBe("Buy milk");
			}
		} finally {
			await db.close();
		}
	});

	it("should be idempotent when registering the same schema twice", async () => {
		const dbResult = await LocalDB.open({ name: "test-idempotent", backend: "memory" });
		expect(dbResult.ok).toBe(true);
		if (!dbResult.ok) return;
		const db = dbResult.value;

		try {
			// Register twice
			const first = await registerSchema(db, todoSchema);
			expect(first.ok).toBe(true);

			const second = await registerSchema(db, todoSchema);
			expect(second.ok).toBe(true);

			// Schema should still be retrievable
			const getResult = await getSchema(db, "todos");
			expect(getResult.ok).toBe(true);
			if (getResult.ok) {
				expect(getResult.value).not.toBeNull();
				expect(getResult.value?.table).toBe("todos");
			}
		} finally {
			await db.close();
		}
	});

	it("should migrate schema by adding a column", async () => {
		const dbResult = await LocalDB.open({ name: "test-migrate-add", backend: "memory" });
		expect(dbResult.ok).toBe(true);
		if (!dbResult.ok) return;
		const db = dbResult.value;

		try {
			// Register initial schema
			const regResult = await registerSchema(db, todoSchema);
			expect(regResult.ok).toBe(true);

			// Define new schema with an added column
			const newSchema: TableSchema = {
				table: "todos",
				columns: [
					{ name: "title", type: "string" },
					{ name: "completed", type: "boolean" },
					{ name: "priority", type: "number" },
					{ name: "description", type: "string" },
				],
			};

			// Migrate
			const migrateResult = await migrateSchema(db, todoSchema, newSchema);
			expect(migrateResult.ok).toBe(true);

			// Verify updated schema is stored
			const getResult = await getSchema(db, "todos");
			expect(getResult.ok).toBe(true);
			if (getResult.ok && getResult.value) {
				expect(getResult.value.columns).toHaveLength(4);
				expect(getResult.value.columns[3]).toEqual({ name: "description", type: "string" });
			}

			// Verify schema_version was incremented
			const versionResult = await db.query<{ schema_version: number }>(
				"SELECT schema_version FROM _lakesync_meta WHERE table_name = ?",
				["todos"],
			);
			expect(versionResult.ok).toBe(true);
			if (versionResult.ok) {
				const firstRow = versionResult.value[0];
				expect(firstRow?.schema_version).toBe(2);
			}

			// Verify the new column exists in the actual table
			const insertResult = await db.exec(
				"INSERT INTO todos (_rowId, title, completed, priority, description) VALUES (?, ?, ?, ?, ?)",
				["row-1", "Buy milk", 0, 1.0, "From the corner shop"],
			);
			expect(insertResult.ok).toBe(true);
		} finally {
			await db.close();
		}
	});

	it("should return SchemaError when removing a column", async () => {
		const dbResult = await LocalDB.open({ name: "test-migrate-remove", backend: "memory" });
		expect(dbResult.ok).toBe(true);
		if (!dbResult.ok) return;
		const db = dbResult.value;

		try {
			// Register initial schema
			await registerSchema(db, todoSchema);

			// Attempt to remove the 'priority' column
			const newSchema: TableSchema = {
				table: "todos",
				columns: [
					{ name: "title", type: "string" },
					{ name: "completed", type: "boolean" },
				],
			};

			const migrateResult = await migrateSchema(db, todoSchema, newSchema);
			expect(migrateResult.ok).toBe(false);
			if (!migrateResult.ok) {
				expect(migrateResult.error.code).toBe("SCHEMA_MISMATCH");
				expect(migrateResult.error.message).toContain("Cannot remove column");
				expect(migrateResult.error.message).toContain("priority");
			}
		} finally {
			await db.close();
		}
	});

	it("should return SchemaError when changing a column type", async () => {
		const dbResult = await LocalDB.open({ name: "test-migrate-type", backend: "memory" });
		expect(dbResult.ok).toBe(true);
		if (!dbResult.ok) return;
		const db = dbResult.value;

		try {
			// Register initial schema
			await registerSchema(db, todoSchema);

			// Attempt to change 'priority' from number to string
			const newSchema: TableSchema = {
				table: "todos",
				columns: [
					{ name: "title", type: "string" },
					{ name: "completed", type: "boolean" },
					{ name: "priority", type: "string" },
				],
			};

			const migrateResult = await migrateSchema(db, todoSchema, newSchema);
			expect(migrateResult.ok).toBe(false);
			if (!migrateResult.ok) {
				expect(migrateResult.error.code).toBe("SCHEMA_MISMATCH");
				expect(migrateResult.error.message).toContain("Cannot change type");
				expect(migrateResult.error.message).toContain("priority");
			}
		} finally {
			await db.close();
		}
	});

	it("should return null for a non-existent table schema", async () => {
		const dbResult = await LocalDB.open({ name: "test-not-found", backend: "memory" });
		expect(dbResult.ok).toBe(true);
		if (!dbResult.ok) return;
		const db = dbResult.value;

		try {
			const getResult = await getSchema(db, "nonexistent");
			expect(getResult.ok).toBe(true);
			if (getResult.ok) {
				expect(getResult.value).toBeNull();
			}
		} finally {
			await db.close();
		}
	});

	it("should return SchemaError when table names do not match in migration", async () => {
		const dbResult = await LocalDB.open({ name: "test-mismatch", backend: "memory" });
		expect(dbResult.ok).toBe(true);
		if (!dbResult.ok) return;
		const db = dbResult.value;

		try {
			const otherSchema: TableSchema = {
				table: "other_table",
				columns: [{ name: "title", type: "string" }],
			};

			const migrateResult = await migrateSchema(db, todoSchema, otherSchema);
			expect(migrateResult.ok).toBe(false);
			if (!migrateResult.ok) {
				expect(migrateResult.error.code).toBe("SCHEMA_MISMATCH");
				expect(migrateResult.error.message).toContain("Table name mismatch");
			}
		} finally {
			await db.close();
		}
	});
});
