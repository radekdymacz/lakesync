import type { TableSchema } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { LocalDB } from "../../db/local-db";
import { registerSchema } from "../../db/schema-registry";
import { SchemaSynchroniser } from "../schema-sync";

describe("SchemaSynchroniser", () => {
	const baseSchema: TableSchema = {
		table: "todos",
		columns: [
			{ name: "title", type: "string" },
			{ name: "completed", type: "boolean" },
		],
	};

	/** Helper: open a fresh in-memory database, register the base schema, and return both */
	async function setup() {
		const dbResult = await LocalDB.open({ name: "test-schema-sync", backend: "memory" });
		expect(dbResult.ok).toBe(true);
		if (!dbResult.ok) throw new Error("Failed to open DB");
		const db = dbResult.value;

		const regResult = await registerSchema(db, baseSchema);
		expect(regResult.ok).toBe(true);

		return db;
	}

	it("should apply ALTER TABLE when server adds a column", async () => {
		const db = await setup();

		try {
			const serverSchema: TableSchema = {
				table: "todos",
				columns: [
					{ name: "title", type: "string" },
					{ name: "completed", type: "boolean" },
					{ name: "priority", type: "number" },
				],
			};

			const syncer = new SchemaSynchroniser(db);
			const result = await syncer.synchronise("todos", serverSchema, 2);
			expect(result.ok).toBe(true);

			// Verify the new column is usable
			const insertResult = await db.exec(
				"INSERT INTO todos (_rowId, title, completed, priority) VALUES (?, ?, ?, ?)",
				["row-1", "Buy milk", 0, 1.5],
			);
			expect(insertResult.ok).toBe(true);

			// Query back the row to confirm the column works
			const queryResult = await db.query<{
				_rowId: string;
				title: string;
				priority: number;
			}>("SELECT _rowId, title, priority FROM todos WHERE _rowId = ?", ["row-1"]);
			expect(queryResult.ok).toBe(true);
			if (queryResult.ok) {
				expect(queryResult.value).toHaveLength(1);
				expect(queryResult.value[0]?.priority).toBe(1.5);
			}

			// Verify version was updated in _lakesync_meta
			const versionResult = await db.query<{ schema_version: number }>(
				"SELECT schema_version FROM _lakesync_meta WHERE table_name = ?",
				["todos"],
			);
			expect(versionResult.ok).toBe(true);
			if (versionResult.ok) {
				expect(versionResult.value[0]?.schema_version).toBe(2);
			}
		} finally {
			await db.close();
		}
	});

	it("should noop when local version equals server version", async () => {
		const db = await setup();

		try {
			const syncer = new SchemaSynchroniser(db);

			// Local version is 1 (from registerSchema), server version is also 1
			const result = await syncer.synchronise("todos", baseSchema, 1);
			expect(result.ok).toBe(true);

			// Verify schema is unchanged — still only 2 columns
			const versionResult = await db.query<{ schema_version: number }>(
				"SELECT schema_version FROM _lakesync_meta WHERE table_name = ?",
				["todos"],
			);
			expect(versionResult.ok).toBe(true);
			if (versionResult.ok) {
				expect(versionResult.value[0]?.schema_version).toBe(1);
			}
		} finally {
			await db.close();
		}
	});

	it("should noop when local version is ahead of server version", async () => {
		const db = await setup();

		try {
			// First, advance the local version by syncing to version 2
			const serverSchemaV2: TableSchema = {
				table: "todos",
				columns: [
					{ name: "title", type: "string" },
					{ name: "completed", type: "boolean" },
					{ name: "priority", type: "number" },
				],
			};

			const syncer = new SchemaSynchroniser(db);
			const firstSync = await syncer.synchronise("todos", serverSchemaV2, 2);
			expect(firstSync.ok).toBe(true);

			// Now try to sync with an older server version — should be a noop
			const result = await syncer.synchronise("todos", baseSchema, 1);
			expect(result.ok).toBe(true);

			// Version should still be 2
			const versionResult = await db.query<{ schema_version: number }>(
				"SELECT schema_version FROM _lakesync_meta WHERE table_name = ?",
				["todos"],
			);
			expect(versionResult.ok).toBe(true);
			if (versionResult.ok) {
				expect(versionResult.value[0]?.schema_version).toBe(2);
			}
		} finally {
			await db.close();
		}
	});

	it("should return error when server removes a column", async () => {
		const db = await setup();

		try {
			// Server schema that removes the 'completed' column
			const serverSchema: TableSchema = {
				table: "todos",
				columns: [{ name: "title", type: "string" }],
			};

			const syncer = new SchemaSynchroniser(db);
			const result = await syncer.synchronise("todos", serverSchema, 2);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("SCHEMA_MISMATCH");
				expect(result.error.message).toContain("Cannot remove column");
				expect(result.error.message).toContain("completed");
			}
		} finally {
			await db.close();
		}
	});

	it("should roll back migration on failure within transaction", async () => {
		const db = await setup();

		try {
			// Attempt a migration that will fail — changing a column type
			const serverSchema: TableSchema = {
				table: "todos",
				columns: [
					{ name: "title", type: "number" },
					{ name: "completed", type: "boolean" },
				],
			};

			const syncer = new SchemaSynchroniser(db);
			const result = await syncer.synchronise("todos", serverSchema, 2);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("SCHEMA_MISMATCH");
				expect(result.error.message).toContain("Cannot change type");
			}

			// Verify the schema version was NOT changed (rollback)
			const versionResult = await db.query<{ schema_version: number }>(
				"SELECT schema_version FROM _lakesync_meta WHERE table_name = ?",
				["todos"],
			);
			expect(versionResult.ok).toBe(true);
			if (versionResult.ok) {
				expect(versionResult.value[0]?.schema_version).toBe(1);
			}
		} finally {
			await db.close();
		}
	});

	it("should return error when no local schema is registered", async () => {
		const dbResult = await LocalDB.open({ name: "test-no-schema", backend: "memory" });
		expect(dbResult.ok).toBe(true);
		if (!dbResult.ok) return;
		const db = dbResult.value;

		try {
			const serverSchema: TableSchema = {
				table: "todos",
				columns: [{ name: "title", type: "string" }],
			};

			const syncer = new SchemaSynchroniser(db);

			// _lakesync_meta table does not exist, so getLocalVersion returns 0
			// and getSchema returns null → error
			const result = await syncer.synchronise("todos", serverSchema, 1);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("SCHEMA_MISMATCH");
				expect(result.error.message).toContain("no local schema registered");
			}
		} finally {
			await db.close();
		}
	});

	it("should handle multi-version jumps by setting exact server version", async () => {
		const db = await setup();

		try {
			// Server is at version 5, but local is at version 1
			const serverSchema: TableSchema = {
				table: "todos",
				columns: [
					{ name: "title", type: "string" },
					{ name: "completed", type: "boolean" },
					{ name: "priority", type: "number" },
					{ name: "description", type: "string" },
				],
			};

			const syncer = new SchemaSynchroniser(db);
			const result = await syncer.synchronise("todos", serverSchema, 5);
			expect(result.ok).toBe(true);

			// Version should be set to exactly 5, not incremented by 1
			const versionResult = await db.query<{ schema_version: number }>(
				"SELECT schema_version FROM _lakesync_meta WHERE table_name = ?",
				["todos"],
			);
			expect(versionResult.ok).toBe(true);
			if (versionResult.ok) {
				expect(versionResult.value[0]?.schema_version).toBe(5);
			}
		} finally {
			await db.close();
		}
	});
});
