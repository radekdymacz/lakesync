import { describe, expect, it } from "vitest";
import { LocalDB } from "../local-db";
import { DbError } from "../types";

describe("LocalDB", () => {
	it("open() + close() lifecycle completes without error", async () => {
		const result = await LocalDB.open({ name: "test-lifecycle" });

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		await result.value.close();
	});

	it("exec() creates a table and inserts a row", async () => {
		const openResult = await LocalDB.open({ name: "test-exec" });
		expect(openResult.ok).toBe(true);
		if (!openResult.ok) return;
		const db = openResult.value;

		const createResult = await db.exec(
			"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
		);
		expect(createResult.ok).toBe(true);

		const insertResult = await db.exec("INSERT INTO items (id, name) VALUES (?, ?)", [1, "first"]);
		expect(insertResult.ok).toBe(true);

		await db.close();
	});

	it("query<T>() returns typed rows as objects", async () => {
		const openResult = await LocalDB.open({ name: "test-query" });
		expect(openResult.ok).toBe(true);
		if (!openResult.ok) return;
		const db = openResult.value;

		await db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, active INTEGER)");
		await db.exec("INSERT INTO users (id, email, active) VALUES (?, ?, ?)", [
			1,
			"alice@example.com",
			1,
		]);
		await db.exec("INSERT INTO users (id, email, active) VALUES (?, ?, ?)", [
			2,
			"bob@example.com",
			0,
		]);

		interface User {
			id: number;
			email: string;
			active: number;
		}

		const queryResult = await db.query<User>("SELECT id, email, active FROM users ORDER BY id");
		expect(queryResult.ok).toBe(true);
		if (!queryResult.ok) return;

		expect(queryResult.value).toHaveLength(2);
		expect(queryResult.value[0]).toEqual({ id: 1, email: "alice@example.com", active: 1 });
		expect(queryResult.value[1]).toEqual({ id: 2, email: "bob@example.com", active: 0 });

		await db.close();
	});

	it("query<T>() returns empty array for no results", async () => {
		const openResult = await LocalDB.open({ name: "test-empty-query" });
		expect(openResult.ok).toBe(true);
		if (!openResult.ok) return;
		const db = openResult.value;

		await db.exec("CREATE TABLE empty_table (id INTEGER PRIMARY KEY)");

		const queryResult = await db.query<{ id: number }>("SELECT * FROM empty_table");
		expect(queryResult.ok).toBe(true);
		if (!queryResult.ok) return;
		expect(queryResult.value).toEqual([]);

		await db.close();
	});

	it("transaction() commits changes on success", async () => {
		const openResult = await LocalDB.open({ name: "test-tx-commit" });
		expect(openResult.ok).toBe(true);
		if (!openResult.ok) return;
		const db = openResult.value;

		await db.exec("CREATE TABLE counters (name TEXT PRIMARY KEY, value INTEGER)");

		const txResult = await db.transaction((tx) => {
			tx.exec("INSERT INTO counters (name, value) VALUES (?, ?)", ["hits", 0]);
			tx.exec("UPDATE counters SET value = ? WHERE name = ?", [42, "hits"]);
			return "done";
		});

		expect(txResult.ok).toBe(true);
		if (!txResult.ok) return;
		expect(txResult.value).toBe("done");

		// Verify the committed data persists outside the transaction
		const queryResult = await db.query<{ name: string; value: number }>(
			"SELECT name, value FROM counters",
		);
		expect(queryResult.ok).toBe(true);
		if (!queryResult.ok) return;
		expect(queryResult.value).toHaveLength(1);
		expect(queryResult.value[0]).toEqual({ name: "hits", value: 42 });

		await db.close();
	});

	it("transaction() rolls back on error", async () => {
		const openResult = await LocalDB.open({ name: "test-tx-rollback" });
		expect(openResult.ok).toBe(true);
		if (!openResult.ok) return;
		const db = openResult.value;

		await db.exec("CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT)");
		await db.exec("INSERT INTO logs (id, msg) VALUES (?, ?)", [1, "before-tx"]);

		const txResult = await db.transaction((tx) => {
			tx.exec("INSERT INTO logs (id, msg) VALUES (?, ?)", [2, "inside-tx"]);
			throw new Error("Simulated failure");
		});

		expect(txResult.ok).toBe(false);
		if (txResult.ok) return;
		expect(txResult.error).toBeInstanceOf(DbError);
		expect(txResult.error.message).toBe("Transaction failed");

		// Verify the row inserted inside the failed transaction was rolled back
		const queryResult = await db.query<{ id: number; msg: string }>(
			"SELECT id, msg FROM logs ORDER BY id",
		);
		expect(queryResult.ok).toBe(true);
		if (!queryResult.ok) return;
		expect(queryResult.value).toHaveLength(1);
		expect(queryResult.value[0]).toEqual({ id: 1, msg: "before-tx" });

		await db.close();
	});

	it("returns DbError for invalid SQL", async () => {
		const openResult = await LocalDB.open({ name: "test-invalid-sql" });
		expect(openResult.ok).toBe(true);
		if (!openResult.ok) return;
		const db = openResult.value;

		const execResult = await db.exec("INVALID SQL STATEMENT");
		expect(execResult.ok).toBe(false);
		if (execResult.ok) return;
		expect(execResult.error).toBeInstanceOf(DbError);
		expect(execResult.error.code).toBe("DB_ERROR");

		const queryResult = await db.query("SELECT * FROM nonexistent_table");
		expect(queryResult.ok).toBe(false);
		if (queryResult.ok) return;
		expect(queryResult.error).toBeInstanceOf(DbError);
		expect(queryResult.error.code).toBe("DB_ERROR");

		await db.close();
	});

	it("handles concurrent query() calls without deadlock", async () => {
		const openResult = await LocalDB.open({ name: "test-concurrent" });
		expect(openResult.ok).toBe(true);
		if (!openResult.ok) return;
		const db = openResult.value;

		await db.exec("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)");
		await db.exec("INSERT INTO data (id, val) VALUES (?, ?)", [1, "alpha"]);
		await db.exec("INSERT INTO data (id, val) VALUES (?, ?)", [2, "beta"]);
		await db.exec("INSERT INTO data (id, val) VALUES (?, ?)", [3, "gamma"]);

		// Fire multiple queries concurrently
		const [r1, r2, r3] = await Promise.all([
			db.query<{ id: number; val: string }>("SELECT * FROM data WHERE id = ?", [1]),
			db.query<{ id: number; val: string }>("SELECT * FROM data WHERE id = ?", [2]),
			db.query<{ id: number; val: string }>("SELECT * FROM data ORDER BY id"),
		]);

		expect(r1.ok).toBe(true);
		expect(r2.ok).toBe(true);
		expect(r3.ok).toBe(true);

		if (!r1.ok || !r2.ok || !r3.ok) return;

		expect(r1.value).toHaveLength(1);
		expect(r1.value[0]?.val).toBe("alpha");

		expect(r2.value).toHaveLength(1);
		expect(r2.value[0]?.val).toBe("beta");

		expect(r3.value).toHaveLength(3);

		await db.close();
	});

	it("transaction query() returns rows correctly", async () => {
		const openResult = await LocalDB.open({ name: "test-tx-query" });
		expect(openResult.ok).toBe(true);
		if (!openResult.ok) return;
		const db = openResult.value;

		await db.exec("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)");
		await db.exec("INSERT INTO kv (key, value) VALUES (?, ?)", ["a", "1"]);

		const txResult = await db.transaction((tx) => {
			tx.exec("INSERT INTO kv (key, value) VALUES (?, ?)", ["b", "2"]);
			const result = tx.query<{ key: string; value: string }>(
				"SELECT key, value FROM kv ORDER BY key",
			);
			if (!result.ok) throw new Error("Query inside transaction failed");
			return result.value;
		});

		expect(txResult.ok).toBe(true);
		if (!txResult.ok) return;
		expect(txResult.value).toHaveLength(2);
		expect(txResult.value[0]).toEqual({ key: "a", value: "1" });
		expect(txResult.value[1]).toEqual({ key: "b", value: "2" });

		await db.close();
	});
});
