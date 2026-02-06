import { LocalDB, registerSchema } from "@lakesync/client";
import type { TableSchema } from "@lakesync/core";
import { unwrapOrThrow } from "@lakesync/core";

/** Todo item as stored in SQLite */
export interface Todo {
	_rowId: string;
	title: string;
	completed: number; // SQLite stores booleans as 0/1
	created_at: string;
	updated_at: string;
}

/** Schema definition for the todos table */
export const todoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
		{ name: "created_at", type: "string" },
		{ name: "updated_at", type: "string" },
	],
};

/** Initialise the local database and register the todo schema. */
export async function initDatabase(): Promise<LocalDB> {
	const result = await LocalDB.open({
		name: "lakesync-todos",
		backend: "idb",
	});
	const db = unwrapOrThrow(result);
	unwrapOrThrow(await registerSchema(db, todoSchema));
	return db;
}
