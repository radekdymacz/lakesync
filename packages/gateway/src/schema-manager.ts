import { Err, Ok, type Result, type RowDelta, SchemaError, type TableSchema } from "@lakesync/core";

/** Immutable snapshot of schema state — swapped atomically on evolution. */
interface SchemaSnapshot {
	schema: TableSchema;
	version: number;
	allowedColumns: Set<string>;
}

/**
 * Manages schema versioning and validation for the gateway.
 *
 * Validates incoming deltas against the current schema and supports
 * safe schema evolution (adding nullable columns only). Schema, version,
 * and allowed columns are held in a single {@link SchemaSnapshot} that
 * is swapped atomically — no intermediate inconsistent state is possible.
 */
export class SchemaManager {
	private state: SchemaSnapshot;

	constructor(schema: TableSchema, version?: number) {
		this.state = {
			schema,
			version: version ?? 1,
			allowedColumns: new Set(schema.columns.map((c) => c.name)),
		};
	}

	/** Get the current schema and version. */
	getSchema(): { schema: TableSchema; version: number } {
		return { schema: this.state.schema, version: this.state.version };
	}

	/**
	 * Validate that a delta's columns are compatible with the current schema.
	 *
	 * Unknown columns result in a SchemaError. Missing columns are fine (sparse deltas).
	 * DELETE ops with empty columns are always valid.
	 */
	validateDelta(delta: RowDelta): Result<void, SchemaError> {
		if (delta.op === "DELETE" && delta.columns.length === 0) {
			return Ok(undefined);
		}

		for (const col of delta.columns) {
			if (!this.state.allowedColumns.has(col.column)) {
				return Err(
					new SchemaError(
						`Unknown column "${col.column}" in delta for table "${delta.table}". Schema version ${this.state.version} does not include this column.`,
					),
				);
			}
		}
		return Ok(undefined);
	}

	/**
	 * Evolve the schema by adding new nullable columns.
	 *
	 * Only adding columns is allowed. Removing columns or changing types
	 * returns a SchemaError.
	 */
	evolveSchema(newSchema: TableSchema): Result<{ version: number }, SchemaError> {
		if (newSchema.table !== this.state.schema.table) {
			return Err(new SchemaError("Cannot evolve schema: table name mismatch"));
		}

		const oldColumnMap = new Map(this.state.schema.columns.map((c) => [c.name, c.type]));
		const newColumnMap = new Map(newSchema.columns.map((c) => [c.name, c.type]));

		// Check for removed columns
		for (const [name] of oldColumnMap) {
			if (!newColumnMap.has(name)) {
				return Err(
					new SchemaError(
						`Cannot remove column "${name}" — only adding nullable columns is supported`,
					),
				);
			}
		}

		// Check for type changes
		for (const [name, oldType] of oldColumnMap) {
			const newType = newColumnMap.get(name);
			if (newType && newType !== oldType) {
				return Err(
					new SchemaError(
						`Cannot change type of column "${name}" from "${oldType}" to "${newType}"`,
					),
				);
			}
		}

		// Atomic swap — no intermediate inconsistent state
		const newVersion = this.state.version + 1;
		this.state = {
			schema: newSchema,
			version: newVersion,
			allowedColumns: new Set(newSchema.columns.map((c) => c.name)),
		};

		return Ok({ version: newVersion });
	}
}
