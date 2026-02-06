import {
	Err,
	Ok,
	type Result,
	type RowDelta,
	SchemaError,
	type TableSchema,
} from "@lakesync/core";

/**
 * Manages schema versioning and validation for the gateway.
 *
 * Validates incoming deltas against the current schema and supports
 * safe schema evolution (adding nullable columns only).
 */
export class SchemaManager {
	private currentSchema: TableSchema;
	private version: number;
	private allowedColumns: Set<string>;

	constructor(schema: TableSchema, version?: number) {
		this.currentSchema = schema;
		this.version = version ?? 1;
		this.allowedColumns = new Set(schema.columns.map((c) => c.name));
	}

	/** Get the current schema and version. */
	getSchema(): { schema: TableSchema; version: number } {
		return { schema: this.currentSchema, version: this.version };
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
			if (!this.allowedColumns.has(col.column)) {
				return Err(
					new SchemaError(
						`Unknown column "${col.column}" in delta for table "${delta.table}". Schema version ${this.version} does not include this column.`,
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
	evolveSchema(
		newSchema: TableSchema,
	): Result<{ version: number }, SchemaError> {
		if (newSchema.table !== this.currentSchema.table) {
			return Err(new SchemaError("Cannot evolve schema: table name mismatch"));
		}

		const oldColumnMap = new Map(
			this.currentSchema.columns.map((c) => [c.name, c.type]),
		);
		const newColumnMap = new Map(
			newSchema.columns.map((c) => [c.name, c.type]),
		);

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

		// Apply evolution
		this.currentSchema = newSchema;
		this.version++;
		this.allowedColumns = new Set(newSchema.columns.map((c) => c.name));

		return Ok({ version: this.version });
	}
}
