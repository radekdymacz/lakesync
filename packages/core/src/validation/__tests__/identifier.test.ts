import { describe, expect, it } from "vitest";
import { SchemaError } from "../../result/errors";
import { assertValidIdentifier, isValidIdentifier, quoteIdentifier } from "../identifier";

describe("isValidIdentifier", () => {
	it("accepts valid identifiers", () => {
		expect(isValidIdentifier("users")).toBe(true);
		expect(isValidIdentifier("_private")).toBe(true);
		expect(isValidIdentifier("col_1")).toBe(true);
		expect(isValidIdentifier("A")).toBe(true);
		expect(isValidIdentifier("_")).toBe(true);
	});

	it("accepts exactly 64 characters", () => {
		expect(isValidIdentifier("a".repeat(64))).toBe(true);
	});

	it("rejects empty string", () => {
		expect(isValidIdentifier("")).toBe(false);
	});

	it("rejects identifiers starting with a digit", () => {
		expect(isValidIdentifier("1starts_with_digit")).toBe(false);
	});

	it("rejects identifiers with hyphens, dots, or spaces", () => {
		expect(isValidIdentifier("has-hyphen")).toBe(false);
		expect(isValidIdentifier("has.dot")).toBe(false);
		expect(isValidIdentifier("has space")).toBe(false);
	});

	it("rejects identifiers longer than 64 characters", () => {
		expect(isValidIdentifier("a".repeat(65))).toBe(false);
	});

	it("rejects SQL injection attempts", () => {
		expect(isValidIdentifier("'; DROP TABLE users--")).toBe(false);
		expect(isValidIdentifier("users; DELETE FROM")).toBe(false);
		expect(isValidIdentifier("1 OR 1=1")).toBe(false);
		expect(isValidIdentifier("table`name")).toBe(false);
		expect(isValidIdentifier('table"name')).toBe(false);
	});

	it("rejects special characters", () => {
		expect(isValidIdentifier("col@name")).toBe(false);
		expect(isValidIdentifier("col#name")).toBe(false);
		expect(isValidIdentifier("col$name")).toBe(false);
		expect(isValidIdentifier("col%name")).toBe(false);
	});

	it("rejects non-ASCII characters", () => {
		expect(isValidIdentifier("日本語")).toBe(false);
	});
});

describe("assertValidIdentifier", () => {
	it("returns Ok for valid identifiers", () => {
		const result = assertValidIdentifier("users");
		expect(result.ok).toBe(true);
	});

	it("returns Err(SchemaError) for invalid identifiers", () => {
		const result = assertValidIdentifier("has-hyphen");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(SchemaError);
		}
	});

	it("error message includes the invalid identifier name", () => {
		const result = assertValidIdentifier("bad name!");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("bad name!");
		}
	});

	it("SchemaError has correct code property", () => {
		const result = assertValidIdentifier("");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("SCHEMA_MISMATCH");
			expect(result.error.name).toBe("SchemaError");
		}
	});
});

describe("quoteIdentifier", () => {
	it("wraps a simple name in double quotes", () => {
		expect(quoteIdentifier("users")).toBe('"users"');
	});

	it("escapes embedded double-quote by doubling it", () => {
		expect(quoteIdentifier('user"name')).toBe('"user""name"');
	});

	it("escapes multiple embedded double-quotes", () => {
		expect(quoteIdentifier('a"b"c')).toBe('"a""b""c"');
	});

	it("handles empty string", () => {
		expect(quoteIdentifier("")).toBe('""');
	});

	it("wraps an already-safe name in double quotes", () => {
		expect(quoteIdentifier("simple")).toBe('"simple"');
	});
});
