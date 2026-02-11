import { describe, expect, it } from "vitest";
import {
	getConnectorDescriptor,
	listConnectorDescriptors,
	registerConnectorDescriptor,
	registerOutputSchemas,
} from "../registry";
import type { ConnectorType } from "../types";
// Side-effect import to ensure built-in descriptors are registered
import "../register-builtin";

describe("listConnectorDescriptors", () => {
	it("returns all 5 built-in types sorted alphabetically", () => {
		const descriptors = listConnectorDescriptors();
		const types = descriptors.map((d) => d.type);
		expect(types).toEqual(["bigquery", "jira", "mysql", "postgres", "salesforce"]);
	});
});

describe("getConnectorDescriptor", () => {
	it("returns the correct descriptor for postgres with all fields", () => {
		const descriptor = getConnectorDescriptor("postgres");
		expect(descriptor).toBeDefined();
		expect(descriptor!.type).toBe("postgres");
		expect(descriptor!.displayName).toBe("PostgreSQL");
		expect(descriptor!.description).toBe("PostgreSQL relational database connector.");
		expect(descriptor!.category).toBe("database");
		expect(descriptor!.configSchema).toBeDefined();
		expect(descriptor!.ingestSchema).toBeDefined();
		expect(descriptor!.outputTables).toBeNull();
	});

	it("returns undefined for an unknown type", () => {
		const descriptor = getConnectorDescriptor("unknown");
		expect(descriptor).toBeUndefined();
	});
});

describe("registerConnectorDescriptor", () => {
	it("registers a custom type that appears in the list", () => {
		registerConnectorDescriptor({
			type: "custom" as ConnectorType,
			displayName: "Custom",
			description: "A custom connector.",
			category: "database",
			configSchema: {
				$schema: "http://json-schema.org/draft-07/schema#",
				type: "object",
				properties: {},
				required: [],
			},
			ingestSchema: {
				$schema: "http://json-schema.org/draft-07/schema#",
				type: "object",
				properties: {},
			},
			outputTables: null,
		});

		const descriptor = getConnectorDescriptor("custom");
		expect(descriptor).toBeDefined();
		expect(descriptor!.displayName).toBe("Custom");

		const all = listConnectorDescriptors();
		const types = all.map((d) => d.type);
		expect(types).toContain("custom" as ConnectorType);
	});
});

describe("registerOutputSchemas", () => {
	it("attaches schemas to an existing registered type", () => {
		// Register a fresh descriptor to avoid polluting built-in state
		registerConnectorDescriptor({
			type: "test-output" as ConnectorType,
			displayName: "Test Output",
			description: "Descriptor for output schema test.",
			category: "api",
			configSchema: {
				$schema: "http://json-schema.org/draft-07/schema#",
				type: "object",
				properties: {},
				required: [],
			},
			ingestSchema: {
				$schema: "http://json-schema.org/draft-07/schema#",
				type: "object",
				properties: {},
			},
			outputTables: null,
		});

		const schemas = [
			{
				table: "items",
				columns: [{ name: "id", type: "string" as const }],
			},
		];

		registerOutputSchemas("test-output", schemas);

		const descriptor = getConnectorDescriptor("test-output");
		expect(descriptor).toBeDefined();
		expect(descriptor!.outputTables).toEqual(schemas);
	});

	it("is a no-op for an unknown type and does not throw", () => {
		expect(() => {
			registerOutputSchemas("nonexistent", [
				{ table: "t", columns: [{ name: "id", type: "string" as const }] },
			]);
		}).not.toThrow();
	});
});

describe("connector categories", () => {
	it.each([
		"postgres",
		"mysql",
		"bigquery",
	] as const)("%s has category 'database' and outputTables null", (type) => {
		const descriptor = getConnectorDescriptor(type);
		expect(descriptor).toBeDefined();
		expect(descriptor!.category).toBe("database");
		expect(descriptor!.outputTables).toBeNull();
	});

	it.each(["jira", "salesforce"] as const)("%s has category 'api'", (type) => {
		const descriptor = getConnectorDescriptor(type);
		expect(descriptor).toBeDefined();
		expect(descriptor!.category).toBe("api");
	});
});

describe("configSchema validity", () => {
	it("all built-in descriptors have valid configSchema with expected keys", () => {
		const builtInTypes = ["bigquery", "jira", "mysql", "postgres", "salesforce"];
		for (const type of builtInTypes) {
			const descriptor = getConnectorDescriptor(type);
			expect(descriptor, `descriptor for ${type} should exist`).toBeDefined();

			const schema = descriptor!.configSchema as Record<string, unknown>;
			expect(schema.$schema, `${type} configSchema should have $schema`).toBeDefined();
			expect(schema.type, `${type} configSchema should have type`).toBeDefined();
			expect(schema.properties, `${type} configSchema should have properties`).toBeDefined();
			expect(schema.required, `${type} configSchema should have required`).toBeDefined();
		}
	});
});
