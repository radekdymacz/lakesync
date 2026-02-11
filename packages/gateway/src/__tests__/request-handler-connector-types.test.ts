import { describe, expect, it } from "vitest";
import { handleListConnectorTypes } from "../request-handler";
// Side-effect import to ensure built-in descriptors are registered
import "@lakesync/core";

describe("handleListConnectorTypes", () => {
	it("returns status 200 with an array body", () => {
		const result = handleListConnectorTypes();

		expect(result.status).toBe(200);
		expect(Array.isArray(result.body)).toBe(true);
	});

	it("contains all 5 built-in connector types", () => {
		const result = handleListConnectorTypes();
		const body = result.body as unknown[];

		expect(body.length).toBeGreaterThanOrEqual(5);
	});

	it("each descriptor has the expected fields", () => {
		const result = handleListConnectorTypes();
		const body = result.body as Array<Record<string, unknown>>;

		for (const descriptor of body) {
			expect(descriptor).toHaveProperty("type");
			expect(descriptor).toHaveProperty("displayName");
			expect(descriptor).toHaveProperty("description");
			expect(descriptor).toHaveProperty("category");
			expect(descriptor).toHaveProperty("configSchema");
			expect(descriptor).toHaveProperty("ingestSchema");

			expect(typeof descriptor.type).toBe("string");
			expect(typeof descriptor.displayName).toBe("string");
			expect(typeof descriptor.description).toBe("string");
			expect(typeof descriptor.category).toBe("string");
			expect(typeof descriptor.configSchema).toBe("object");
			expect(typeof descriptor.ingestSchema).toBe("object");
		}
	});
});
