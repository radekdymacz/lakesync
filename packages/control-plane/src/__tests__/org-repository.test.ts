import type { Pool } from "pg";
import { beforeEach, describe, expect, it } from "vitest";
import { ControlPlaneError } from "../errors";
import { PgOrgRepository } from "../postgres/org-repository";
import { createMockPool, duplicateKeyError, mockOrgRow } from "./test-helpers";

describe("PgOrgRepository", () => {
	let repo: PgOrgRepository;
	let mock: ReturnType<typeof createMockPool>;

	beforeEach(() => {
		mock = createMockPool();
		repo = new PgOrgRepository(mock.pool as unknown as Pool);
	});

	describe("create", () => {
		it("creates an organisation and returns Ok", async () => {
			mock.queueResult([mockOrgRow({ name: "My Org", slug: "my-org" })]);

			const result = await repo.create({ name: "My Org", slug: "my-org" });
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.name).toBe("My Org");
				expect(result.value.slug).toBe("my-org");
				expect(result.value.plan).toBe("free");
			}
			expect(mock.mockQuery).toHaveBeenCalledOnce();
		});

		it("returns Err DUPLICATE on slug conflict", async () => {
			mock.mockQuery.mockRejectedValueOnce(duplicateKeyError());

			const result = await repo.create({ name: "Dup", slug: "existing" });
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error).toBeInstanceOf(ControlPlaneError);
				expect(result.error.code).toBe("DUPLICATE");
			}
		});

		it("defaults to free plan when none specified", async () => {
			mock.queueResult([mockOrgRow()]);

			const result = await repo.create({ name: "Test", slug: "test" });
			expect(result.ok).toBe(true);
			// Check that the query was called with "free" as the plan parameter
			const queryArgs = mock.mockQuery.mock.calls[0];
			expect(queryArgs?.[1]?.[3]).toBe("free");
		});

		it("accepts custom plan", async () => {
			mock.queueResult([mockOrgRow({ plan: "pro" })]);

			const result = await repo.create({ name: "Pro Org", slug: "pro-org", plan: "pro" });
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.plan).toBe("pro");
			}
		});
	});

	describe("getById", () => {
		it("returns the organisation when found", async () => {
			mock.queueResult([mockOrgRow()]);

			const result = await repo.getById("org_abc123");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).not.toBeNull();
				expect(result.value?.id).toBe("org_abc123");
			}
		});

		it("returns null when not found", async () => {
			mock.queueResult([]);

			const result = await repo.getById("nonexistent");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBeNull();
			}
		});
	});

	describe("getBySlug", () => {
		it("returns the organisation when found", async () => {
			mock.queueResult([mockOrgRow({ slug: "acme" })]);

			const result = await repo.getBySlug("acme");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value?.slug).toBe("acme");
			}
		});

		it("returns null when not found", async () => {
			mock.queueResult([]);

			const result = await repo.getBySlug("nonexistent");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBeNull();
			}
		});
	});

	describe("update", () => {
		it("updates name and returns updated org", async () => {
			mock.queueResult([mockOrgRow({ name: "Updated" })]);

			const result = await repo.update("org_abc123", { name: "Updated" });
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.name).toBe("Updated");
			}
		});

		it("returns NOT_FOUND when org does not exist", async () => {
			mock.queueResult([]);

			const result = await repo.update("nonexistent", { name: "X" });
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});

		it("returns DUPLICATE on slug conflict during update", async () => {
			mock.mockQuery.mockRejectedValueOnce(duplicateKeyError());

			const result = await repo.update("org_abc123", { slug: "taken" });
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("DUPLICATE");
			}
		});

		it("returns current org when no fields to update", async () => {
			mock.queueResult([mockOrgRow()]);

			const result = await repo.update("org_abc123", {});
			expect(result.ok).toBe(true);
		});
	});

	describe("delete", () => {
		it("deletes an organisation", async () => {
			mock.mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 1 });

			const result = await repo.delete("org_abc123");
			expect(result.ok).toBe(true);
		});

		it("returns NOT_FOUND when org does not exist", async () => {
			mock.mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });

			const result = await repo.delete("nonexistent");
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});
	});
});
