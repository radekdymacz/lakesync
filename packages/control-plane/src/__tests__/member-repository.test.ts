import type { Pool } from "pg";
import { beforeEach, describe, expect, it } from "vitest";
import { ControlPlaneError } from "../errors";
import { PgMemberRepository } from "../postgres/member-repository";
import { createMockPool, duplicateKeyError, mockMemberRow } from "./test-helpers";

describe("PgMemberRepository", () => {
	let repo: PgMemberRepository;
	let mock: ReturnType<typeof createMockPool>;

	beforeEach(() => {
		mock = createMockPool();
		repo = new PgMemberRepository(mock.pool as unknown as Pool);
	});

	describe("add", () => {
		it("adds a member and returns Ok", async () => {
			mock.queueResult([mockMemberRow({ role: "admin" })]);

			const result = await repo.add({
				orgId: "org_abc123",
				userId: "user_abc123",
				role: "admin",
			});
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.role).toBe("admin");
				expect(result.value.orgId).toBe("org_abc123");
				expect(result.value.userId).toBe("user_abc123");
			}
		});

		it("returns DUPLICATE when member already exists", async () => {
			mock.mockQuery.mockRejectedValueOnce(duplicateKeyError());

			const result = await repo.add({
				orgId: "org_abc123",
				userId: "user_abc123",
				role: "member",
			});
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("DUPLICATE");
			}
		});
	});

	describe("remove", () => {
		it("removes a member", async () => {
			mock.mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 1 });

			const result = await repo.remove("org_abc123", "user_abc123");
			expect(result.ok).toBe(true);
		});

		it("returns NOT_FOUND when member does not exist", async () => {
			mock.mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });

			const result = await repo.remove("org_abc123", "nonexistent");
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});
	});

	describe("listByOrg", () => {
		it("returns all members for an org", async () => {
			mock.queueResult([
				mockMemberRow({ user_id: "user_1", role: "owner" }),
				mockMemberRow({ user_id: "user_2", role: "member" }),
			]);

			const result = await repo.listByOrg("org_abc123");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(2);
				expect(result.value[0]?.role).toBe("owner");
				expect(result.value[1]?.role).toBe("member");
			}
		});

		it("returns empty array for org with no members", async () => {
			mock.queueResult([]);

			const result = await repo.listByOrg("org_empty");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(0);
			}
		});
	});

	describe("getRole", () => {
		it("returns role when member found", async () => {
			mock.queueResult([{ role: "admin" }]);

			const result = await repo.getRole("org_abc123", "user_abc123");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBe("admin");
			}
		});

		it("returns null when member not found", async () => {
			mock.queueResult([]);

			const result = await repo.getRole("org_abc123", "nonexistent");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBeNull();
			}
		});
	});

	describe("updateRole", () => {
		it("updates role successfully", async () => {
			mock.mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 1 });

			const result = await repo.updateRole("org_abc123", "user_abc123", "admin");
			expect(result.ok).toBe(true);
		});

		it("returns NOT_FOUND when member does not exist", async () => {
			mock.mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });

			const result = await repo.updateRole("org_abc123", "nonexistent", "admin");
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});
	});
});
