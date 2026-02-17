import { Ok } from "@lakesync/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ClerkWebhookDeps, ClerkWebhookEvent } from "../auth/clerk-webhook";
import { processClerkWebhook } from "../auth/clerk-webhook";
import { ControlPlaneError } from "../errors";
import { mockMemberRow, mockOrgRow } from "./test-helpers";

function createMockDeps(): ClerkWebhookDeps {
	return {
		orgRepo: {
			create: vi.fn().mockResolvedValue(Ok(toOrg(mockOrgRow()))),
			getById: vi.fn().mockResolvedValue(Ok(null)),
			getBySlug: vi.fn().mockResolvedValue(Ok(null)),
			update: vi.fn().mockResolvedValue(Ok(toOrg(mockOrgRow()))),
			delete: vi.fn().mockResolvedValue(Ok(undefined)),
		},
		memberRepo: {
			add: vi.fn().mockResolvedValue(Ok(toMember(mockMemberRow({ role: "owner" })))),
			remove: vi.fn().mockResolvedValue(Ok(undefined)),
			listByOrg: vi.fn().mockResolvedValue(Ok([])),
			getRole: vi.fn().mockResolvedValue(Ok("owner")),
			updateRole: vi.fn().mockResolvedValue(Ok(undefined)),
		},
	};
}

function toOrg(row: Record<string, unknown>) {
	return {
		id: row.id,
		name: row.name,
		slug: row.slug,
		plan: row.plan,
		createdAt: new Date(row.created_at as string),
		updatedAt: new Date(row.updated_at as string),
	};
}

function toMember(row: Record<string, unknown>) {
	return {
		orgId: row.org_id,
		userId: row.user_id,
		role: row.role,
		createdAt: new Date(row.created_at as string),
	};
}

describe("processClerkWebhook", () => {
	let deps: ClerkWebhookDeps;

	beforeEach(() => {
		deps = createMockDeps();
	});

	describe("user.created", () => {
		it("creates an org and adds user as owner", async () => {
			const event: ClerkWebhookEvent = {
				type: "user.created",
				data: {
					id: "user_123",
					email_addresses: [{ email_address: "alice@example.com" }],
					first_name: "Alice",
					last_name: "Smith",
				},
			};

			const result = await processClerkWebhook(event, deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.processed).toBe(true);
				expect(result.value.action).toContain("org + member created");
			}
			expect(deps.orgRepo.create).toHaveBeenCalledOnce();
			expect(deps.memberRepo.add).toHaveBeenCalledOnce();
		});

		it("retries with random suffix on slug collision", async () => {
			const deps = createMockDeps();
			(deps.orgRepo.create as ReturnType<typeof vi.fn>)
				.mockResolvedValueOnce({
					ok: false,
					error: new ControlPlaneError("duplicate", "DUPLICATE"),
				})
				.mockResolvedValueOnce(Ok(toOrg(mockOrgRow({ slug: "alice-smith-abc123" }))));

			const event: ClerkWebhookEvent = {
				type: "user.created",
				data: {
					id: "user_123",
					first_name: "Alice",
					last_name: "Smith",
				},
			};

			const result = await processClerkWebhook(event, deps);
			expect(result.ok).toBe(true);
			expect(deps.orgRepo.create).toHaveBeenCalledTimes(2);
		});

		it("uses user ID as fallback name", async () => {
			const event: ClerkWebhookEvent = {
				type: "user.created",
				data: { id: "user_456" },
			};

			const result = await processClerkWebhook(event, deps);
			expect(result.ok).toBe(true);
			const call = (deps.orgRepo.create as ReturnType<typeof vi.fn>).mock.calls[0]?.[0];
			expect(call.name).toContain("user_456");
		});
	});

	describe("user.deleted", () => {
		it("processes deletion event", async () => {
			const event: ClerkWebhookEvent = {
				type: "user.deleted",
				data: { id: "user_123" },
			};

			const result = await processClerkWebhook(event, deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.processed).toBe(true);
				expect(result.value.action).toContain("user.deleted");
			}
		});
	});

	describe("session.created", () => {
		it("processes session event as no-op", async () => {
			const event: ClerkWebhookEvent = {
				type: "session.created",
				data: { id: "sess_123", user_id: "user_123" },
			};

			const result = await processClerkWebhook(event, deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.processed).toBe(true);
				expect(result.value.action).toContain("no-op");
			}
		});
	});
});
