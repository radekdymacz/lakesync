import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { parseArgs } from "../args";

describe("bin command dispatch", () => {
	let mockStdout: string[];
	let mockStderr: string[];

	beforeEach(() => {
		mockStdout = [];
		mockStderr = [];

		vi.spyOn(process.stdout, "write").mockImplementation((data) => {
			mockStdout.push(String(data));
			return true;
		});
		vi.spyOn(process.stderr, "write").mockImplementation((data) => {
			mockStderr.push(String(data));
			return true;
		});
	});

	afterEach(() => {
		vi.restoreAllMocks();
	});

	it("parses gateways list as two-word command", () => {
		const result = parseArgs(["node", "lakesync", "gateways", "list", "--org", "org-1"]);
		expect(result.command).toEqual(["gateways", "list"]);
		expect(result.flags.org).toBe("org-1");
	});

	it("parses gateways create with flags", () => {
		const result = parseArgs([
			"node",
			"lakesync",
			"gateways",
			"create",
			"--name",
			"prod",
			"--region",
			"us-east-1",
		]);
		expect(result.command).toEqual(["gateways", "create"]);
		expect(result.flags.name).toBe("prod");
		expect(result.flags.region).toBe("us-east-1");
	});

	it("parses gateways delete with --id flag", () => {
		const result = parseArgs(["node", "lakesync", "gateways", "delete", "--id", "gw-123"]);
		expect(result.command).toEqual(["gateways", "delete"]);
		expect(result.flags.id).toBe("gw-123");
	});

	it("parses keys list as two-word command", () => {
		const result = parseArgs(["node", "lakesync", "keys", "list", "--org", "org-1"]);
		expect(result.command).toEqual(["keys", "list"]);
		expect(result.flags.org).toBe("org-1");
	});

	it("parses keys create with all flags", () => {
		const result = parseArgs([
			"node",
			"lakesync",
			"keys",
			"create",
			"--name",
			"ci-key",
			"--role",
			"admin",
			"--gateway",
			"gw-1",
		]);
		expect(result.command).toEqual(["keys", "create"]);
		expect(result.flags.name).toBe("ci-key");
		expect(result.flags.role).toBe("admin");
		expect(result.flags.gateway).toBe("gw-1");
	});

	it("parses keys revoke with --id flag", () => {
		const result = parseArgs(["node", "lakesync", "keys", "revoke", "--id", "key-abc"]);
		expect(result.command).toEqual(["keys", "revoke"]);
		expect(result.flags.id).toBe("key-abc");
	});

	it("parses login with url and token", () => {
		const result = parseArgs([
			"node",
			"lakesync",
			"login",
			"--url",
			"http://localhost:3000",
			"--token",
			"eyJ...",
		]);
		expect(result.command).toEqual(["login"]);
		expect(result.flags.url).toBe("http://localhost:3000");
		expect(result.flags.token).toBe("eyJ...");
	});

	it("parses logout as single command", () => {
		const result = parseArgs(["node", "lakesync", "logout"]);
		expect(result.command).toEqual(["logout"]);
		expect(result.flags).toEqual({});
	});
});
