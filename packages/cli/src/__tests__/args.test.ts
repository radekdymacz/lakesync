import { describe, expect, it } from "vitest";
import { parseArgs } from "../args";

describe("parseArgs", () => {
	it("parses a simple command", () => {
		const result = parseArgs(["node", "lakesync", "init"]);
		expect(result.command).toEqual(["init"]);
		expect(result.flags).toEqual({});
		expect(result.positional).toEqual([]);
	});

	it("parses a two-word command", () => {
		const result = parseArgs(["node", "lakesync", "token", "create"]);
		expect(result.command).toEqual(["token", "create"]);
	});

	it("parses --flag value pairs", () => {
		const result = parseArgs([
			"node", "lakesync", "token", "create",
			"--secret", "my-secret",
			"--gateway", "gw-1",
		]);
		expect(result.command).toEqual(["token", "create"]);
		expect(result.flags).toEqual({ secret: "my-secret", gateway: "gw-1" });
	});

	it("parses --flag=value syntax", () => {
		const result = parseArgs([
			"node", "lakesync", "token", "create",
			"--secret=my-secret",
			"--role=admin",
		]);
		expect(result.flags).toEqual({ secret: "my-secret", role: "admin" });
	});

	it("parses boolean flags (no value)", () => {
		const result = parseArgs(["node", "lakesync", "init", "--force"]);
		expect(result.command).toEqual(["init"]);
		expect(result.flags).toEqual({ force: "true" });
	});

	it("parses positional arguments after single-word command", () => {
		const result = parseArgs([
			"node", "lakesync", "push",
			"data.json",
			"--url", "http://localhost:3000",
		]);
		expect(result.command).toEqual(["push"]);
		expect(result.positional).toEqual(["data.json"]);
		expect(result.flags).toEqual({ url: "http://localhost:3000" });
	});

	it("handles empty arguments", () => {
		const result = parseArgs(["node", "lakesync"]);
		expect(result.command).toEqual([]);
		expect(result.flags).toEqual({});
		expect(result.positional).toEqual([]);
	});

	it("parses -h short flags", () => {
		const result = parseArgs(["node", "lakesync", "-h"]);
		expect(result.flags).toEqual({ h: "true" });
	});

	it("parses mixed command, flags, and positional args", () => {
		const result = parseArgs([
			"node", "lakesync", "push",
			"--url", "http://localhost:3000",
			"deltas.json",
			"--gateway", "gw-1",
		]);
		expect(result.command).toEqual(["push"]);
		expect(result.flags).toEqual({
			url: "http://localhost:3000",
			gateway: "gw-1",
		});
		expect(result.positional).toEqual(["deltas.json"]);
	});
});
