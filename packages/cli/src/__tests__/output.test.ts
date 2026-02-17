import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { print, printTable } from "../output";

describe("output", () => {
	let mockStdout: string[];

	beforeEach(() => {
		mockStdout = [];
		vi.spyOn(process.stdout, "write").mockImplementation((data) => {
			mockStdout.push(String(data));
			return true;
		});
	});

	afterEach(() => {
		vi.restoreAllMocks();
	});

	it("prints a message to stdout", () => {
		print("hello world");
		expect(mockStdout).toEqual(["hello world\n"]);
	});

	it("prints a table with headers and rows", () => {
		printTable([
			{ name: "gw-1", status: "active", region: "us" },
			{ name: "gw-2", status: "suspended", region: "eu" },
		]);

		const output = mockStdout.join("");
		expect(output).toContain("name");
		expect(output).toContain("status");
		expect(output).toContain("gw-1");
		expect(output).toContain("active");
		expect(output).toContain("gw-2");
		expect(output).toContain("suspended");
	});

	it("prints (none) for empty table", () => {
		printTable([]);
		expect(mockStdout.join("")).toContain("(none)");
	});
});
