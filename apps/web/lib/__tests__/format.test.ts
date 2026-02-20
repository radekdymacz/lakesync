import { describe, expect, it } from "vitest";
import { formatBytes, formatCurrency, formatDate, formatTimestamp } from "@/lib/format";

describe("formatBytes", () => {
	it("returns '0 B' for zero", () => {
		expect(formatBytes(0)).toBe("0 B");
	});

	it("formats small byte values", () => {
		expect(formatBytes(100)).toBe("100 B");
		expect(formatBytes(1)).toBe("1.0 B");
	});

	it("formats kilobytes", () => {
		expect(formatBytes(1024)).toBe("1.0 KB");
		expect(formatBytes(1536)).toBe("1.5 KB");
	});

	it("formats megabytes", () => {
		expect(formatBytes(1024 * 1024)).toBe("1.0 MB");
		expect(formatBytes(12 * 1024 * 1024)).toBe("12 MB");
	});

	it("formats gigabytes", () => {
		expect(formatBytes(1024 ** 3)).toBe("1.0 GB");
	});

	it("formats terabytes", () => {
		expect(formatBytes(1024 ** 4)).toBe("1.0 TB");
	});
});

describe("formatDate", () => {
	it("formats an ISO date string as M/D", () => {
		const result = formatDate("2026-02-17T00:00:00Z");
		// Month and day depend on local timezone, but the format is M/D
		expect(result).toMatch(/^\d{1,2}\/\d{1,2}$/);
	});

	it("formats a date-only string", () => {
		// Date-only strings are parsed as UTC midnight
		const result = formatDate("2026-01-05");
		expect(result).toMatch(/^\d{1,2}\/\d{1,2}$/);
	});
});

describe("formatTimestamp", () => {
	it("formats a unix timestamp as a long date", () => {
		// 2026-02-17 00:00:00 UTC = 1771200000
		const result = formatTimestamp(1771200000);
		expect(result).toContain("2026");
		expect(result).toContain("February");
	});
});

describe("formatCurrency", () => {
	it("returns '$0' for zero cents", () => {
		expect(formatCurrency(0)).toBe("$0");
	});

	it("formats small cent values", () => {
		expect(formatCurrency(99)).toBe("$1");
		expect(formatCurrency(50)).toBe("$1");
	});

	it("formats dollar amounts", () => {
		expect(formatCurrency(4900)).toBe("$49");
		expect(formatCurrency(9900)).toBe("$99");
	});

	it("formats large amounts", () => {
		expect(formatCurrency(49900)).toBe("$499");
		expect(formatCurrency(100000)).toBe("$1000");
	});

	it("returns 'Custom' for -1", () => {
		expect(formatCurrency(-1)).toBe("Custom");
	});
});
