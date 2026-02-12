import { describe, expect, it } from "vitest";
import { type LogEntry, Logger } from "../logger";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Collect log output into an array of parsed entries. */
function createTestLogger(level: "debug" | "info" | "warn" | "error" = "info") {
	const lines: LogEntry[] = [];
	const writeFn = (line: string) => {
		lines.push(JSON.parse(line) as LogEntry);
	};
	const logger = new Logger(level, {}, writeFn);
	return { logger, lines };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Logger", () => {
	it("outputs valid JSON lines", () => {
		const { logger, lines } = createTestLogger();
		logger.info("hello world");

		expect(lines).toHaveLength(1);
		expect(lines[0]!.level).toBe("info");
		expect(lines[0]!.msg).toBe("hello world");
		expect(lines[0]!.ts).toBeDefined();
		// Verify ts is a valid ISO date
		expect(new Date(lines[0]!.ts).toISOString()).toBe(lines[0]!.ts);
	});

	it("includes extra data in the log entry", () => {
		const { logger, lines } = createTestLogger();
		logger.info("push received", { deltas: 5, table: "tasks" });

		expect(lines[0]!.deltas).toBe(5);
		expect(lines[0]!.table).toBe("tasks");
	});

	it("filters messages below the minimum level", () => {
		const { logger, lines } = createTestLogger("warn");
		logger.debug("should not appear");
		logger.info("should not appear");
		logger.warn("should appear");
		logger.error("should appear");

		expect(lines).toHaveLength(2);
		expect(lines[0]!.level).toBe("warn");
		expect(lines[1]!.level).toBe("error");
	});

	it("debug level allows all messages", () => {
		const { logger, lines } = createTestLogger("debug");
		logger.debug("d");
		logger.info("i");
		logger.warn("w");
		logger.error("e");

		expect(lines).toHaveLength(4);
	});

	it("error level only allows error messages", () => {
		const { logger, lines } = createTestLogger("error");
		logger.debug("no");
		logger.info("no");
		logger.warn("no");
		logger.error("yes");

		expect(lines).toHaveLength(1);
		expect(lines[0]!.level).toBe("error");
	});

	describe("child()", () => {
		it("includes parent bindings in child output", () => {
			const { logger, lines } = createTestLogger();
			const child = logger.child({ requestId: "req-123" });
			child.info("handling request");

			expect(lines[0]!.requestId).toBe("req-123");
			expect(lines[0]!.msg).toBe("handling request");
		});

		it("merges child bindings with extra data", () => {
			const { logger, lines } = createTestLogger();
			const child = logger.child({ requestId: "req-456", method: "POST" });
			child.info("push", { deltas: 3 });

			expect(lines[0]!.requestId).toBe("req-456");
			expect(lines[0]!.method).toBe("POST");
			expect(lines[0]!.deltas).toBe(3);
		});

		it("child inherits level filtering from parent", () => {
			const { logger, lines } = createTestLogger("warn");
			const child = logger.child({ requestId: "req-789" });
			child.info("should not appear");
			child.warn("should appear");

			expect(lines).toHaveLength(1);
			expect(lines[0]!.level).toBe("warn");
		});

		it("supports nested children", () => {
			const { logger, lines } = createTestLogger();
			const child = logger.child({ gatewayId: "gw-1" });
			const grandchild = child.child({ requestId: "req-abc" });
			grandchild.info("nested");

			expect(lines[0]!.gatewayId).toBe("gw-1");
			expect(lines[0]!.requestId).toBe("req-abc");
		});
	});
});
