// ---------------------------------------------------------------------------
// Structured Logger — minimal JSON-lines logger for gateway-server
// ---------------------------------------------------------------------------

/** Supported log levels, ordered by severity. */
export type LogLevel = "debug" | "info" | "warn" | "error";

/** A single structured log entry. */
export interface LogEntry {
	level: LogLevel;
	msg: string;
	ts: string;
	[key: string]: unknown;
}

/** Numeric severity values for level comparison. */
const LEVEL_VALUE: Record<LogLevel, number> = {
	debug: 0,
	info: 1,
	warn: 2,
	error: 3,
};

/**
 * Minimal structured logger that outputs JSON lines to stdout.
 *
 * Supports log-level filtering and child loggers with bound context.
 * No external dependencies — compatible with any log aggregator that
 * consumes JSON lines.
 *
 * @example
 * ```ts
 * const logger = new Logger("info");
 * const reqLogger = logger.child({ requestId: "abc-123" });
 * reqLogger.info("push received", { deltas: 5 });
 * // => {"level":"info","msg":"push received","ts":"...","requestId":"abc-123","deltas":5}
 * ```
 */
export class Logger {
	private readonly minLevelValue: number;
	private readonly bindings: Record<string, unknown>;

	/** Output function — defaults to stdout, overridable for testing. */
	private readonly writeFn: (line: string) => void;

	constructor(
		minLevel: LogLevel = "info",
		bindings: Record<string, unknown> = {},
		writeFn?: (line: string) => void,
	) {
		this.minLevelValue = LEVEL_VALUE[minLevel];
		this.bindings = bindings;
		this.writeFn = writeFn ?? ((line) => process.stdout.write(`${line}\n`));
	}

	/** Log at debug level. */
	debug(msg: string, data?: Record<string, unknown>): void {
		this.log("debug", msg, data);
	}

	/** Log at info level. */
	info(msg: string, data?: Record<string, unknown>): void {
		this.log("info", msg, data);
	}

	/** Log at warn level. */
	warn(msg: string, data?: Record<string, unknown>): void {
		this.log("warn", msg, data);
	}

	/** Log at error level. */
	error(msg: string, data?: Record<string, unknown>): void {
		this.log("error", msg, data);
	}

	/**
	 * Create a child logger with additional bound context.
	 *
	 * The child inherits the parent's level and write function, plus
	 * merges any parent bindings with the new ones.
	 */
	child(bindings: Record<string, unknown>): Logger {
		return new Logger(this.minLevelName(), { ...this.bindings, ...bindings }, this.writeFn);
	}

	// -----------------------------------------------------------------------
	// Internal
	// -----------------------------------------------------------------------

	private log(level: LogLevel, msg: string, data?: Record<string, unknown>): void {
		if (LEVEL_VALUE[level] < this.minLevelValue) return;

		const entry: LogEntry = {
			level,
			msg,
			ts: new Date().toISOString(),
			...this.bindings,
			...data,
		};

		this.writeFn(JSON.stringify(entry));
	}

	private minLevelName(): LogLevel {
		for (const [name, val] of Object.entries(LEVEL_VALUE)) {
			if (val === this.minLevelValue) return name as LogLevel;
		}
		return "info";
	}
}
