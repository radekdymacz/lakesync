/** Structured log levels. */
export type LogLevel = "info" | "warn" | "error";

/** Structured log entry for Cloudflare Workers. */
interface LogEntry {
	level: LogLevel;
	message: string;
	[key: string]: unknown;
}

/** Emit a structured JSON log line. */
function log(entry: LogEntry): void {
	const output = JSON.stringify(entry);
	switch (entry.level) {
		case "error":
			console.error(output);
			break;
		case "warn":
			console.warn(output);
			break;
		default:
			console.log(output);
	}
}

export const logger = {
	info(message: string, data?: Record<string, unknown>): void {
		log({ level: "info", message, timestamp: Date.now(), ...data });
	},
	warn(message: string, data?: Record<string, unknown>): void {
		log({ level: "warn", message, timestamp: Date.now(), ...data });
	},
	error(message: string, data?: Record<string, unknown>): void {
		log({ level: "error", message, timestamp: Date.now(), ...data });
	},
};
