/** Log severity levels. */
export type LogLevel = "debug" | "info" | "warn" | "error";

/**
 * Injectable logging callback.
 *
 * Library code calls this instead of writing to `console` directly,
 * allowing consumers to route log output however they wish.
 */
export type Logger = (level: LogLevel, message: string, meta?: Record<string, unknown>) => void;

/** Default logger that writes to `console`. */
export const defaultLogger: Logger = (level, message) => console[level](`[lakesync] ${message}`);
