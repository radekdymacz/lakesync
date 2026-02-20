/**
 * Shared formatting utilities for the dashboard.
 */

/** Format a byte count into a human-readable string (e.g. "1.2 MB"). */
export function formatBytes(bytes: number): string {
	if (bytes === 0) return "0 B";
	const units = ["B", "KB", "MB", "GB", "TB"];
	const i = Math.floor(Math.log(bytes) / Math.log(1024));
	const val = bytes / 1024 ** i;
	return `${val.toFixed(val < 10 ? 1 : 0)} ${units[i]}`;
}

/** Format a date string as a short "M/D" label (e.g. "2/17"). */
export function formatDate(dateStr: string): string {
	const d = new Date(dateStr);
	return `${d.getMonth() + 1}/${d.getDate()}`;
}

/** Format a unix timestamp (seconds) as a long date (e.g. "17 February 2026"). */
export function formatTimestamp(timestamp: number): string {
	return new Date(timestamp * 1000).toLocaleDateString("en-GB", {
		year: "numeric",
		month: "long",
		day: "numeric",
	});
}

/** Format a price in cents as a dollar string (e.g. "$49"). Returns "Custom" for -1. */
export function formatCurrency(cents: number): string {
	if (cents === -1) return "Custom";
	return `$${(cents / 100).toFixed(0)}`;
}
