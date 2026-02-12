// ---------------------------------------------------------------------------
// Prometheus-Compatible Metrics — counters, gauges, histograms
// ---------------------------------------------------------------------------

/** Label set for a metric observation. */
export type Labels = Record<string, string>;

// ---------------------------------------------------------------------------
// Counter
// ---------------------------------------------------------------------------

/**
 * Monotonically increasing counter.
 *
 * @example
 * ```ts
 * const pushTotal = new Counter("lakesync_push_total", "Total push requests");
 * pushTotal.inc({ status: "ok" });
 * ```
 */
export class Counter {
	private readonly values = new Map<string, number>();

	constructor(
		readonly name: string,
		readonly help: string,
	) {}

	/** Increment the counter by `n` (default 1). */
	inc(labels: Labels = {}, n = 1): void {
		const key = labelKey(labels);
		this.values.set(key, (this.values.get(key) ?? 0) + n);
	}

	/** Return the current value for the given labels. */
	get(labels: Labels = {}): number {
		return this.values.get(labelKey(labels)) ?? 0;
	}

	/** Reset all values. */
	reset(): void {
		this.values.clear();
	}

	/** Serialise to Prometheus text exposition format. */
	expose(): string {
		const lines: string[] = [];
		lines.push(`# HELP ${this.name} ${this.help}`);
		lines.push(`# TYPE ${this.name} counter`);
		for (const [key, val] of this.values) {
			lines.push(`${this.name}${key} ${val}`);
		}
		return lines.join("\n");
	}
}

// ---------------------------------------------------------------------------
// Gauge
// ---------------------------------------------------------------------------

/**
 * Gauge that can go up and down.
 *
 * @example
 * ```ts
 * const bufferBytes = new Gauge("lakesync_buffer_bytes", "Buffer size in bytes");
 * bufferBytes.set({}, 1024);
 * ```
 */
export class Gauge {
	private readonly values = new Map<string, number>();

	constructor(
		readonly name: string,
		readonly help: string,
	) {}

	/** Set to an absolute value. */
	set(labels: Labels = {}, value: number = 0): void {
		this.values.set(labelKey(labels), value);
	}

	/** Increment by `n` (default 1). */
	inc(labels: Labels = {}, n = 1): void {
		const key = labelKey(labels);
		this.values.set(key, (this.values.get(key) ?? 0) + n);
	}

	/** Decrement by `n` (default 1). */
	dec(labels: Labels = {}, n = 1): void {
		const key = labelKey(labels);
		this.values.set(key, (this.values.get(key) ?? 0) - n);
	}

	/** Return the current value for the given labels. */
	get(labels: Labels = {}): number {
		return this.values.get(labelKey(labels)) ?? 0;
	}

	/** Reset all values. */
	reset(): void {
		this.values.clear();
	}

	/** Serialise to Prometheus text exposition format. */
	expose(): string {
		const lines: string[] = [];
		lines.push(`# HELP ${this.name} ${this.help}`);
		lines.push(`# TYPE ${this.name} gauge`);
		for (const [key, val] of this.values) {
			lines.push(`${this.name}${key} ${val}`);
		}
		return lines.join("\n");
	}
}

// ---------------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------------

/** Internal per-label-set histogram state. */
interface HistogramBucket {
	bucketCounts: number[];
	sum: number;
	count: number;
}

/**
 * Histogram with configurable buckets.
 *
 * @example
 * ```ts
 * const latency = new Histogram(
 *   "lakesync_push_latency_ms",
 *   "Push latency in ms",
 *   [1, 5, 10, 50, 100, 500],
 * );
 * latency.observe({}, 42);
 * ```
 */
export class Histogram {
	private readonly data = new Map<string, HistogramBucket>();

	constructor(
		readonly name: string,
		readonly help: string,
		readonly buckets: number[],
	) {
		// Ensure buckets are sorted ascending
		this.buckets = [...buckets].sort((a, b) => a - b);
	}

	/** Record an observation. */
	observe(labels: Labels = {}, value: number = 0): void {
		const key = labelKey(labels);
		let bucket = this.data.get(key);
		if (!bucket) {
			bucket = {
				bucketCounts: new Array(this.buckets.length + 1).fill(0) as number[],
				sum: 0,
				count: 0,
			};
			this.data.set(key, bucket);
		}
		bucket.sum += value;
		bucket.count += 1;
		for (let i = 0; i < this.buckets.length; i++) {
			if (value <= this.buckets[i]!) {
				bucket.bucketCounts[i]!++;
			}
		}
		// +Inf bucket (last element)
		bucket.bucketCounts[this.buckets.length]!++;
	}

	/** Return the count of observations for the given labels. */
	getCount(labels: Labels = {}): number {
		return this.data.get(labelKey(labels))?.count ?? 0;
	}

	/** Return the sum of observations for the given labels. */
	getSum(labels: Labels = {}): number {
		return this.data.get(labelKey(labels))?.sum ?? 0;
	}

	/** Reset all values. */
	reset(): void {
		this.data.clear();
	}

	/** Serialise to Prometheus text exposition format. */
	expose(): string {
		const lines: string[] = [];
		lines.push(`# HELP ${this.name} ${this.help}`);
		lines.push(`# TYPE ${this.name} histogram`);

		for (const [key, bucket] of this.data) {
			const labelStr = key === "" ? "" : key;
			const separator = labelStr === "" ? "{" : `${labelStr.slice(0, -1)},`;
			const closeBrace = "}";

			for (let i = 0; i < this.buckets.length; i++) {
				const le = this.buckets[i]!;
				lines.push(
					`${this.name}_bucket${separator}le="${le}"${closeBrace} ${bucket.bucketCounts[i]}`,
				);
			}
			lines.push(
				`${this.name}_bucket${separator}le="+Inf"${closeBrace} ${bucket.bucketCounts[this.buckets.length]}`,
			);
			lines.push(`${this.name}_sum${labelStr} ${bucket.sum}`);
			lines.push(`${this.name}_count${labelStr} ${bucket.count}`);
		}

		return lines.join("\n");
	}
}

// ---------------------------------------------------------------------------
// Metrics Registry
// ---------------------------------------------------------------------------

/**
 * Pre-configured metrics registry for the gateway server.
 *
 * Exposes all standard lakesync metrics and a single `expose()` method
 * that returns the complete Prometheus text exposition payload.
 */
export class MetricsRegistry {
	readonly pushTotal = new Counter("lakesync_push_total", "Total push requests");
	readonly pullTotal = new Counter("lakesync_pull_total", "Total pull requests");
	readonly flushTotal = new Counter("lakesync_flush_total", "Total flush operations");

	readonly flushDuration = new Histogram(
		"lakesync_flush_duration_ms",
		"Flush duration in milliseconds",
		[10, 50, 100, 500, 1000, 5000],
	);

	readonly pushLatency = new Histogram(
		"lakesync_push_latency_ms",
		"Push request latency in milliseconds",
		[1, 5, 10, 50, 100, 500],
	);

	readonly bufferBytes = new Gauge("lakesync_buffer_bytes", "Current buffer size in bytes");
	readonly bufferDeltas = new Gauge("lakesync_buffer_deltas", "Current number of buffered deltas");
	readonly wsConnections = new Gauge("lakesync_ws_connections", "Active WebSocket connections");
	readonly activeRequests = new Gauge("lakesync_active_requests", "In-flight HTTP requests");

	/** Return the full Prometheus text exposition payload. */
	expose(): string {
		const sections = [
			this.pushTotal.expose(),
			this.pullTotal.expose(),
			this.flushTotal.expose(),
			this.flushDuration.expose(),
			this.pushLatency.expose(),
			this.bufferBytes.expose(),
			this.bufferDeltas.expose(),
			this.wsConnections.expose(),
			this.activeRequests.expose(),
		];
		return `${sections.join("\n\n")}\n`;
	}

	/** Reset all metrics. */
	reset(): void {
		this.pushTotal.reset();
		this.pullTotal.reset();
		this.flushTotal.reset();
		this.flushDuration.reset();
		this.pushLatency.reset();
		this.bufferBytes.reset();
		this.bufferDeltas.reset();
		this.wsConnections.reset();
		this.activeRequests.reset();
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a Prometheus-format label key string like `{status="ok"}`. */
function labelKey(labels: Labels): string {
	const entries = Object.entries(labels);
	if (entries.length === 0) return "";
	const parts = entries.map(([k, v]) => `${k}="${v}"`).join(",");
	return `{${parts}}`;
}
