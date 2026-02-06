import { type LakeSyncError, Ok, type Result } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import type { MaintenanceReport, MaintenanceRunner } from "../maintenance";
import {
	CompactionScheduler,
	DEFAULT_SCHEDULER_CONFIG,
	type MaintenanceTask,
	type MaintenanceTaskProvider,
} from "../scheduler";

/** Minimal no-op maintenance report for test stubs */
const EMPTY_REPORT: MaintenanceReport = {
	compaction: {
		baseFilesWritten: 0,
		deleteFilesWritten: 0,
		deltaFilesCompacted: 0,
		bytesRead: 0,
		bytesWritten: 0,
	},
	snapshotsExpired: 0,
	orphansRemoved: 0,
};

/** Default test task returned by the task provider */
const TEST_TASK: MaintenanceTask = {
	deltaFileKeys: ["delta/file-0.parquet", "delta/file-1.parquet"],
	outputPrefix: "output/",
	storagePrefix: "data/",
};

/**
 * Create a mock MaintenanceRunner that records calls and returns
 * configurable results.
 */
function createMockRunner(
	result: Result<MaintenanceReport, LakeSyncError> = Ok(EMPTY_REPORT),
): MaintenanceRunner & {
	calls: Array<{
		deltaFileKeys: string[];
		outputPrefix: string;
		storagePrefix: string;
	}>;
} {
	const calls: Array<{
		deltaFileKeys: string[];
		outputPrefix: string;
		storagePrefix: string;
	}> = [];

	return {
		calls,
		run: vi.fn(
			async (
				deltaFileKeys: string[],
				outputPrefix: string,
				storagePrefix: string,
			): Promise<Result<MaintenanceReport, LakeSyncError>> => {
				calls.push({ deltaFileKeys, outputPrefix, storagePrefix });
				return result;
			},
		),
	} as unknown as MaintenanceRunner & {
		calls: Array<{
			deltaFileKeys: string[];
			outputPrefix: string;
			storagePrefix: string;
		}>;
	};
}

/**
 * Helper to create a simple task provider that always returns the given task.
 */
function staticTaskProvider(task: MaintenanceTask | null = TEST_TASK): MaintenanceTaskProvider {
	return async () => task;
}

/** Helper to sleep for a given duration using real timers */
function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

describe("CompactionScheduler", () => {
	describe("configuration", () => {
		it("uses default config values when none provided", () => {
			const runner = createMockRunner();
			const scheduler = new CompactionScheduler(runner, staticTaskProvider());

			expect(DEFAULT_SCHEDULER_CONFIG.intervalMs).toBe(60_000);
			expect(DEFAULT_SCHEDULER_CONFIG.enabled).toBe(true);
			expect(scheduler.isRunning).toBe(false);
		});

		it("accepts partial config overrides", () => {
			const runner = createMockRunner();
			const scheduler = new CompactionScheduler(runner, staticTaskProvider(), {
				intervalMs: 5000,
			});

			// Should still be constructable with partial config
			expect(scheduler.isRunning).toBe(false);
		});

		it("returns error when starting a disabled scheduler", () => {
			const runner = createMockRunner();
			const scheduler = new CompactionScheduler(runner, staticTaskProvider(), {
				enabled: false,
			});

			const result = scheduler.start();

			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("SCHEDULER_DISABLED");
			}
			expect(scheduler.isRunning).toBe(false);
		});
	});

	describe("start/stop lifecycle", () => {
		it("starts and becomes running", () => {
			const runner = createMockRunner();
			const scheduler = new CompactionScheduler(runner, staticTaskProvider(), {
				intervalMs: 1000,
			});

			const result = scheduler.start();

			expect(result.ok).toBe(true);
			expect(scheduler.isRunning).toBe(true);

			// Clean up (fire-and-forget is fine here since no tick has fired)
			void scheduler.stop();
		});

		it("returns error when starting an already running scheduler", async () => {
			const runner = createMockRunner();
			const scheduler = new CompactionScheduler(runner, staticTaskProvider(), {
				intervalMs: 1000,
			});

			scheduler.start();
			const result = scheduler.start();

			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("SCHEDULER_ALREADY_RUNNING");
			}

			await scheduler.stop();
		});

		it("stops and becomes not running", async () => {
			const runner = createMockRunner();
			const scheduler = new CompactionScheduler(runner, staticTaskProvider(), {
				intervalMs: 1000,
			});

			scheduler.start();
			const result = await scheduler.stop();

			expect(result.ok).toBe(true);
			expect(scheduler.isRunning).toBe(false);
		});

		it("returns error when stopping a scheduler that is not running", async () => {
			const runner = createMockRunner();
			const scheduler = new CompactionScheduler(runner, staticTaskProvider(), {
				intervalMs: 1000,
			});

			const result = await scheduler.stop();

			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("SCHEDULER_NOT_RUNNING");
			}
		});

		it("runs maintenance on each interval tick", async () => {
			const runner = createMockRunner();
			const scheduler = new CompactionScheduler(runner, staticTaskProvider(), {
				intervalMs: 30,
			});

			scheduler.start();

			// Wait long enough for at least 3 ticks to fire
			await sleep(120);

			expect(runner.calls.length).toBeGreaterThanOrEqual(3);
			expect(runner.calls[0]!.deltaFileKeys).toEqual(TEST_TASK.deltaFileKeys);
			expect(runner.calls[0]!.outputPrefix).toBe(TEST_TASK.outputPrefix);
			expect(runner.calls[0]!.storagePrefix).toBe(TEST_TASK.storagePrefix);

			await scheduler.stop();
		});

		it("does not run after stop is called", async () => {
			const runner = createMockRunner();
			const scheduler = new CompactionScheduler(runner, staticTaskProvider(), {
				intervalMs: 30,
			});

			scheduler.start();
			await sleep(50);

			const callsBeforeStop = runner.calls.length;
			expect(callsBeforeStop).toBeGreaterThanOrEqual(1);

			await scheduler.stop();

			// Wait for a few more intervals
			await sleep(100);

			// No further calls after stop
			expect(runner.calls.length).toBe(callsBeforeStop);
		});

		it("stop waits for in-progress run to finish", async () => {
			let resolveRun!: (value: Result<MaintenanceReport, LakeSyncError>) => void;
			const slowRunner = {
				run: vi.fn(
					() =>
						new Promise<Result<MaintenanceReport, LakeSyncError>>((resolve) => {
							resolveRun = resolve;
						}),
				),
			} as unknown as MaintenanceRunner;

			const scheduler = new CompactionScheduler(slowRunner, staticTaskProvider(), {
				intervalMs: 30,
			});

			scheduler.start();

			// Wait for the first tick to fire
			await sleep(50);

			// Start stopping — this should wait for the in-flight run
			const stopPromise = scheduler.stop();

			// The scheduler should no longer be "running" in terms of timer
			expect(scheduler.isRunning).toBe(false);

			// Resolve the in-progress maintenance run
			resolveRun(Ok(EMPTY_REPORT));

			// stop() should now resolve
			const result = await stopPromise;
			expect(result.ok).toBe(true);
		});
	});

	describe("skip-if-busy behaviour", () => {
		it("skips tick when a previous run is still in progress", async () => {
			let callCount = 0;
			let resolveFirst!: (value: Result<MaintenanceReport, LakeSyncError>) => void;

			const slowRunner = {
				run: vi.fn(() => {
					callCount++;
					if (callCount === 1) {
						// First call: block until manually resolved
						return new Promise<Result<MaintenanceReport, LakeSyncError>>((resolve) => {
							resolveFirst = resolve;
						});
					}
					// Subsequent calls resolve immediately
					return Promise.resolve(Ok(EMPTY_REPORT));
				}),
			} as unknown as MaintenanceRunner;

			const scheduler = new CompactionScheduler(slowRunner, staticTaskProvider(), {
				intervalMs: 30,
			});

			scheduler.start();

			// Wait for first tick to start, plus a couple more tick intervals
			await sleep(120);

			// Even though multiple intervals elapsed, only 1 call was made
			// because the first is still in progress
			expect(callCount).toBe(1);

			// Resolve the first run
			resolveFirst(Ok(EMPTY_REPORT));

			// Wait for another tick to fire and complete
			await sleep(60);

			// Now a second call should have been made
			expect(callCount).toBeGreaterThanOrEqual(2);

			await scheduler.stop();
		});
	});

	describe("runOnce", () => {
		it("executes a single maintenance run", async () => {
			const runner = createMockRunner();
			const scheduler = new CompactionScheduler(runner, staticTaskProvider(), {
				intervalMs: 60_000,
			});

			const result = await scheduler.runOnce();

			expect(result.ok).toBe(true);
			expect(runner.calls.length).toBe(1);
			expect(runner.calls[0]!.deltaFileKeys).toEqual(TEST_TASK.deltaFileKeys);
		});

		it("returns the maintenance report on success", async () => {
			const report: MaintenanceReport = {
				compaction: {
					baseFilesWritten: 1,
					deleteFilesWritten: 0,
					deltaFilesCompacted: 5,
					bytesRead: 1024,
					bytesWritten: 512,
				},
				snapshotsExpired: 0,
				orphansRemoved: 2,
			};

			const runner = createMockRunner(Ok(report));
			const scheduler = new CompactionScheduler(runner, staticTaskProvider());

			const result = await scheduler.runOnce();

			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.compaction.deltaFilesCompacted).toBe(5);
				expect(result.value.orphansRemoved).toBe(2);
			}
		});

		it("returns error when a run is already in progress", async () => {
			let resolveRun!: (value: Result<MaintenanceReport, LakeSyncError>) => void;
			const slowRunner = {
				run: vi.fn(
					() =>
						new Promise<Result<MaintenanceReport, LakeSyncError>>((resolve) => {
							resolveRun = resolve;
						}),
				),
			} as unknown as MaintenanceRunner;

			const scheduler = new CompactionScheduler(slowRunner, staticTaskProvider(), {
				intervalMs: 60_000,
			});

			// Start a run (do not await)
			const firstRun = scheduler.runOnce();

			// Allow the microtask for taskProvider to settle so executeMaintenance
			// reaches the runner.run() call and sets inFlightPromise
			await sleep(10);

			// Try to run again while first is in progress
			const secondRun = await scheduler.runOnce();

			expect(secondRun.ok).toBe(false);
			if (!secondRun.ok) {
				expect(secondRun.error.code).toBe("SCHEDULER_BUSY");
			}

			// Clean up
			resolveRun(Ok(EMPTY_REPORT));
			await firstRun;
		});

		it("returns empty report when task provider returns null", async () => {
			const runner = createMockRunner();
			const scheduler = new CompactionScheduler(runner, staticTaskProvider(null));

			const result = await scheduler.runOnce();

			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.compaction.deltaFilesCompacted).toBe(0);
				expect(result.value.orphansRemoved).toBe(0);
			}
			// Runner should not have been called
			expect(runner.calls.length).toBe(0);
		});

		it("returns error when task provider throws", async () => {
			const runner = createMockRunner();
			const failingProvider: MaintenanceTaskProvider = async () => {
				throw new Error("Provider exploded");
			};
			const scheduler = new CompactionScheduler(runner, failingProvider);

			const result = await scheduler.runOnce();

			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("SCHEDULER_TASK_PROVIDER_ERROR");
				expect(result.error.message).toContain("Provider exploded");
			}
		});

		it("works without starting the scheduler", async () => {
			const runner = createMockRunner();
			const scheduler = new CompactionScheduler(runner, staticTaskProvider());

			expect(scheduler.isRunning).toBe(false);

			const result = await scheduler.runOnce();
			expect(result.ok).toBe(true);
			expect(runner.calls.length).toBe(1);
		});
	});
});
