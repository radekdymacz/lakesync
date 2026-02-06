import { Err, LakeSyncError, Ok, type Result } from "@lakesync/core";
import type { MaintenanceReport, MaintenanceRunner } from "./maintenance";

/** Parameters for a single maintenance run */
export interface MaintenanceTask {
	/** Storage keys of the delta Parquet files to compact */
	deltaFileKeys: string[];
	/** Prefix for the output base/delete file keys */
	outputPrefix: string;
	/** Prefix under which all related storage files live */
	storagePrefix: string;
}

/**
 * Provider function that resolves the maintenance task parameters for each run.
 * Called before every scheduled tick to determine what files to compact.
 * Return `null` to skip this tick (e.g. when there is nothing to compact).
 */
export type MaintenanceTaskProvider = () => Promise<MaintenanceTask | null>;

/** Configuration for the compaction scheduler */
export interface SchedulerConfig {
	/** Interval between maintenance runs in milliseconds (default 60000) */
	intervalMs: number;
	/** Whether the scheduler is enabled (default true) */
	enabled: boolean;
}

/** Default scheduler configuration values */
export const DEFAULT_SCHEDULER_CONFIG: SchedulerConfig = {
	intervalMs: 60_000,
	enabled: true,
};

/**
 * Manages interval-based compaction scheduling.
 *
 * Wraps a {@link MaintenanceRunner} and executes maintenance cycles on a
 * configurable interval. The scheduler is safe against concurrent runs:
 * if a previous tick is still in progress when the next fires, the tick
 * is silently skipped.
 */
export class CompactionScheduler {
	private readonly runner: MaintenanceRunner;
	private readonly taskProvider: MaintenanceTaskProvider;
	private readonly config: SchedulerConfig;

	private timer: ReturnType<typeof setInterval> | null = null;
	private running = false;
	private inFlightPromise: Promise<Result<MaintenanceReport, LakeSyncError>> | null = null;

	/**
	 * Create a new CompactionScheduler instance.
	 *
	 * @param runner - The maintenance runner to execute on each tick
	 * @param taskProvider - Function that provides maintenance task parameters for each run
	 * @param config - Scheduler configuration (interval and enabled flag)
	 */
	constructor(
		runner: MaintenanceRunner,
		taskProvider: MaintenanceTaskProvider,
		config: Partial<SchedulerConfig> = {},
	) {
		this.runner = runner;
		this.taskProvider = taskProvider;
		this.config = { ...DEFAULT_SCHEDULER_CONFIG, ...config };
	}

	/**
	 * Whether the scheduler is currently active (timer is ticking).
	 */
	get isRunning(): boolean {
		return this.running;
	}

	/**
	 * Start the scheduler interval timer.
	 *
	 * Begins executing maintenance runs at the configured interval.
	 * If the scheduler is already running or disabled, returns an error.
	 *
	 * @returns A Result indicating success or a descriptive error
	 */
	start(): Result<void, LakeSyncError> {
		if (!this.config.enabled) {
			return Err(new LakeSyncError("Scheduler is disabled", "SCHEDULER_DISABLED"));
		}

		if (this.running) {
			return Err(new LakeSyncError("Scheduler is already running", "SCHEDULER_ALREADY_RUNNING"));
		}

		this.running = true;
		this.timer = setInterval(() => {
			void this.tick();
		}, this.config.intervalMs);

		return Ok(undefined);
	}

	/**
	 * Stop the scheduler and wait for any in-progress run to finish.
	 *
	 * Clears the interval timer and, if a maintenance run is currently
	 * executing, awaits its completion before returning.
	 *
	 * @returns A Result indicating success or a descriptive error
	 */
	async stop(): Promise<Result<void, LakeSyncError>> {
		if (!this.running) {
			return Err(new LakeSyncError("Scheduler is not running", "SCHEDULER_NOT_RUNNING"));
		}

		if (this.timer !== null) {
			clearInterval(this.timer);
			this.timer = null;
		}

		this.running = false;

		// Wait for any in-progress run to complete
		if (this.inFlightPromise !== null) {
			await this.inFlightPromise;
			this.inFlightPromise = null;
		}

		return Ok(undefined);
	}

	/**
	 * Manually trigger a single maintenance run.
	 *
	 * Useful for testing or administrative purposes. If a run is already
	 * in progress, skips and returns an error.
	 *
	 * @returns A Result containing the MaintenanceReport, or a LakeSyncError on failure
	 */
	async runOnce(): Promise<Result<MaintenanceReport, LakeSyncError>> {
		if (this.inFlightPromise !== null) {
			return Err(new LakeSyncError("A maintenance run is already in progress", "SCHEDULER_BUSY"));
		}

		return this.executeMaintenance();
	}

	/**
	 * Internal tick handler called by the interval timer.
	 * Skips if a previous run is still in progress.
	 */
	private async tick(): Promise<void> {
		if (this.inFlightPromise !== null) {
			return;
		}

		await this.executeMaintenance();
	}

	/**
	 * Execute a single maintenance cycle.
	 *
	 * Calls the task provider to get parameters, then runs the maintenance
	 * runner. Tracks the in-flight promise so concurrent runs are prevented.
	 */
	private async executeMaintenance(): Promise<Result<MaintenanceReport, LakeSyncError>> {
		const taskResult = await this.resolveTask();
		if (!taskResult.ok) {
			return taskResult;
		}

		const task = taskResult.value;
		if (task === null) {
			return Ok({
				compaction: {
					baseFilesWritten: 0,
					deleteFilesWritten: 0,
					deltaFilesCompacted: 0,
					bytesRead: 0,
					bytesWritten: 0,
				},
				snapshotsExpired: 0,
				orphansRemoved: 0,
			});
		}

		const promise = this.runner.run(task.deltaFileKeys, task.outputPrefix, task.storagePrefix);
		this.inFlightPromise = promise;

		try {
			const result = await promise;
			return result;
		} finally {
			this.inFlightPromise = null;
		}
	}

	/**
	 * Resolve the maintenance task from the provider, wrapping any thrown
	 * exceptions into a Result error.
	 */
	private async resolveTask(): Promise<Result<MaintenanceTask | null, LakeSyncError>> {
		try {
			const task = await this.taskProvider();
			return Ok(task);
		} catch (error) {
			return Err(
				new LakeSyncError(
					`Task provider failed: ${error instanceof Error ? error.message : String(error)}`,
					"SCHEDULER_TASK_PROVIDER_ERROR",
				),
			);
		}
	}
}
