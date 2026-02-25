import { Err, Ok, type Result } from "../result/result";
import { validateFlowConfig } from "./dsl";
import type {
	FlowConfig,
	FlowEngine,
	FlowEngineDeps,
	FlowSnapshot,
	FlowState,
	FlowStatus,
} from "./types";
import { FlowError } from "./types";

// ---------------------------------------------------------------------------
// Internal flow entry — tracks per-flow runtime state via atomic snapshot
// ---------------------------------------------------------------------------

interface FlowEntry {
	readonly config: FlowConfig;
	/** Atomic swap target — all runtime mutations replace this object. */
	snapshot: FlowSnapshot;
}

// ---------------------------------------------------------------------------
// createFlowEngine — factory function
// ---------------------------------------------------------------------------

/**
 * Create a {@link FlowEngine} that manages the lifecycle of declarative flows.
 *
 * Each flow is isolated — starting or stopping one does not affect others.
 * The engine currently manages state transitions only; actual adapter wiring
 * is deferred to a later phase.
 */
export function createFlowEngine(deps: FlowEngineDeps = {}): FlowEngine {
	const flows = new Map<string, FlowEntry>();

	function transitionState(entry: FlowEntry, to: FlowState): void {
		const from = entry.snapshot.state;
		entry.snapshot = { ...entry.snapshot, state: to };
		deps.onFlowStateChange?.(entry.config.name, from, to);
	}

	const engine: FlowEngine = {
		addFlow(config: FlowConfig): Result<void, FlowError> {
			// Validate config before registering
			const validation = validateFlowConfig(config);
			if (!validation.ok) return validation;

			if (flows.has(config.name)) {
				return Err(new FlowError(`Flow "${config.name}" already exists`, "FLOW_ALREADY_EXISTS"));
			}

			flows.set(config.name, {
				config,
				snapshot: { state: "idle", deltasProcessed: 0 },
			});

			return Ok(undefined);
		},

		async startFlow(name: string): Promise<Result<void, FlowError>> {
			const entry = flows.get(name);
			if (!entry) {
				return Err(new FlowError(`Flow "${name}" not found`, "FLOW_NOT_FOUND"));
			}

			if (entry.snapshot.state === "running") {
				return Ok(undefined);
			}

			try {
				// If a runtime is provided, wire the flow's adapters/gateway/materialisation
				const handle = deps.runtime ? await deps.runtime.start(entry.config) : undefined;
				const from = entry.snapshot.state;
				entry.snapshot = {
					...entry.snapshot,
					state: "running",
					handle,
					lastActivityAt: new Date(),
				};
				deps.onFlowStateChange?.(entry.config.name, from, "running");
				return Ok(undefined);
			} catch (err) {
				const message = err instanceof Error ? err.message : String(err);
				entry.snapshot = { ...entry.snapshot, lastError: message };
				transitionState(entry, "error");
				return Err(
					new FlowError(`Failed to start flow "${name}": ${message}`, "FLOW_START_FAILED"),
				);
			}
		},

		async stopFlow(name: string): Promise<Result<void, FlowError>> {
			const entry = flows.get(name);
			if (!entry) {
				return Err(new FlowError(`Flow "${name}" not found`, "FLOW_NOT_FOUND"));
			}

			if (entry.snapshot.state === "stopped" || entry.snapshot.state === "idle") {
				return Ok(undefined);
			}

			// Stop the runtime handle if present
			if (entry.snapshot.handle) {
				await entry.snapshot.handle.stop();
			}

			const from = entry.snapshot.state;
			entry.snapshot = { ...entry.snapshot, state: "stopped", handle: undefined };
			deps.onFlowStateChange?.(entry.config.name, from, "stopped");
			return Ok(undefined);
		},

		async startAll(): Promise<Result<void, FlowError>> {
			const errors: string[] = [];

			for (const [name, entry] of flows) {
				if (entry.snapshot.state === "running") continue;
				const result = await engine.startFlow(name);
				if (!result.ok) {
					errors.push(result.error.message);
				}
			}

			if (errors.length > 0) {
				return Err(
					new FlowError(`Failed to start some flows: ${errors.join("; ")}`, "FLOW_START_FAILED"),
				);
			}

			return Ok(undefined);
		},

		async stopAll(): Promise<void> {
			for (const [name] of flows) {
				await engine.stopFlow(name);
			}
		},

		getStatus(): FlowStatus[] {
			const statuses: FlowStatus[] = [];
			for (const entry of flows.values()) {
				const { state, deltasProcessed, lastError, lastActivityAt } = entry.snapshot;
				const status: FlowStatus = {
					name: entry.config.name,
					state,
					deltasProcessed,
				};
				if (lastError !== undefined) {
					status.lastError = lastError;
				}
				if (lastActivityAt !== undefined) {
					status.lastActivityAt = lastActivityAt;
				}
				statuses.push(status);
			}
			return statuses;
		},
	};

	return engine;
}
