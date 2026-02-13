import { Err, Ok, type Result } from "../result/result";
import { validateFlowConfig } from "./dsl";
import type { FlowConfig, FlowEngine, FlowEngineDeps, FlowState, FlowStatus } from "./types";
import { FlowError } from "./types";

// ---------------------------------------------------------------------------
// Internal flow entry — tracks per-flow runtime state
// ---------------------------------------------------------------------------

interface FlowEntry {
	config: FlowConfig;
	state: FlowState;
	deltasProcessed: number;
	lastError?: string;
	lastActivityAt?: Date;
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
		const from = entry.state;
		entry.state = to;
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
				state: "idle",
				deltasProcessed: 0,
			});

			return Ok(undefined);
		},

		async startFlow(name: string): Promise<Result<void, FlowError>> {
			const entry = flows.get(name);
			if (!entry) {
				return Err(new FlowError(`Flow "${name}" not found`, "FLOW_NOT_FOUND"));
			}

			if (entry.state === "running") {
				return Ok(undefined);
			}

			try {
				transitionState(entry, "running");
				entry.lastActivityAt = new Date();
				return Ok(undefined);
			} catch (err) {
				const message = err instanceof Error ? err.message : String(err);
				entry.lastError = message;
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

			if (entry.state === "stopped" || entry.state === "idle") {
				return Ok(undefined);
			}

			transitionState(entry, "stopped");
			return Ok(undefined);
		},

		async startAll(): Promise<Result<void, FlowError>> {
			const errors: string[] = [];

			for (const [name, entry] of flows) {
				if (entry.state === "running") continue;
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
				const status: FlowStatus = {
					name: entry.config.name,
					state: entry.state,
					deltasProcessed: entry.deltasProcessed,
				};
				if (entry.lastError !== undefined) {
					status.lastError = entry.lastError;
				}
				if (entry.lastActivityAt !== undefined) {
					status.lastActivityAt = entry.lastActivityAt;
				}
				statuses.push(status);
			}
			return statuses;
		},
	};

	return engine;
}
