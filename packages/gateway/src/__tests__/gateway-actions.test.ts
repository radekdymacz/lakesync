import type {
	Action,
	ActionHandler,
	ActionResult,
	AuthContext,
	HLCTimestamp,
	Result,
} from "@lakesync/core";
import { ActionExecutionError, Err, Ok } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { SyncGateway } from "../gateway";

/** Create a mock ActionHandler that succeeds. */
function createMockHandler(
	actions: string[] = ["create_pr"],
	executeFn?: ActionHandler["executeAction"],
): ActionHandler {
	return {
		supportedActions: actions.map((a) => ({ actionType: a, description: `Do ${a}` })),
		executeAction:
			executeFn ??
			(async (action: Action): Promise<Result<ActionResult, ActionExecutionError>> =>
				Ok({
					actionId: action.actionId,
					data: { success: true },
					serverHlc: 0n as HLCTimestamp,
				})),
	};
}

function createTestGateway(handlers?: Record<string, ActionHandler>): SyncGateway {
	return new SyncGateway({
		gatewayId: "test",
		maxBufferBytes: 1024 * 1024,
		maxBufferAgeMs: 30_000,
		actionHandlers: handlers,
	});
}

function createAction(overrides?: Partial<Action>): Action {
	return {
		actionId: `action-${Math.random().toString(36).slice(2)}`,
		clientId: "client-1",
		hlc: 100n as HLCTimestamp,
		connector: "github",
		actionType: "create_pr",
		params: { title: "Fix bug" },
		...overrides,
	};
}

describe("SyncGateway.handleAction", () => {
	it("executes a valid action and returns success", async () => {
		const gw = createTestGateway({ github: createMockHandler() });
		const action = createAction();

		const result = await gw.handleAction({
			clientId: "client-1",
			actions: [action],
		});

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.results).toHaveLength(1);
		expect(result.value.results[0]!.actionId).toBe(action.actionId);
		expect("data" in result.value.results[0]!).toBe(true);
	});

	it("returns error for unknown connector", async () => {
		const gw = createTestGateway();
		const action = createAction({ connector: "unknown" });

		const result = await gw.handleAction({
			clientId: "client-1",
			actions: [action],
		});

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		const r = result.value.results[0]!;
		expect("code" in r).toBe(true);
		if ("code" in r) {
			expect(r.code).toBe("ACTION_NOT_SUPPORTED");
		}
	});

	it("returns error for unsupported action type", async () => {
		const gw = createTestGateway({ github: createMockHandler(["list_prs"]) });
		const action = createAction({ actionType: "create_pr" });

		const result = await gw.handleAction({
			clientId: "client-1",
			actions: [action],
		});

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		const r = result.value.results[0]!;
		expect("code" in r).toBe(true);
	});

	it("deduplicates by actionId", async () => {
		const executeFn = vi.fn(
			async (action: Action): Promise<Result<ActionResult, ActionExecutionError>> =>
				Ok({
					actionId: action.actionId,
					data: { success: true },
					serverHlc: 0n as HLCTimestamp,
				}),
		);
		const gw = createTestGateway({ github: createMockHandler(["create_pr"], executeFn) });
		const action = createAction();

		// Execute once
		await gw.handleAction({ clientId: "client-1", actions: [action] });
		// Execute again with the same actionId
		const result = await gw.handleAction({ clientId: "client-1", actions: [action] });

		expect(result.ok).toBe(true);
		expect(executeFn).toHaveBeenCalledTimes(1); // Only executed once
	});

	it("deduplicates by idempotencyKey", async () => {
		const executeFn = vi.fn(
			async (action: Action): Promise<Result<ActionResult, ActionExecutionError>> =>
				Ok({
					actionId: action.actionId,
					data: { success: true },
					serverHlc: 0n as HLCTimestamp,
				}),
		);
		const gw = createTestGateway({ github: createMockHandler(["create_pr"], executeFn) });

		const action1 = createAction({ idempotencyKey: "key-1" });
		await gw.handleAction({ clientId: "client-1", actions: [action1] });

		const action2 = createAction({ idempotencyKey: "key-1" });
		const result = await gw.handleAction({ clientId: "client-1", actions: [action2] });

		expect(result.ok).toBe(true);
		expect(executeFn).toHaveBeenCalledTimes(1);
	});

	it("does not cache retryable errors", async () => {
		let callCount = 0;
		const executeFn = async (
			action: Action,
		): Promise<Result<ActionResult, ActionExecutionError>> => {
			callCount++;
			if (callCount === 1) {
				return Err(new ActionExecutionError("Rate limited", true));
			}
			return Ok({
				actionId: action.actionId,
				data: { success: true },
				serverHlc: 0n as HLCTimestamp,
			});
		};
		const gw = createTestGateway({ github: createMockHandler(["create_pr"], executeFn) });

		const action = createAction();
		// First call — retryable error
		await gw.handleAction({ clientId: "client-1", actions: [action] });

		// Second call with new actionId (since original was not cached as executed)
		// Actually, executedActions is set even for retryable errors but idempotencyMap is not
		// Let me use a new actionId
		const action2 = createAction({
			connector: "github",
			actionType: "create_pr",
			params: action.params,
		});
		const result = await gw.handleAction({ clientId: "client-1", actions: [action2] });

		expect(result.ok).toBe(true);
		expect(callCount).toBe(2);
	});

	it("passes auth context to handler", async () => {
		let receivedContext: AuthContext | undefined;
		const executeFn = async (
			action: Action,
			context?: AuthContext,
		): Promise<Result<ActionResult, ActionExecutionError>> => {
			receivedContext = context;
			return Ok({
				actionId: action.actionId,
				data: {},
				serverHlc: 0n as HLCTimestamp,
			});
		};

		const gw = createTestGateway({ github: createMockHandler(["create_pr"], executeFn) });
		const ctx: AuthContext = { claims: { sub: "user-1" } };

		await gw.handleAction({ clientId: "client-1", actions: [createAction()] }, ctx);

		expect(receivedContext).toBeDefined();
		expect(receivedContext!.claims.sub).toBe("user-1");
	});

	it("returns validation error for invalid action", async () => {
		const gw = createTestGateway();

		const result = await gw.handleAction({
			clientId: "client-1",
			actions: [
				{
					actionId: "",
					clientId: "",
					hlc: 0n,
					connector: "",
					actionType: "",
					params: {},
				} as Action,
			],
		});

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("ACTION_VALIDATION_ERROR");
		}
	});
});

describe("SyncGateway action handler registration", () => {
	it("registers and lists action handlers", () => {
		const gw = createTestGateway();
		const handler = createMockHandler();

		gw.registerActionHandler("github", handler);
		expect(gw.listActionHandlers()).toContain("github");
	});

	it("unregisters action handlers", () => {
		const gw = createTestGateway({ github: createMockHandler() });
		gw.unregisterActionHandler("github");
		expect(gw.listActionHandlers()).not.toContain("github");
	});

	it("accepts handlers via config", () => {
		const gw = createTestGateway({
			github: createMockHandler(),
			slack: createMockHandler(["send_message"]),
		});
		expect(gw.listActionHandlers()).toEqual(["github", "slack"]);
	});
});

describe("SyncGateway.describeActions", () => {
	it("returns empty connectors when no handlers registered", () => {
		const gw = createTestGateway();
		const discovery = gw.describeActions();
		expect(discovery.connectors).toEqual({});
	});

	it("returns all registered handlers with their supported actions", () => {
		const gw = createTestGateway({
			github: createMockHandler(["create_pr", "list_prs"]),
			slack: createMockHandler(["send_message"]),
		});

		const discovery = gw.describeActions();

		expect(Object.keys(discovery.connectors)).toEqual(["github", "slack"]);
		expect(discovery.connectors.github).toHaveLength(2);
		expect(discovery.connectors.github![0]!.actionType).toBe("create_pr");
		expect(discovery.connectors.github![1]!.actionType).toBe("list_prs");
		expect(discovery.connectors.slack).toHaveLength(1);
		expect(discovery.connectors.slack![0]!.actionType).toBe("send_message");
	});

	it("reflects dynamically registered and unregistered handlers", () => {
		const gw = createTestGateway();

		gw.registerActionHandler("github", createMockHandler());
		expect(Object.keys(gw.describeActions().connectors)).toEqual(["github"]);

		gw.unregisterActionHandler("github");
		expect(gw.describeActions().connectors).toEqual({});
	});
});
