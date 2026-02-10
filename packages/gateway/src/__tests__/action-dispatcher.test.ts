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
import { ActionDispatcher } from "../action-dispatcher";

/** Create a mock ActionHandler that succeeds by default. */
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

/** Simple hlcNow callback for tests. */
const hlcNow = () => 100n as HLCTimestamp;

describe("ActionDispatcher.dispatch", () => {
	it("dispatches a single action and returns Ok with results", async () => {
		const dispatcher = new ActionDispatcher({ github: createMockHandler() });
		const action = createAction();

		const result = await dispatcher.dispatch({ clientId: "client-1", actions: [action] }, hlcNow);

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.results).toHaveLength(1);
		expect(result.value.results[0]!.actionId).toBe(action.actionId);
		expect("data" in result.value.results[0]!).toBe(true);
	});

	it("dispatches multiple actions in one push and returns all results", async () => {
		const dispatcher = new ActionDispatcher({ github: createMockHandler() });
		const action1 = createAction();
		const action2 = createAction();

		const result = await dispatcher.dispatch(
			{ clientId: "client-1", actions: [action1, action2] },
			hlcNow,
		);

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.results).toHaveLength(2);
		expect(result.value.results[0]!.actionId).toBe(action1.actionId);
		expect(result.value.results[1]!.actionId).toBe(action2.actionId);
	});

	it("returns ACTION_NOT_SUPPORTED for unknown connector", async () => {
		const dispatcher = new ActionDispatcher();
		const action = createAction({ connector: "unknown" });

		const result = await dispatcher.dispatch({ clientId: "client-1", actions: [action] }, hlcNow);

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		const r = result.value.results[0]!;
		expect("code" in r).toBe(true);
		if ("code" in r) {
			expect(r.code).toBe("ACTION_NOT_SUPPORTED");
		}
	});

	it("returns ACTION_NOT_SUPPORTED for unsupported action type", async () => {
		const dispatcher = new ActionDispatcher({ github: createMockHandler(["list_prs"]) });
		const action = createAction({ actionType: "create_pr" });

		const result = await dispatcher.dispatch({ clientId: "client-1", actions: [action] }, hlcNow);

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		const r = result.value.results[0]!;
		expect("code" in r).toBe(true);
		if ("code" in r) {
			expect(r.code).toBe("ACTION_NOT_SUPPORTED");
			expect(r.message).toContain("create_pr");
		}
	});

	it("deduplicates by actionId — executeFn called only once", async () => {
		const executeFn = vi.fn(
			async (action: Action): Promise<Result<ActionResult, ActionExecutionError>> =>
				Ok({
					actionId: action.actionId,
					data: { success: true },
					serverHlc: 0n as HLCTimestamp,
				}),
		);
		const dispatcher = new ActionDispatcher({
			github: createMockHandler(["create_pr"], executeFn),
		});
		const action = createAction();

		// Execute once
		await dispatcher.dispatch({ clientId: "client-1", actions: [action] }, hlcNow);
		// Execute again with the same actionId
		const result = await dispatcher.dispatch({ clientId: "client-1", actions: [action] }, hlcNow);

		expect(result.ok).toBe(true);
		expect(executeFn).toHaveBeenCalledTimes(1);
		if (result.ok) {
			expect(result.value.results).toHaveLength(1);
			expect(result.value.results[0]!.actionId).toBe(action.actionId);
		}
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
		const dispatcher = new ActionDispatcher({
			github: createMockHandler(["create_pr"], executeFn),
		});

		const action1 = createAction({ idempotencyKey: "key-1" });
		await dispatcher.dispatch({ clientId: "client-1", actions: [action1] }, hlcNow);

		const action2 = createAction({ idempotencyKey: "key-1" });
		const result = await dispatcher.dispatch({ clientId: "client-1", actions: [action2] }, hlcNow);

		expect(result.ok).toBe(true);
		expect(executeFn).toHaveBeenCalledTimes(1);
	});

	it("does not cache retryable errors — allows retry with new actionId", async () => {
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
		const dispatcher = new ActionDispatcher({
			github: createMockHandler(["create_pr"], executeFn),
		});

		const action = createAction();
		// First call — retryable error
		await dispatcher.dispatch({ clientId: "client-1", actions: [action] }, hlcNow);

		// Retry with new actionId
		const action2 = createAction();
		const result = await dispatcher.dispatch({ clientId: "client-1", actions: [action2] }, hlcNow);

		expect(result.ok).toBe(true);
		expect(callCount).toBe(2);
		if (result.ok) {
			expect("data" in result.value.results[0]!).toBe(true);
		}
	});

	it("caches non-retryable errors", async () => {
		const executeFn = vi.fn(
			async (_action: Action): Promise<Result<ActionResult, ActionExecutionError>> =>
				Err(new ActionExecutionError("Forbidden", false)),
		);
		const dispatcher = new ActionDispatcher({
			github: createMockHandler(["create_pr"], executeFn),
		});

		const action = createAction();
		await dispatcher.dispatch({ clientId: "client-1", actions: [action] }, hlcNow);
		// Second call with same actionId should return cached error
		const result = await dispatcher.dispatch({ clientId: "client-1", actions: [action] }, hlcNow);

		expect(result.ok).toBe(true);
		expect(executeFn).toHaveBeenCalledTimes(1);
		if (result.ok) {
			const r = result.value.results[0]!;
			expect("code" in r).toBe(true);
			if ("code" in r) {
				expect(r.code).toBe("ACTION_EXECUTION_ERROR");
			}
		}
	});

	it("passes auth context through to handler", async () => {
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

		const dispatcher = new ActionDispatcher({
			github: createMockHandler(["create_pr"], executeFn),
		});
		const ctx: AuthContext = { claims: { sub: "user-1" } };

		await dispatcher.dispatch({ clientId: "client-1", actions: [createAction()] }, hlcNow, ctx);

		expect(receivedContext).toBeDefined();
		expect(receivedContext!.claims.sub).toBe("user-1");
	});

	it("returns Err(ActionValidationError) for structurally invalid action", async () => {
		const dispatcher = new ActionDispatcher();

		const result = await dispatcher.dispatch(
			{
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
			},
			hlcNow,
		);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("ACTION_VALIDATION_ERROR");
		}
	});

	it("uses hlcNow callback for serverHlc in response", async () => {
		const customHlc = 999n as HLCTimestamp;
		const dispatcher = new ActionDispatcher({ github: createMockHandler() });

		const result = await dispatcher.dispatch(
			{ clientId: "client-1", actions: [createAction()] },
			() => customHlc,
		);

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.serverHlc).toBe(customHlc);
	});

	it("handles mixed success and error in a single push", async () => {
		const dispatcher = new ActionDispatcher({
			github: createMockHandler(["create_pr"]),
		});

		const goodAction = createAction();
		const badAction = createAction({ connector: "unknown" });

		const result = await dispatcher.dispatch(
			{ clientId: "client-1", actions: [goodAction, badAction] },
			hlcNow,
		);

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.results).toHaveLength(2);
		// First action succeeded
		expect("data" in result.value.results[0]!).toBe(true);
		// Second action has error code
		expect("code" in result.value.results[1]!).toBe(true);
	});
});

describe("ActionDispatcher registration", () => {
	it("registerHandler adds a handler", () => {
		const dispatcher = new ActionDispatcher();
		dispatcher.registerHandler("github", createMockHandler());
		expect(dispatcher.listHandlers()).toContain("github");
	});

	it("unregisterHandler removes a handler", () => {
		const dispatcher = new ActionDispatcher({ github: createMockHandler() });
		dispatcher.unregisterHandler("github");
		expect(dispatcher.listHandlers()).not.toContain("github");
	});

	it("listHandlers returns all registered names", () => {
		const dispatcher = new ActionDispatcher({
			github: createMockHandler(),
			slack: createMockHandler(["send_message"]),
		});
		expect(dispatcher.listHandlers()).toEqual(["github", "slack"]);
	});

	it("constructor accepts handlers via config", () => {
		const dispatcher = new ActionDispatcher({
			github: createMockHandler(),
			slack: createMockHandler(["send_message"]),
		});
		expect(dispatcher.listHandlers()).toHaveLength(2);
		expect(dispatcher.listHandlers()).toContain("github");
		expect(dispatcher.listHandlers()).toContain("slack");
	});

	it("dispatch fails after handler is unregistered", async () => {
		const dispatcher = new ActionDispatcher({ github: createMockHandler() });
		dispatcher.unregisterHandler("github");

		const result = await dispatcher.dispatch(
			{ clientId: "client-1", actions: [createAction()] },
			hlcNow,
		);

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		const r = result.value.results[0]!;
		expect("code" in r).toBe(true);
		if ("code" in r) {
			expect(r.code).toBe("ACTION_NOT_SUPPORTED");
		}
	});
});

describe("ActionDispatcher.describe", () => {
	it("returns empty connectors when no handlers registered", () => {
		const dispatcher = new ActionDispatcher();
		const discovery = dispatcher.describe();
		expect(discovery.connectors).toEqual({});
	});

	it("returns all registered handlers with their supported actions", () => {
		const dispatcher = new ActionDispatcher({
			github: createMockHandler(["create_pr", "list_prs"]),
			slack: createMockHandler(["send_message"]),
		});

		const discovery = dispatcher.describe();

		expect(Object.keys(discovery.connectors)).toEqual(["github", "slack"]);
		expect(discovery.connectors.github).toHaveLength(2);
		expect(discovery.connectors.github![0]!.actionType).toBe("create_pr");
		expect(discovery.connectors.github![1]!.actionType).toBe("list_prs");
		expect(discovery.connectors.slack).toHaveLength(1);
		expect(discovery.connectors.slack![0]!.actionType).toBe("send_message");
	});

	it("reflects dynamic register and unregister", () => {
		const dispatcher = new ActionDispatcher();

		dispatcher.registerHandler("github", createMockHandler());
		expect(Object.keys(dispatcher.describe().connectors)).toEqual(["github"]);

		dispatcher.unregisterHandler("github");
		expect(dispatcher.describe().connectors).toEqual({});
	});
});
