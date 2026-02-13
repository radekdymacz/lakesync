import { act, renderHook } from "@testing-library/react";
import { createElement } from "react";
import { describe, expect, it, vi } from "vitest";
import type { LakeSyncProviderProps } from "../context";
import { LakeSyncProvider, useLakeSync } from "../context";
import { useMutation } from "../use-mutation";

// ────────────────────────────────────────────────────────────
// Mock coordinator
// ────────────────────────────────────────────────────────────

interface Listeners {
	onChange: Array<(...args: unknown[]) => void>;
	onSyncStart: Array<(...args: unknown[]) => void>;
	onSyncComplete: Array<(...args: unknown[]) => void>;
	onError: Array<(...args: unknown[]) => void>;
}

function mockCoordinator() {
	const listeners: Listeners = {
		onChange: [],
		onSyncStart: [],
		onSyncComplete: [],
		onError: [],
	};

	return {
		tracker: {
			insert: vi.fn().mockResolvedValue({ ok: true, value: undefined }),
			update: vi.fn().mockResolvedValue({ ok: true, value: undefined }),
			delete: vi.fn().mockResolvedValue({ ok: true, value: undefined }),
			query: vi.fn().mockResolvedValue({ ok: true, value: [] }),
		},
		on: vi.fn((event: string, cb: (...args: unknown[]) => void) => {
			if (event in listeners) {
				listeners[event as keyof Listeners].push(cb);
			}
		}),
		off: vi.fn(),
		queueDepth: vi.fn().mockResolvedValue(0),
		lastSyncTime: null,
		state: { syncing: false, lastSyncTime: null, lastSyncedHlc: 0n },
		_listeners: listeners,
	};
}

type MockCoordinator = ReturnType<typeof mockCoordinator>;

function wrapper(coordinator: MockCoordinator) {
	return function Wrapper({ children }: { children: React.ReactNode }) {
		return createElement(
			LakeSyncProvider,
			{ coordinator } as unknown as LakeSyncProviderProps,
			children,
		);
	};
}

// ────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────

describe("useMutation", () => {
	it("insert calls tracker.insert and invalidates on success", async () => {
		const coord = mockCoordinator();

		const { result } = renderHook(() => ({ mutation: useMutation(), ctx: useLakeSync() }), {
			wrapper: wrapper(coord),
		});

		const versionBefore = result.current.ctx.dataVersion;

		await act(async () => {
			const res = await result.current.mutation.insert("todos", "row-1", { text: "Buy milk" });
			expect(res.ok).toBe(true);
		});

		expect(coord.tracker.insert).toHaveBeenCalledWith("todos", "row-1", { text: "Buy milk" });
		expect(result.current.ctx.dataVersion).toBe(versionBefore + 1);
	});

	it("update calls tracker.update and invalidates on success", async () => {
		const coord = mockCoordinator();

		const { result } = renderHook(() => ({ mutation: useMutation(), ctx: useLakeSync() }), {
			wrapper: wrapper(coord),
		});

		const versionBefore = result.current.ctx.dataVersion;

		await act(async () => {
			const res = await result.current.mutation.update("todos", "row-1", { done: 1 });
			expect(res.ok).toBe(true);
		});

		expect(coord.tracker.update).toHaveBeenCalledWith("todos", "row-1", { done: 1 });
		expect(result.current.ctx.dataVersion).toBe(versionBefore + 1);
	});

	it("remove calls tracker.delete and invalidates on success", async () => {
		const coord = mockCoordinator();

		const { result } = renderHook(() => ({ mutation: useMutation(), ctx: useLakeSync() }), {
			wrapper: wrapper(coord),
		});

		const versionBefore = result.current.ctx.dataVersion;

		await act(async () => {
			const res = await result.current.mutation.remove("todos", "row-1");
			expect(res.ok).toBe(true);
		});

		expect(coord.tracker.delete).toHaveBeenCalledWith("todos", "row-1");
		expect(result.current.ctx.dataVersion).toBe(versionBefore + 1);
	});

	it("does not invalidate on failure", async () => {
		const coord = mockCoordinator();
		coord.tracker.insert.mockResolvedValue({
			ok: false,
			error: new Error("DB_ERROR"),
		});

		const { result } = renderHook(() => ({ mutation: useMutation(), ctx: useLakeSync() }), {
			wrapper: wrapper(coord),
		});

		const versionBefore = result.current.ctx.dataVersion;

		await act(async () => {
			const res = await result.current.mutation.insert("todos", "row-1", { text: "fail" });
			expect(res.ok).toBe(false);
		});

		expect(result.current.ctx.dataVersion).toBe(versionBefore);
	});

	it("insert invalidates only the affected table", async () => {
		const coord = mockCoordinator();

		const { result } = renderHook(() => ({ mutation: useMutation(), ctx: useLakeSync() }), {
			wrapper: wrapper(coord),
		});

		await act(async () => {
			await result.current.mutation.insert("todos", "row-1", { text: "Buy milk" });
		});

		expect(result.current.ctx.tableVersions.get("todos")).toBe(1);
		expect(result.current.ctx.tableVersions.has("users")).toBe(false);
	});
});
