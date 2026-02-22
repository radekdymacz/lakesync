import { act, renderHook, waitFor } from "@testing-library/react";
import { createElement } from "react";
import { describe, expect, it, vi } from "vitest";
import type { LakeSyncProviderProps } from "../context";
import { LakeSyncProvider } from "../context";
import { useSyncStatus } from "../use-sync-status";

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

	const coord = {
		tracker: {
			insert: vi.fn(),
			update: vi.fn(),
			delete: vi.fn(),
			query: vi.fn(),
		},
		engine: {
			syncing: false,
			lastSyncTime: null as Date | null,
			lastSyncedHlc: 0n,
			clientId: "test-client",
		},
		on: vi.fn((event: string, cb: (...args: unknown[]) => void) => {
			if (event in listeners) {
				listeners[event as keyof Listeners].push(cb);
			}
		}),
		off: vi.fn(),
		subscribe: vi.fn((handlers: Record<string, (...args: unknown[]) => void>) => {
			for (const [event, handler] of Object.entries(handlers)) {
				if (event in listeners && handler) {
					listeners[event as keyof Listeners].push(handler);
				}
			}
			return () => {};
		}),
		queueDepth: vi.fn().mockResolvedValue(0),
		_listeners: listeners,
	};

	return coord;
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

describe("useSyncStatus", () => {
	it("returns initial state", async () => {
		const coord = mockCoordinator();

		const { result } = renderHook(() => useSyncStatus(), {
			wrapper: wrapper(coord),
		});

		await waitFor(() => {
			expect(result.current.isSyncing).toBe(false);
			expect(result.current.lastSyncTime).toBeNull();
			expect(result.current.queueDepth).toBe(0);
			expect(result.current.error).toBeNull();
		});
	});

	it("updates lastSyncTime and clears error on syncComplete", async () => {
		const coord = mockCoordinator();
		const syncTime = new Date();
		coord.engine.lastSyncTime = syncTime;
		coord.engine.lastSyncTime = syncTime;

		const { result } = renderHook(() => useSyncStatus(), {
			wrapper: wrapper(coord),
		});

		await waitFor(() => {
			expect(result.current.queueDepth).toBe(0);
		});

		act(() => {
			for (const cb of coord._listeners.onSyncComplete) {
				cb();
			}
		});

		await waitFor(() => {
			expect(result.current.lastSyncTime).toBe(syncTime);
			expect(result.current.error).toBeNull();
		});
	});

	it("captures error on onError", async () => {
		const coord = mockCoordinator();
		const err = new Error("sync failed");

		const { result } = renderHook(() => useSyncStatus(), {
			wrapper: wrapper(coord),
		});

		await waitFor(() => {
			expect(result.current.queueDepth).toBe(0);
		});

		act(() => {
			for (const cb of coord._listeners.onError) {
				cb(err);
			}
		});

		expect(result.current.error).toBe(err);
		expect(result.current.isSyncing).toBe(false);
	});

	it("refreshes queue depth on onChange", async () => {
		const coord = mockCoordinator();
		coord.queueDepth.mockResolvedValue(3);

		const { result } = renderHook(() => useSyncStatus(), {
			wrapper: wrapper(coord),
		});

		await waitFor(() => {
			expect(result.current.queueDepth).toBe(3);
		});

		coord.queueDepth.mockResolvedValue(1);

		act(() => {
			for (const cb of coord._listeners.onChange) {
				cb(1);
			}
		});

		await waitFor(() => {
			expect(result.current.queueDepth).toBe(1);
		});
	});

	it("clears error after successful sync", async () => {
		const coord = mockCoordinator();
		const err = new Error("sync failed");

		const { result } = renderHook(() => useSyncStatus(), {
			wrapper: wrapper(coord),
		});

		await waitFor(() => {
			expect(result.current.queueDepth).toBe(0);
		});

		// First: error
		act(() => {
			for (const cb of coord._listeners.onError) {
				cb(err);
			}
		});

		expect(result.current.error).toBe(err);

		// Then: success clears error
		coord.engine.lastSyncTime = new Date();
		act(() => {
			for (const cb of coord._listeners.onSyncComplete) {
				cb();
			}
		});

		await waitFor(() => {
			expect(result.current.error).toBeNull();
		});
	});

	it("sets isSyncing on onSyncStart", async () => {
		const coord = mockCoordinator();

		const { result } = renderHook(() => useSyncStatus(), {
			wrapper: wrapper(coord),
		});

		await waitFor(() => {
			expect(result.current.isSyncing).toBe(false);
		});

		act(() => {
			for (const cb of coord._listeners.onSyncStart) {
				cb();
			}
		});

		expect(result.current.isSyncing).toBe(true);
	});
});
