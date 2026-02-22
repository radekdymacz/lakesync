import { act, renderHook } from "@testing-library/react";
import { createElement } from "react";
import { describe, expect, it, vi } from "vitest";
import type { LakeSyncProviderProps } from "../context";
import { LakeSyncProvider, useLakeSync } from "../context";

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
			insert: vi.fn(),
			update: vi.fn(),
			delete: vi.fn(),
			query: vi.fn(),
		},
		engine: {
			syncing: false,
			lastSyncTime: null,
			lastSyncedHlc: 0n,
			clientId: "test-client",
		},
		on: vi.fn((event: string, cb: (...args: unknown[]) => void) => {
			if (event in listeners) {
				listeners[event as keyof Listeners].push(cb);
			}
		}),
		off: vi.fn((event: string, cb: (...args: unknown[]) => void) => {
			if (event in listeners) {
				const arr = listeners[event as keyof Listeners];
				const idx = arr.indexOf(cb);
				if (idx !== -1) arr.splice(idx, 1);
			}
		}),
		subscribe: vi.fn((handlers: Record<string, (...args: unknown[]) => void>) => {
			for (const [event, handler] of Object.entries(handlers)) {
				if (event in listeners && handler) {
					listeners[event as keyof Listeners].push(handler);
				}
			}
			return () => {
				for (const [event, handler] of Object.entries(handlers)) {
					if (event in listeners && handler) {
						const arr = listeners[event as keyof Listeners];
						const idx = arr.indexOf(handler);
						if (idx !== -1) arr.splice(idx, 1);
					}
				}
			};
		}),
		queueDepth: vi.fn().mockResolvedValue(0),
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

describe("LakeSyncProvider + useLakeSync", () => {
	it("provides coordinator and tracker via context", () => {
		const coord = mockCoordinator();
		const { result } = renderHook(() => useLakeSync(), {
			wrapper: wrapper(coord),
		});

		expect(result.current.coordinator).toBe(coord);
		expect(result.current.tracker).toBe(coord.tracker);
	});

	it("throws when used outside provider", () => {
		expect(() => {
			renderHook(() => useLakeSync());
		}).toThrow(/must be used within a <LakeSyncProvider>/);
	});

	it("subscribes to onChange on mount and unsubscribes on unmount", () => {
		const coord = mockCoordinator();
		const { unmount } = renderHook(() => useLakeSync(), {
			wrapper: wrapper(coord),
		});

		expect(coord.subscribe).toHaveBeenCalledWith(
			expect.objectContaining({ onChange: expect.any(Function) }),
		);

		unmount();

		// subscribe returns an unsubscribe function that is called on unmount
		expect(coord._listeners.onChange).toHaveLength(0);
	});

	it("increments dataVersion when onChange fires", () => {
		const coord = mockCoordinator();
		const { result } = renderHook(() => useLakeSync(), {
			wrapper: wrapper(coord),
		});

		expect(result.current.dataVersion).toBe(0);

		act(() => {
			for (const cb of coord._listeners.onChange) {
				cb(1);
			}
		});

		expect(result.current.dataVersion).toBe(1);
	});

	it("increments dataVersion when invalidate() is called", () => {
		const coord = mockCoordinator();
		const { result } = renderHook(() => useLakeSync(), {
			wrapper: wrapper(coord),
		});

		expect(result.current.dataVersion).toBe(0);

		act(() => {
			result.current.invalidate();
		});

		expect(result.current.dataVersion).toBe(1);
	});

	it("provides tableVersions map in context", () => {
		const coord = mockCoordinator();
		const { result } = renderHook(() => useLakeSync(), {
			wrapper: wrapper(coord),
		});

		expect(result.current.tableVersions).toBeInstanceOf(Map);
		expect(result.current.tableVersions.size).toBe(0);
	});

	it("bumps globalVersion and dataVersion when onChange fires", () => {
		const coord = mockCoordinator();
		const { result } = renderHook(() => useLakeSync(), {
			wrapper: wrapper(coord),
		});

		expect(result.current.globalVersion).toBe(0);

		act(() => {
			for (const cb of coord._listeners.onChange) {
				cb(1);
			}
		});

		expect(result.current.globalVersion).toBe(1);
		expect(result.current.dataVersion).toBe(1);
	});

	it("invalidateTables bumps only specified tables", () => {
		const coord = mockCoordinator();
		const { result } = renderHook(() => useLakeSync(), {
			wrapper: wrapper(coord),
		});

		act(() => {
			result.current.invalidateTables(["todos"]);
		});

		expect(result.current.tableVersions.get("todos")).toBe(1);
		expect(result.current.tableVersions.has("users")).toBe(false);
		expect(result.current.dataVersion).toBe(1);
	});
});
