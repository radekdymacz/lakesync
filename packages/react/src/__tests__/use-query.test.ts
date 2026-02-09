import { act, renderHook, waitFor } from "@testing-library/react";
import { createElement } from "react";
import { describe, expect, it, vi } from "vitest";
import type { LakeSyncProviderProps } from "../context";
import { LakeSyncProvider } from "../context";
import { useQuery } from "../use-query";

// ────────────────────────────────────────────────────────────
// Mock coordinator
// ────────────────────────────────────────────────────────────

interface Listeners {
	onChange: Array<(...args: unknown[]) => void>;
	onSyncComplete: Array<(...args: unknown[]) => void>;
	onError: Array<(...args: unknown[]) => void>;
}

function mockCoordinator(
	queryResult: { ok: true; value: unknown[] } | { ok: false; error: Error },
) {
	const listeners: Listeners = {
		onChange: [],
		onSyncComplete: [],
		onError: [],
	};

	return {
		tracker: {
			insert: vi.fn(),
			update: vi.fn(),
			delete: vi.fn(),
			query: vi.fn().mockResolvedValue(queryResult),
		},
		on: vi.fn((event: string, cb: (...args: unknown[]) => void) => {
			if (event in listeners) {
				listeners[event as keyof Listeners].push(cb);
			}
		}),
		off: vi.fn(),
		queueDepth: vi.fn().mockResolvedValue(0),
		lastSyncTime: null,
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

interface Todo {
	_rowId: string;
	text: string;
	done: number;
}

describe("useQuery", () => {
	it("returns loading state initially, then resolves data", async () => {
		const rows: Todo[] = [{ _rowId: "1", text: "Buy milk", done: 0 }];
		const coord = mockCoordinator({ ok: true, value: rows });

		const { result } = renderHook(() => useQuery<Todo>("SELECT * FROM todos"), {
			wrapper: wrapper(coord),
		});

		// Initially loading
		expect(result.current.isLoading).toBe(true);

		await waitFor(() => {
			expect(result.current.isLoading).toBe(false);
		});

		expect(result.current.data).toEqual(rows);
		expect(result.current.error).toBeNull();
	});

	it("returns error when query fails", async () => {
		const err = new Error("DB_ERROR");
		const coord = mockCoordinator({ ok: false, error: err });

		const { result } = renderHook(() => useQuery<Todo>("SELECT * FROM bad_table"), {
			wrapper: wrapper(coord),
		});

		await waitFor(() => {
			expect(result.current.isLoading).toBe(false);
		});

		expect(result.current.error).toBe(err);
		expect(result.current.data).toEqual([]);
	});

	it("re-runs query when dataVersion changes (onChange)", async () => {
		const coord = mockCoordinator({ ok: true, value: [{ _rowId: "1", text: "A", done: 0 }] });

		const { result } = renderHook(() => useQuery<Todo>("SELECT * FROM todos"), {
			wrapper: wrapper(coord),
		});

		await waitFor(() => {
			expect(result.current.isLoading).toBe(false);
		});

		expect(coord.tracker.query).toHaveBeenCalledTimes(1);

		// Simulate remote sync — triggers onChange
		coord.tracker.query.mockResolvedValue({
			ok: true,
			value: [
				{ _rowId: "1", text: "A", done: 0 },
				{ _rowId: "2", text: "B", done: 0 },
			],
		});

		act(() => {
			for (const cb of coord._listeners.onChange) {
				cb(1);
			}
		});

		await waitFor(() => {
			expect(result.current.data).toHaveLength(2);
		});

		expect(coord.tracker.query).toHaveBeenCalledTimes(2);
	});

	it("re-runs query when sql changes", async () => {
		const coord = mockCoordinator({ ok: true, value: [] });

		let sql = "SELECT * FROM todos WHERE done = 0";
		const { result, rerender } = renderHook(() => useQuery<Todo>(sql), { wrapper: wrapper(coord) });

		await waitFor(() => {
			expect(result.current.isLoading).toBe(false);
		});

		expect(coord.tracker.query).toHaveBeenCalledTimes(1);

		sql = "SELECT * FROM todos WHERE done = 1";
		rerender();

		await waitFor(() => {
			expect(coord.tracker.query).toHaveBeenCalledTimes(2);
		});
	});

	it("passes params to tracker.query", async () => {
		const coord = mockCoordinator({ ok: true, value: [] });

		renderHook(() => useQuery<Todo>("SELECT * FROM todos WHERE done = ?", [0]), {
			wrapper: wrapper(coord),
		});

		await waitFor(() => {
			expect(coord.tracker.query).toHaveBeenCalledWith("SELECT * FROM todos WHERE done = ?", [0]);
		});
	});

	it("refetch() triggers a manual re-run", async () => {
		const coord = mockCoordinator({ ok: true, value: [] });

		const { result } = renderHook(() => useQuery<Todo>("SELECT * FROM todos"), {
			wrapper: wrapper(coord),
		});

		await waitFor(() => {
			expect(result.current.isLoading).toBe(false);
		});

		expect(coord.tracker.query).toHaveBeenCalledTimes(1);

		act(() => {
			result.current.refetch();
		});

		await waitFor(() => {
			expect(coord.tracker.query).toHaveBeenCalledTimes(2);
		});
	});
});
