import type { SyncCoordinator } from "./sync";

/** Todo row shape returned from SQLite queries */
interface Todo {
	_rowId: string;
	title: string;
	completed: number;
	created_at: string;
	updated_at: string;
}

/** Wire up DOM event handlers and rendering */
export function setupUI(coordinator: SyncCoordinator): void {
	const input = document.getElementById("todo-input") as HTMLInputElement;
	const addBtn = document.getElementById("add-btn") as HTMLButtonElement;
	const list = document.getElementById("todo-list") as HTMLUListElement;
	const status = document.getElementById("status") as HTMLDivElement;
	const flushBtn = document.getElementById("flush-btn") as HTMLButtonElement;

	async function render(): Promise<void> {
		const result = await coordinator.tracker.query<Todo>(
			"SELECT * FROM todos ORDER BY created_at DESC",
		);
		if (!result.ok) {
			status.textContent = `Error loading todos: ${result.error.message}`;
			return;
		}

		const todos = result.value;
		list.innerHTML = "";

		for (const todo of todos) {
			const li = document.createElement("li");
			li.className = "todo-item";

			const checkbox = document.createElement("input");
			checkbox.type = "checkbox";
			checkbox.checked = !!todo.completed;
			checkbox.addEventListener("change", async () => {
				const newCompleted = checkbox.checked ? 1 : 0;
				const now = new Date().toISOString();
				const updateResult = await coordinator.tracker.update("todos", todo._rowId, {
					completed: newCompleted,
					updated_at: now,
				});
				if (!updateResult.ok) {
					status.textContent = `Error updating todo: ${updateResult.error.message}`;
				}
				await coordinator.pushToGateway();
				await render();
				await updateStatus();
			});

			const span = document.createElement("span");
			span.textContent = todo.title;
			if (todo.completed) span.className = "completed";

			const deleteBtn = document.createElement("button");
			deleteBtn.textContent = "\u00d7";
			deleteBtn.addEventListener("click", async () => {
				const deleteResult = await coordinator.tracker.delete("todos", todo._rowId);
				if (!deleteResult.ok) {
					status.textContent = `Error deleting todo: ${deleteResult.error.message}`;
				}
				await coordinator.pushToGateway();
				await render();
				await updateStatus();
			});

			li.appendChild(checkbox);
			li.appendChild(span);
			li.appendChild(deleteBtn);
			list.appendChild(li);
		}
	}

	async function updateStatus(): Promise<void> {
		const stats = coordinator.stats;
		const depth = await coordinator.queueDepth();
		const syncState = depth === 0 ? "synced" : `${depth} pending`;
		status.textContent = `Buffer: ${stats.logSize} deltas | ${stats.indexSize} rows | Queue: ${syncState} | Client: ${coordinator.clientId.slice(0, 8)}...`;
	}

	addBtn.addEventListener("click", async () => {
		const title = input.value.trim();
		if (!title) return;

		const id = crypto.randomUUID();
		const now = new Date().toISOString();
		const insertResult = await coordinator.tracker.insert("todos", id, {
			title,
			completed: 0,
			created_at: now,
			updated_at: now,
		});

		if (!insertResult.ok) {
			status.textContent = `Error adding todo: ${insertResult.error.message}`;
			return;
		}

		await coordinator.pushToGateway();
		input.value = "";
		await render();
		await updateStatus();
	});

	input.addEventListener("keydown", (e) => {
		if (e.key === "Enter") addBtn.click();
	});

	flushBtn.addEventListener("click", async () => {
		status.textContent = "Flushing...";
		const result = await coordinator.flush();
		status.textContent = result.ok
			? `Flushed! ${result.message}`
			: `Flush failed: ${result.message}`;
		setTimeout(() => {
			void updateStatus();
		}, 2000);
	});

	void render();
	void updateStatus();
}
