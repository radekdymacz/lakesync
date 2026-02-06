import type { Todo, TodoDB } from "./db";
import type { SyncManager } from "./sync";

/** Wire up DOM event handlers and rendering */
export function setupUI(db: TodoDB, sync: SyncManager): void {
	const input = document.getElementById("todo-input") as HTMLInputElement;
	const addBtn = document.getElementById("add-btn") as HTMLButtonElement;
	const list = document.getElementById("todo-list") as HTMLUListElement;
	const status = document.getElementById("status") as HTMLDivElement;
	const flushBtn = document.getElementById("flush-btn") as HTMLButtonElement;

	function render(): void {
		const todos = db.getAll();
		list.innerHTML = "";

		for (const todo of todos) {
			const li = document.createElement("li");
			li.className = "todo-item";

			const checkbox = document.createElement("input");
			checkbox.type = "checkbox";
			checkbox.checked = todo.completed;
			checkbox.addEventListener("change", async () => {
				const before = db.get(todo.id);
				const updated: Todo = {
					...todo,
					completed: checkbox.checked,
					updated_at: new Date().toISOString(),
				};
				db.set(updated);
				await sync.trackChange(before ?? null, updated, todo.id);
				render();
				updateStatus();
			});

			const span = document.createElement("span");
			span.textContent = todo.title;
			if (todo.completed) span.className = "completed";

			const deleteBtn = document.createElement("button");
			deleteBtn.textContent = "\u00d7";
			deleteBtn.addEventListener("click", async () => {
				const before = db.delete(todo.id);
				await sync.trackChange(before ?? null, null, todo.id);
				render();
				updateStatus();
			});

			li.appendChild(checkbox);
			li.appendChild(span);
			li.appendChild(deleteBtn);
			list.appendChild(li);
		}
	}

	function updateStatus(): void {
		const stats = sync.stats;
		status.textContent = `Buffer: ${stats.logSize} deltas | ${stats.indexSize} rows | Client: ${sync.clientId.slice(0, 8)}...`;
	}

	addBtn.addEventListener("click", async () => {
		const title = input.value.trim();
		if (!title) return;

		const todo: Todo = {
			id: crypto.randomUUID(),
			title,
			completed: false,
			created_at: new Date().toISOString(),
			updated_at: new Date().toISOString(),
		};

		db.set(todo);
		await sync.trackChange(null, todo, todo.id);
		input.value = "";
		render();
		updateStatus();
	});

	input.addEventListener("keydown", (e) => {
		if (e.key === "Enter") addBtn.click();
	});

	flushBtn.addEventListener("click", async () => {
		status.textContent = "Flushing...";
		const result = await sync.flush();
		status.textContent = result.ok
			? `Flushed! ${result.message}`
			: `Flush failed: ${result.message}`;
		setTimeout(updateStatus, 2000);
	});

	render();
	updateStatus();
}
