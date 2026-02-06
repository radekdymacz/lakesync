/** Todo item */
export interface Todo {
  id: string;
  title: string;
  completed: boolean;
  created_at: string;
  updated_at: string;
}

/** In-memory todo database */
export class TodoDB {
  private store = new Map<string, Todo>();

  getAll(): Todo[] {
    return [...this.store.values()].sort(
      (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
    );
  }

  get(id: string): Todo | undefined {
    return this.store.get(id);
  }

  /** Returns the previous state (for delta extraction) */
  set(todo: Todo): Todo | undefined {
    const prev = this.store.get(todo.id);
    this.store.set(todo.id, todo);
    return prev ?? undefined;
  }

  delete(id: string): Todo | undefined {
    const prev = this.store.get(id);
    this.store.delete(id);
    return prev ?? undefined;
  }
}
