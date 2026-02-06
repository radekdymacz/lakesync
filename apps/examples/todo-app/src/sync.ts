import { HLC, extractDelta } from '@lakesync/core';
import { MemoryQueue } from '@lakesync/client';
import { SyncGateway } from '@lakesync/gateway';
import type { Todo } from './db';

const CLIENT_ID = `client-${crypto.randomUUID()}`;

/** Coordinates the sync queue, gateway, and adapter */
export class SyncManager {
  private hlc = new HLC();
  private queue = new MemoryQueue();
  private gateway: SyncGateway;

  constructor(gateway: SyncGateway) {
    this.gateway = gateway;
  }

  /** Track a todo change and queue the delta */
  async trackChange(
    before: Todo | null | undefined,
    after: Todo | null | undefined,
    id: string,
  ): Promise<void> {
    const beforeRecord = before ? this.todoToRecord(before) : null;
    const afterRecord = after ? this.todoToRecord(after) : null;

    const delta = await extractDelta(beforeRecord, afterRecord, {
      table: 'todos',
      rowId: id,
      clientId: CLIENT_ID,
      hlc: this.hlc.now(),
    });

    if (delta) {
      await this.queue.push(delta);
      await this.syncToGateway();
    }
  }

  /** Push queued deltas to the gateway */
  private async syncToGateway(): Promise<void> {
    const peekResult = await this.queue.peek(100);
    if (!peekResult.ok || peekResult.value.length === 0) return;

    const entries = peekResult.value;
    const ids = entries.map((e) => e.id);
    await this.queue.markSending(ids);

    const pushResult = this.gateway.handlePush({
      clientId: CLIENT_ID,
      deltas: entries.map((e) => e.delta),
      lastSeenHlc: this.hlc.now(),
    });

    if (pushResult.ok) {
      await this.queue.ack(ids);
    } else {
      await this.queue.nack(ids);
    }
  }

  /** Flush gateway buffer to storage */
  async flush(): Promise<{ ok: boolean; message: string }> {
    const result = await this.gateway.flush();
    if (result.ok) {
      return { ok: true, message: 'Flushed successfully' };
    }
    return { ok: false, message: result.error.message };
  }

  /** Get sync statistics */
  get stats() {
    return this.gateway.bufferStats;
  }

  get clientId(): string {
    return CLIENT_ID;
  }

  private todoToRecord(todo: Todo): Record<string, unknown> {
    return {
      title: todo.title,
      completed: todo.completed,
      created_at: todo.created_at,
      updated_at: todo.updated_at,
    };
  }
}
