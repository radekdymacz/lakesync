import { SyncGateway } from '@lakesync/gateway';
import { MinIOAdapter } from '@lakesync/adapter';
import { TodoDB } from './db';
import { SyncManager } from './sync';
import { setupUI } from './ui';

// Initialise database
const db = new TodoDB();

// Optionally connect to MinIO (if Docker is running)
let adapter: MinIOAdapter | undefined;
try {
  adapter = new MinIOAdapter({
    endpoint: 'http://localhost:9000',
    bucket: 'lakesync-dev',
    credentials: {
      accessKeyId: 'lakesync',
      secretAccessKey: 'lakesync123',
    },
  });
} catch {
  console.warn('MinIO not configured — running without storage backend');
}

// Create gateway
const gateway = new SyncGateway(
  {
    gatewayId: 'todo-gateway',
    maxBufferBytes: 10 * 1024 * 1024, // 10 MiB
    maxBufferAgeMs: 30_000, // 30s
  },
  adapter,
);

// Create sync manager
const sync = new SyncManager(gateway);

// Set up the UI
setupUI(db, sync);
