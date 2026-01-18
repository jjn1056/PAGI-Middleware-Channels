# Task Queue with Progress Example

Demonstrates PAGI-Channels with:
- Task queue processing
- Real-time progress updates via pattern subscriptions
- Multiple worker coordination

## Running

1. Start Redis:
```bash
cd t && docker compose up -d && cd ..
```

2. Start a worker:
```bash
PAGI_CHANNELS_BACKEND=redis://localhost:6379 perl examples/task-queue/server.pl worker
```

3. In another terminal, run the demo:
```bash
PAGI_CHANNELS_BACKEND=redis://localhost:6379 perl examples/task-queue/server.pl demo
```

## Features Demonstrated

- **Task queue**: Workers poll for tasks from shared queue
- **Progress updates**: Pattern subscription (`task.{id}.**`) receives all task events
- **Broadcast completion**: Workers publish completion to all interested parties
