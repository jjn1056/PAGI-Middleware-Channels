# Chat with Workers Example

Demonstrates PAGI-Channels with:
- WebSocket chat with multiple rooms
- Presence tracking (who's online)
- Worker pool for background tasks
- Request/reply pattern

## Architecture

```
+-------------+     +-------------+     +-------------+
|  Worker 1   |     |  Worker 2   |     |  Worker 3   |
|  (WS conns) |     |  (WS conns) |     |  (WS conns) |
+------+------+     +------+------+     +------+------+
       |                   |                   |
       +-------------------+-------------------+
                           |
                    +------+------+
                    |    Redis    |
                    |  (channels) |
                    +-------------+
```

## Running

1. Start Redis:
```bash
cd t && docker compose up -d && cd ..
```

2. Start workers (run multiple):
```bash
PAGI_CHANNELS_BACKEND=redis://localhost:6379 perl examples/chat-with-workers/worker.pl
```

3. Start chat server:
```bash
PAGI_CHANNELS_BACKEND=redis://localhost:6379 perl examples/chat-with-workers/app.pl
```

4. Connect WebSocket clients to `/chat/general?user=alice`

## Features Demonstrated

- **Presence tracking**: Workers register themselves, clients can see who's online
- **Request/reply**: Client sends image processing request, worker replies with result
- **Cross-process messaging**: Workers and app communicate via Redis channel layer
