# Chat Example

Simple chat with presence tracking demonstrating PAGI-Channels cross-process messaging.

## What It Demonstrates

When you run this with multiple workers:

```bash
pagi-server --workers 4 app.pl
```

Each worker is a separate process, but they all share:
- **Chat messages** - Sent to all users in a room (via Redis pub/sub)
- **Presence tracking** - See who joins/leaves (synced across all workers)

This proves PAGI-Channels is working correctly across process boundaries.

## Running

**Terminal 1: Redis**
```bash
cd t && docker compose up -d
```

**Terminal 2: App (with multiple workers)**
```bash
PAGI_CHANNELS_BACKEND=redis://localhost:6379 pagi-server --workers 4 examples/chat/app.pl
```

## Testing

Connect multiple WebSocket clients to different rooms:

```
ws://localhost:8000/chat/general?user=alice
ws://localhost:8000/chat/general?user=bob
```

Messages from Alice will appear for Bob (and vice versa), even if they're being handled by different worker processes.

## Features

- **Room-based chat**: `/chat/{room}` path routing
- **Presence tracking**: `presence.join` and `presence.leave` events
- **Message exclusion**: Sender doesn't receive their own messages
