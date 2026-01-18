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
cd examples/chat
PAGI_CHANNELS_BACKEND=redis://localhost:6379 pagi-server --workers 4 app.pl
```

**Terminal 3: Serve the HTML**
```bash
cd examples/chat
python3 -m http.server 3000
```

**Browser: Open the chat UI**
- Open http://localhost:3000 in multiple browser windows
- Enter different usernames and connect
- Messages and presence updates flow between all clients

## Features

- **Room-based chat**: `/chat/{room}` path routing
- **Presence tracking**: `presence.join` and `presence.leave` events
- **Message exclusion**: Sender doesn't receive their own messages
- **Web UI**: Simple HTML/JS frontend in `index.html`
