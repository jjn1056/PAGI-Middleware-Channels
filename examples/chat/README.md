# Chat Example

Multi-user chat demonstrating PAGI-Channels cross-process messaging.

## What It Demonstrates

- **Presence tracking** - See who joins and leaves in real-time
- **Pub/sub messaging** - Messages broadcast to all users in a room
- **Cross-worker messaging** - Works with `--workers N` (multiple processes)
- **Message exclusion** - Sender doesn't receive their own messages

## Running

**Terminal 1: Start Redis**
```bash
cd t && docker compose up -d
```

**Terminal 2: Start the app**
```bash
cd examples/chat
pagi-server --workers 4 app.pl
```

Override the Redis URI with `PAGI_REDIS_URI=redis://host:port` if your
Redis isn't on `localhost:6379`.

**Browser: Open the chat**
- Open http://localhost:8000
- Enter a username and join
- Open multiple browser tabs with different usernames
- Chat messages and presence updates flow between all clients

## Features

The app serves its own static files (no separate web server needed):
- `/` - Chat UI (public/index.html)
- `/ws/chat` - WebSocket endpoint

## PAGI-Channels Features Used

```perl
# Subscribe with presence tracking
await $ch->subscribe("chat.$room",
    presence => { user => $username }
);

# Get current users
my @users = await $ch->list_presence("chat.$room");

# Publish (excluding sender)
await $ch->publish("chat.$room", $message, exclude => $my_channel);
```

## Architecture

```
Browser 1 ──┐                           ┌── Browser 3
            │      ┌─────────────┐      │
Browser 2 ──┼─────>│   Redis     │<─────┼── Browser 4
            │      │  (pub/sub)  │      │
            │      └─────────────┘      │
            │             ^             │
            v             v             v
      ┌─────────┐   ┌─────────┐   ┌─────────┐
      │ Worker 1│   │ Worker 2│   │ Worker 3│
      └─────────┘   └─────────┘   └─────────┘
            └─────────┴─────────┘
                 pagi-server
```

Each worker is a separate process. PAGI-Channels routes messages between them via Redis.

## Clean stale sessions

redis-cli FLUSHDB 
