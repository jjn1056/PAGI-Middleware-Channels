# CLAUDE.md

This file provides guidance to Claude Code when working with this repository.

## Overview

PAGI-Channels provides cross-process and cross-server messaging for PAGI applications
via a middleware wrapper pattern. It follows the ASGI ecosystem pattern where the
server knows nothing about channel layers.

## Architecture

```
PAGI::Channels (middleware wrapper)
    └── Backend (pluggable)
        ├── Memory (v1 - single process)
        └── Redis (future - uses Async::Redis)
```

## Key Concepts

- **Channel**: Unique identifier for a connection (e.g., `conn.12345.1735689600.1`)
- **Topic/Group**: Named channel for broadcast messaging
- **wrap()**: Middleware that injects `pagi.channels` and `pagi.channel` into scope

## Backend Interface

Backends implement these methods via Role::Tiny:

| Method | Purpose |
|--------|---------|
| `send($channel, $msg)` | Queue message for channel |
| `poll($channel)` | Non-blocking check, returns msg or undef |
| `subscribe($channel, $topic)` | Add channel to topic group |
| `unsubscribe($channel, $topic)` | Remove channel from topic |
| `publish($topic, $msg, %opts)` | Send to all topic subscribers |
| `flush()` | Clear all state (testing) |
| `cleanup($channel)` | Remove channel and all subscriptions |

## Redis Backend (Future)

Will use `Async::Redis` - an event-loop agnostic Redis client built on Future::IO:

- Connection pooling via `Async::Redis::Pool` for high-throughput
- Safe concurrent commands via Response Queue pattern
- Pipeline support for batching

## Design Documents

- `docs/design/channel-layer.mkdn` - Full specification

## Dependencies

### v1 (Memory backend)
- Role::Tiny
- Future::AsyncAwait

### Future (Redis backend)
- Async::Redis
- JSON (or Sereal for serialization)
