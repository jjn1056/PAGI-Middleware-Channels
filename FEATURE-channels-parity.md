# Feature: Django Channels Parity (and Beyond)

## Status: BRAINSTORMING

## Repo Recon

| Item | Finding |
|------|---------|
| Layout | `docs/design/` only - no `lib/` or `t/` yet |
| Test framework | Test2::V0 (per PAGI conventions) |
| Dependencies | cpanfile (to be created) |
| Build system | Dist::Zilla |
| Test command | `prove -l t/` |
| Min Perl | 5.018+ (for signatures, postderef) |
| Conventions | Follow PAGI patterns |

**Note:** This is a greenfield project. Design doc exists at `docs/design/channel-layer.mkdn`.

## CRITICAL CONSTRAINT: Loop Agnosticism

**Main library code MUST use Future::IO only** - no direct IO::Async, Mojo::IOLoop, or other loop-specific code.

- `lib/` → Future::IO primitives only
- `t/` → IO::Async for test harness (via Future::IO backend)
- `examples/` → Can show IO::Async examples
- `docs/` → Must document loop agnosticism prominently

This ensures PAGI-Channels works with any Future::IO-compatible event loop.

## Research Summary

### Django Channels Features

| Feature | Django Built-in | PAGI Design |
|---------|-----------------|-------------|
| group_add/discard/send | Yes | Yes (subscribe/unsubscribe/publish) |
| send (point-to-point) | Yes | Yes |
| Memory backend | Yes (dev only) | Yes (v1) |
| Redis backend | Yes (channels_redis) | Planned |
| Group expiry | Yes | Yes (86400s default) |
| Message expiry | Yes | Yes (60s default) |
| Capacity limits | Yes | Yes (100 msg default) |
| Automatic cleanup | Yes | Yes |
| Auth middleware | Yes | TBD (via PAGI middleware) |
| **Presence tracking** | **No** (needs django-channels-presence) | MAY implement |
| **Pattern subscriptions** | **No** | MAY implement |
| **Message history** | **No** | MAY implement |
| **Delayed messages** | **No** | Not in design |

### Phoenix Channels Features (Elixir)

| Feature | Phoenix | PAGI Design |
|---------|---------|-------------|
| Built-in Presence | **Yes** (CRDT-based) | MAY implement |
| Presence diffs (join/leave events) | **Yes** | Not in design |
| Presence metadata | **Yes** | Not in design |
| Distributed presence (multi-node) | **Yes** | Not in design |
| PubSub | Yes | Yes (publish) |
| Millions of connections | Yes | Depends on backend |

## Feature Gap Analysis

### To Match Django Channels
PAGI-Channels design already covers Django's core features. Implementation needed.

### To EXCEED Django Channels
These features would make PAGI-Channels better than Django Channels:

1. **Built-in Presence Tracking** (Phoenix has this, Django doesn't)
2. **Pattern Subscriptions** (`chat.*` matches `chat.room1`, `chat.room2`)
3. **Delayed/Scheduled Messages** (send message after X seconds)
4. **Message History/Replay** (new subscribers get last N messages)

## Design Decisions (from brainstorming)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| v1 Features | Presence + Patterns + Delays + History | Exceed Django, "fail big > win small" |
| Presence API | Hybrid (auto-track via subscribe + explicit track) | Flexible, covers all use cases |
| Pattern syntax | Redis-style `*` (single) and `**` (recursive) | Explicit, matches Redis PSUBSCRIBE |
| Presence events | Both poll AND events | Poll for initial state, events for real-time |
| Backends in v1 | Memory + Redis | Async::Redis ready, Redis makes delays/history trivial |
| Event loop | Future::IO only (loop agnostic) | Works with IO::Async, Mojo, UV, etc. |
| Serialization | JSON default, Sereal optional | Debuggable default, fast opt-in |
| History config | Global default + per-topic override | Simple but flexible |
| Delay cancellation | No cancellation in v1 | Keep simple, add later if needed |
| Testing | Docker for Redis tests | Real Redis in CI/local testing |

### Presence API (decided)

```perl
# Auto-track via subscribe (most common case)
await $channels->subscribe("chat.room1", presence => { user => 'alice', status => 'online' });

# Explicit track for non-subscription presence (workers, services)
await $channels->track("workers.pool", { worker_id => $$, started => time() });
await $channels->untrack("workers.pool");

# Poll presence anytime
my @users = await $channels->list_presence("chat.room1");

# Presence events arrive in receive queue
my $event = await $receive->();
# { type => 'presence.join', topic => 'chat.room1', presence => { user => 'alice', ... } }
# { type => 'presence.leave', topic => 'chat.room1', presence => { user => 'alice', ... } }
```

### Pattern Subscriptions (decided)

```perl
# Single-level: * matches exactly one segment
await $channels->psubscribe("chat.*");
# Matches: chat.room1, chat.general
# NOT: chat.room1.messages

# Multi-level: ** matches zero or more segments
await $channels->psubscribe("notifications.**");
# Matches: notifications, notifications.user, notifications.user.123.email
```

### Delayed Messages (decided)

```perl
# Point-to-point with delay
await $channels->send($channel, { type => 'reminder', ... }, delay => 300);

# Broadcast with delay
await $channels->publish("chat.room1", { type => 'warning', ... }, delay => 60);
```

### Message History (decided)

```perl
# Subscribe with history - receive last N messages immediately
await $channels->subscribe("chat.room1", history => 10);

# Configure history retention per-topic or globally (TBD)
```

## Open Questions

**All questions resolved!** Ready to proceed to planning phase.

## Proposed CPAN Dependencies (needs approval)

### Required
| Module | Why | Risk Reduction |
|--------|-----|----------------|
| Future::AsyncAwait | async/await syntax | Core to design |
| Future::IO | Loop-agnostic I/O | **Critical** for portability |
| Role::Tiny | Backend interface | Lightweight, no deps |
| JSON::MaybeXS | Default serialization | Fast, portable JSON |
| Async::Redis | Redis backend | You built it! Ready to use |

### Optional (for performance)
| Module | Why | When Needed |
|--------|-----|-------------|
| Sereal::Encoder + Sereal::Decoder | Fast serialization | High-throughput deployments |

### Test Dependencies
| Module | Why |
|--------|-----|
| Test2::V0 | Modern test framework |
| IO::Async | Event loop for tests |
| Test::Pod | POD validation |

### Installation Note
**Use latest Async::Redis from CPAN** - `cpanm Async::Redis` (not local dev version)

## Example Applications (v1 deliverables)

### Example 1: Presence-Enabled Chat with Workers
Based on PAGI WebSocket chat example, enhanced with:
- Channel layer for cross-worker messaging
- Presence tracking (who's online)
- Custom events for worker communication
- Multiple worker processes sharing connections

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Worker 1   │     │  Worker 2   │     │  Worker 3   │
│  (WS conns) │     │  (WS conns) │     │  (WS conns) │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                    ┌──────┴──────┐
                    │    Redis    │
                    │  (channels) │
                    └─────────────┘
```

### Example 2: Task Queue with Progress Updates
Real-time task processing with:
- HTTP endpoint submits tasks
- Worker pool processes tasks
- WebSocket clients receive progress updates
- Demonstrates request/reply pattern + broadcast

```perl
# HTTP: Submit task
POST /tasks → { task_id => 123 }

# Worker: Process + broadcast progress
await $channels->publish("task.123.progress", { percent => 50 });

# WebSocket: Client subscribes to updates
await $channels->subscribe("task.123.**");
```

## Testing Infrastructure

- **Docker Compose** for Redis (same pattern as Async::Redis)
- Tests run with `REDIS_HOST=localhost prove -l t/`
- CI-friendly: skip Redis tests if not available

## Sources

- [Django Channels Documentation](https://channels.readthedocs.io/)
- [Django Channels Channel Layers](https://channels.readthedocs.io/en/stable/topics/channel_layers.html)
- [django-channels-presence](https://django-channels-presence.readthedocs.io/en/latest/)
- [Phoenix.Presence Documentation](https://hexdocs.pm/phoenix/Phoenix.Presence.html)
- [Phoenix Presence Guide](https://hexdocs.pm/phoenix/presence.html)

## Scratch Space

(Notes during brainstorming)
