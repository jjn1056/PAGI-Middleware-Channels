# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Perl Environment

**IMPORTANT:** Always use perlbrew before running Perl commands:

```bash
source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default
```

This ensures you have the correct Perl with all dependencies (Future::AsyncAwait, etc.).

## Build & Test Commands

```bash
# Start Redis for tests
cd t && docker compose up -d && cd ..

# Run all tests
REDIS_HOST=localhost prove -lr t/

# Run memory backend tests (no Redis needed)
prove -l t/10-memory/

# Run Redis backend tests
REDIS_HOST=localhost prove -l t/30-redis/

# Run facade tests
prove -l t/20-facade/
```

## Architecture

**CRITICAL: Loop Agnosticism**
- `lib/` - Future::IO ONLY, no IO::Async/Mojo/etc.
- `t/` - Uses IO::Async via Future::IO backend
- `examples/` - Can use any loop (examples show IO::Async)

**Core Modules:**
- `PAGI::Middleware::Channels` - Middleware factory with `wrap()`
- `PAGI::Channel` - Per-connection handle; get it with `PAGI::Channel->from_scope($scope)`
- `PAGI::Channels` - Compatibility shim for `PAGI::Channel` (do not use in new code)
- `PAGI::Middleware::Channels::Backend` - Role defining backend interface
- `PAGI::Middleware::Channels::Backend::Memory` - In-memory (single process)
- `PAGI::Middleware::Channels::Backend::Redis` - Redis (multi-process); takes a caller-owned `Async::Redis` instance

**Advanced Features (v1):**
- Presence tracking (subscribe with presence option)
- Pattern subscriptions (psubscribe with * and **)
- Delayed messages (delay option on send/publish)
- Message history (history option on subscribe)

## Dependencies

Core: Future::AsyncAwait, Future::IO, JSON::MaybeXS, PAGI::Middleware
Redis: Async::Redis
Test: Test2::V0, IO::Async

## Testing

Tests are organized by backend:
- `t/00-load.t` - Module loading
- `t/10-memory/` - Memory backend (7 files)
- `t/20-facade/` - Facade and wrap() (2 files)
- `t/30-redis/` - Redis backend (6 files)
