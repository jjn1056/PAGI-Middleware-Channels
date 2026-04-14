# Redis Backend & Facade — Pure-Instance Construction

> **For Claude:** Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Drop URI-string construction from both `PAGI::Channels` and `PAGI::Channels::Backend::Redis`. Both layers accept instances. Code to an interface, not a class.

- `PAGI::Channels::Backend::Redis->new(redis => $async_redis)` — handed a configured `Async::Redis` (or anything that ducks the same interface). Does no connection management, no URI parsing, no top-level prefix handling. Lifecycle, reconnect, prefix, fork-safety, and pooling are the Redis-client's job.
- `PAGI::Channels->new(backend => $backend_instance)` — handed any backend instance. No URI parsing, no backend dispatch.

Magic can come back later as a convenience helper if users ask for it — not in this plan.

**Key packaging consequence:** `Backend::Redis` never `use`s `Async::Redis`; it only calls methods on the passed-in object. The PAGI-Channels distribution therefore has **no Async::Redis dependency** — users bring their own client, and anything with the right method signatures (Async::Redis today, conceivably Net::Async::Redis or Mojo::Redis tomorrow) works. This is what keeps the Redis backend shippable inside the main distribution instead of needing a separate CPAN module.

**Architecture removed:**

- `Backend::Redis`: `connect`, `disconnect`, `_ensure_connected`, `connected`, `_parse_uri`, `uri` constructor option, `prefix` constructor option (top-level prefix delegates to Async::Redis; structural markers `q:` / `g:` / `p:` / `h:` / `pat:` / `delayed` stay).
- `PAGI::Channels`: `_init_backend`, URI dispatch, `PAGI_CHANNELS_BACKEND` env var.

**Tech stack:** Perl 5.18+, Future::AsyncAwait, Async::Redis 0.001005+ (lazy-connect + fork-safety), Test2::V0.

**Perlbrew:** `source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default`

**Test setup:** `docker compose -f t/docker-compose.yml up -d`

**Test command:** `REDIS_HOST=localhost prove -lr t/`

**Verification gates (REQUIRED at end of every task):**

1. `prove -lr t/` — ALL pass (memory + facade + redis with Docker up).
2. Dead-code audit — no unused methods, stale imports, orphan helpers.
3. Doc/code consistency — POD matches actual constructor signatures and behavior.
4. Fix all issues before committing.

---

### Task 1: `Backend::Redis` — accept `redis` instance only

**Files:**

- `lib/PAGI/Channels/Backend/Redis.pm` — rewrite constructor + connection handling.
- `t/30-redis/01-core.t` through `t/30-redis/06-history.t` — update construction.
- `t/lib/Test/PAGI/Channels.pm` — consider adding a `make_redis_backend()` helper to reduce per-test boilerplate.

**Context:** Backend::Redis currently parses a URI, owns an Async::Redis it created, runs `_ensure_connected` in every async method, and prepends `$self->{prefix}` in every key helper. We flip that: the caller passes a ready Async::Redis; Async::Redis owns connection lifecycle and user-facing prefix; the backend only concerns itself with structural keys (`q:`, `g:`, …) and channel semantics.

**Step 1 — write the new construction pattern in tests.**

Add a test helper in `t/lib/Test/PAGI/Channels.pm`:

```perl
sub make_redis {
    require Async::Redis;
    Async::Redis->new(
        uri    => "redis://" . redis_host() . ":" . redis_port(),
        prefix => "test:$$:",          # per-process isolation
    );
}
```

Then update the first subtest in `t/30-redis/01-core.t` to use it:

```perl
subtest 'construct from Async::Redis instance' => sub {
    require PAGI::Channels::Backend::Redis;
    my $backend = PAGI::Channels::Backend::Redis->new(
        redis => make_redis(),
    );
    run { $backend->flush() };

    run { $backend->send('ch1', { type => 'hi', n => 1 }) };
    my $msg = run { $backend->poll('ch1') };
    is($msg->{n}, 1, 'send/poll round trip via passed Async::Redis');

    run { $backend->flush() };
};
```

**Step 2 — verify RED.**

`REDIS_HOST=localhost prove -lv t/30-redis/01-core.t` — expected failure: constructor doesn't understand `redis =>`.

**Step 3 — rewrite `Backend::Redis::new`.**

```perl
sub new {
    my ($class, %args) = @_;

    my $redis = $args{redis}
        or die "PAGI::Channels::Backend::Redis: 'redis' argument required (Async::Redis instance)";

    return bless {
        _redis       => $redis,
        _channel_id  => undef,
        capacity     => $args{capacity}     // DEFAULT_CAPACITY,
        expiry       => $args{expiry}       // DEFAULT_EXPIRY,
        group_expiry => $args{group_expiry} // DEFAULT_GROUP_EXPIRY,
        max_size     => $args{max_size}     // DEFAULT_MAX_SIZE,
        history_size => $args{history_size} // DEFAULT_HISTORY_SIZE,
    }, $class;
}
```

**Step 4 — delete connection management.**

Remove from the module: `connect`, `disconnect`, `connected`, `_ensure_connected`, `_parse_uri`, and every `await $self->_ensure_connected()` call in async methods. Async::Redis 0.001005+ handles lazy-connect and reconnect itself.

**Step 5 — strip top-level prefix.**

Update the key helpers to drop `$self->{prefix}`:

```perl
sub _queue_key    { 'q:'   . $_[1] }
sub _group_key    { 'g:'   . $_[1] }
sub _presence_key { 'p:'   . $_[1] }
sub _history_key  { 'h:'   . $_[1] }
sub _pattern_key  { 'pat:' . $_[1] }
sub _delayed_key  { 'delayed' }
```

Remove `DEFAULT_PREFIX` and the `prefix` constructor option. Async::Redis's own `prefix` handles namespacing; these structural markers are relative to that.

Update `flush`:

```perl
async sub flush {
    my ($self) = @_;
    my $keys_ref = await $self->{_redis}->keys('*');
    my @keys = ref $keys_ref eq 'ARRAY' ? @$keys_ref : ();
    await $self->{_redis}->del(@keys) if @keys;
    return 1;
}
```

(`keys('*')` is scoped by Async::Redis's prefix — if a caller shares the prefix with other code, that's their problem. Document this on the `flush` POD.)

**Step 6 — migrate the other Redis test files.**

For each of `02-pubsub.t`, `03-patterns.t`, `04-presence.t`, `05-delayed.t`, `06-history.t`: replace the URI-based `$make_backend` closure with one that calls `make_redis()` and passes the instance. Drop `$backend->connect()` and `$backend->disconnect()` calls — no-ops now.

**Step 7 — verify GREEN.**

`REDIS_HOST=localhost prove -lr t/30-redis/ -v` — all must pass.

**Step 8 — full-suite check and dead-code audit.**

`prove -lr t/` — all pass. Confirm `_parse_uri`, `connect`, `disconnect`, `_ensure_connected`, `connected`, `DEFAULT_PREFIX` are gone. Confirm no `$self->{prefix}` references remain in `Backend/Redis.pm`.

**Step 9 — commit.**

```
refactor(redis): accept Async::Redis instance; drop URI and connection management

Backend::Redis now requires a configured Async::Redis via `redis =>`.
Connection lifecycle, reconnect, fork-safety, and top-level prefix are
delegated to Async::Redis. The backend owns only structural keys
(q:, g:, p:, h:, pat:, delayed). connect, disconnect, _ensure_connected,
connected, and _parse_uri are gone.
```

---

### Task 2: `PAGI::Channels` — accept `backend` instance only

**Files:**

- `lib/PAGI/Channels.pm` — rewrite `new`, drop `_init_backend`.
- `t/20-facade/01-basic.t`, `t/20-facade/02-wrap.t` — update construction.

**Context:** The facade currently parses a URI string to pick a backend. We drop the parse and require a backend instance.

**Step 1 — update the first facade test.**

In `t/20-facade/01-basic.t`, replace any `PAGI::Channels->new(backend => 'memory://')` with:

```perl
my $channels = PAGI::Channels->new(
    backend => PAGI::Channels::Backend::Memory->new,
);
```

**Step 2 — verify RED.** Test fails because facade still runs URI parsing and doesn't know what to do with an object.

**Step 3 — rewrite `PAGI::Channels::new`.**

```perl
sub new {
    my ($class, %args) = @_;

    my $backend = $args{backend}
        or die "PAGI::Channels: 'backend' argument required (PAGI::Channels::Backend instance)";

    return bless {
        _backend => $backend,
        _counter => 0,
    }, $class;
}
```

Delete `_init_backend` entirely. Remove the `PAGI_CHANNELS_BACKEND` env-var logic and any mention in the file.

**Step 4 — migrate the other facade test.** Same pattern for `02-wrap.t`.

**Step 5 — verify GREEN.** `prove -lr t/10-memory/ t/20-facade/ -v` — all pass.

**Step 6 — full suite.** `REDIS_HOST=localhost prove -lr t/` — all pass.

**Step 7 — commit.**

```
refactor(facade): accept Backend instance; drop URI dispatch and env var

PAGI::Channels now requires a backend instance. No more URI parsing,
no more PAGI_CHANNELS_BACKEND env var, no more _init_backend.
```

---

### Task 3: update the chat example

**Files:** `examples/chat/app.pl`

**Context:** `examples/chat/app.pl` currently does:

```perl
my $channels = PAGI::Channels->new(
    backend => $ENV{PAGI_CHANNELS_BACKEND} // 'redis://localhost:6379',
);
```

Replace with explicit instance construction:

```perl
use Async::Redis;
use PAGI::Channels::Backend::Redis;

my $redis = Async::Redis->new(
    uri       => $ENV{PAGI_REDIS_URI} // 'redis://localhost:6379',
    prefix    => 'chat:',
    reconnect => 1,
);

my $channels = PAGI::Channels->new(
    backend => PAGI::Channels::Backend::Redis->new(redis => $redis),
);
```

Update `examples/chat/README.md`: the run command no longer uses `PAGI_CHANNELS_BACKEND`; show `PAGI_REDIS_URI` if we keep env configurability.

**Step 1 — manual verification.** Start Redis (Docker). Run `pagi-server --workers 4 app.pl`. Open two browser tabs at `http://localhost:5000`, join a room, send messages. Verify cross-worker delivery and presence still work.

**Step 2 — commit.**

```
refactor(example): chat app constructs Async::Redis + Backend::Redis explicitly
```

---

### Task 4: update POD

**Files:** `lib/PAGI/Channels.pm`, `lib/PAGI/Channels/Backend/Redis.pm`, `lib/PAGI/Channels/Backend/Memory.pm` (spot check), `lib/PAGI/Channels/Backend.pm` (spot check), `README.md`.

**Step 1 — `PAGI::Channels` POD:**

- Replace the SYNOPSIS `backend => 'redis://...'` with the full form (Async::Redis + Backend::Redis).
- Remove the `PAGI_CHANNELS_BACKEND` section entirely.
- Remove the `Memory (memory://)` / `Redis (redis://host:port)` headings in BACKENDS; replace with paragraphs describing `PAGI::Channels::Backend::Memory` and `PAGI::Channels::Backend::Redis` as instance types.

**Step 2 — `Backend::Redis` POD:**

- SYNOPSIS shows `Async::Redis->new(...)` then `Backend::Redis->new(redis => $redis)`.
- CONSTRUCTOR OPTIONS: remove `uri` and `prefix`. Add `redis` (required). Keep `capacity`, `expiry`, `group_expiry`, `history_size`, `max_size`.
- Document that the caller owns the Async::Redis lifecycle and that `flush()` wipes everything under the instance's prefix.

**Step 3 — `Backend::Memory` POD:** verify the SYNOPSIS doesn't mention a URI. Should already be instance-based; confirm and move on.

**Step 4 — `README.md`:** update the Quick Start to show the full instance form for Redis.

**Step 5 — commit.**

```
docs: update POD and README for instance-based construction
```

---

### Task 5: `cpanfile` cleanup

**Files:** `cpanfile`.

The distribution no longer depends on `Async::Redis` — `Backend::Redis` never `use`s it. Move the Async::Redis dep out of top-level `recommends` and into the test section (tests construct an instance to pass in; runtime users do the same themselves).

**Step 1 — edit `cpanfile`:**

- Remove `recommends 'Async::Redis', '0.001003'` and its "Latest from CPAN" comment from the top-level block.
- Inside `on 'test' => sub { ... }`, add `recommends 'Async::Redis', '0.001005'` (0.001005+ for lazy-connect + fork-safety in the tests). Tests already skip when Redis isn't reachable; add a guard skipping when `Async::Redis` isn't loadable.

**Step 2 — commit.**

```
deps(cpanfile): drop Async::Redis from distribution deps

Backend::Redis no longer uses Async::Redis — it calls methods on a
passed-in object. Users bring their own client. Keep Async::Redis as
a test-time recommend at >= 0.001005 for the Redis-backend test
files to run.
```

---

### Task 6: final verification and cleanup

**Files:** none (verification only).

1. `docker compose -f t/docker-compose.yml up -d`
2. `REDIS_HOST=localhost prove -lr t/ -v` — ALL pass.
3. `perl -Ilib -c examples/chat/app.pl` — clean.
4. Grep audit: `grep -r 'PAGI_CHANNELS_BACKEND\|_parse_uri\|_ensure_connected\|_init_backend' lib/ t/ examples/` returns nothing.
5. Zero-dep check: `grep -rE 'use Async::Redis|require Async::Redis' lib/` returns nothing — proves the distribution has no Async::Redis dep.
5. `docker compose -f t/docker-compose.yml down`.
6. If anything needs a fix, commit it; otherwise done.

---

## Summary

| Task | Scope | Est lines changed |
|------|-------|-------------------|
| 1 | `Backend::Redis` pure-instance | ~80 in module, ~30 across 6 test files |
| 2 | Facade pure-instance | ~20 in module, ~10 across 2 test files |
| 3 | Chat example | ~10 |
| 4 | POD + README | ~80 |
| 5 | `cpanfile` (+ `dist.ini` if wired) | ~3 |
| 6 | Verification | 0 |

Total: ~230 lines across ~15 files, 5 commits.
