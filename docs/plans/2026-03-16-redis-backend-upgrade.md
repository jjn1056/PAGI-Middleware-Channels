# Redis Backend: Accept Async::Redis Instance

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor the Redis backend to accept a pre-configured `Async::Redis` instance instead of creating its own connection, leveraging Async::Redis's built-in reconnect, prefix, fork safety, and connection pooling.

**Architecture:** The constructor gains a `redis` option that accepts an `Async::Redis` instance. When provided, URI parsing, connection management, and manual prefix handling are bypassed — Async::Redis handles all of that. When `redis` is not provided, the backend creates its own instance from `uri` (backward compatible). Key prefix management moves to Async::Redis's built-in `prefix` option. The `_ensure_connected` pattern is replaced by Async::Redis's lazy connect + auto-reconnect.

**Tech Stack:** Perl 5.18+, Future::AsyncAwait, Async::Redis 0.001005+, Test2::V0

**Perlbrew:** `source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default`

**Test setup:** `docker compose -f t/docker-compose.yml up -d` (starts Redis on localhost:6379)

**Test command:** `prove -lr t/30-redis/ -v`

**Verification gates (REQUIRED at end of every task):**
1. Run: `prove -lr t/ -v` — ALL must pass (memory tests + redis tests if Docker running)
2. Dead code check: verify no unused methods, stale imports, orphaned helpers
3. Doc/code consistency: verify POD matches actual constructor options and behavior
4. Fix ALL issues before committing

---

### Task 1: Accept Async::Redis instance in constructor

**Files:**
- Modify: `lib/PAGI/Channels/Backend/Redis.pm:24-42` (constructor)
- Modify: `t/30-redis/01-core.t` (add test for new constructor pattern)

**Context:** Currently the constructor takes a `uri` string, parses it manually, and creates `Async::Redis->new(host => ..., port => ...)` in `connect()`. We add a `redis` option that accepts a pre-configured Async::Redis instance, skipping URI parsing and manual connection management.

**Step 1: Write the failing test**

Add to `t/30-redis/01-core.t`, after the existing 'connect to Redis' subtest:

```perl
subtest 'accept Async::Redis instance' => sub {
    require Async::Redis;
    my $redis = Async::Redis->new(
        host      => redis_host(),
        port      => redis_port(),
        reconnect => 1,
        prefix    => 'test:instance:',
    );
    run { $redis->connect };

    my $backend = PAGI::Channels::Backend::Redis->new(
        redis => $redis,
    );

    ok($backend->connected, 'connected via passed instance');

    # Verify it works
    run { $backend->send('ch1', { type => 'test', val => 42 }) };
    my $msg = run { $backend->poll('ch1') };
    is($msg->{val}, 42, 'send/poll works with passed instance');

    # Cleanup
    run { $backend->flush() };
    $redis->disconnect;
};
```

**Step 2: Run test to verify it fails**

Run: `prove -lr t/30-redis/01-core.t -v`
Expected: FAIL — constructor doesn't accept `redis` option

**Step 3: Update the constructor**

In `lib/PAGI/Channels/Backend/Redis.pm`, replace the constructor (lines 24-42):

```perl
sub new {
    my ($class, %args) = @_;

    my $self = bless {
        prefix       => $args{prefix}       // DEFAULT_PREFIX,
        capacity     => $args{capacity}     // DEFAULT_CAPACITY,
        expiry       => $args{expiry}       // DEFAULT_EXPIRY,
        group_expiry => $args{group_expiry} // DEFAULT_GROUP_EXPIRY,
        max_size     => $args{max_size}     // DEFAULT_MAX_SIZE,
        history_size => $args{history_size} // DEFAULT_HISTORY_SIZE,
        _channel_id  => undef,
    }, $class;

    if ($args{redis}) {
        # Use provided Async::Redis instance
        $self->{_redis} = $args{redis};
        $self->{_connected} = $args{redis}->is_connected ? 1 : 0;
        $self->{_external_redis} = 1;
    }
    else {
        # Create our own connection from URI (backward compatible)
        $self->{uri} = $args{uri} // 'redis://localhost:6379';
        $self->{_redis} = undef;
        $self->{_connected} = 0;
        $self->{_external_redis} = 0;
    }

    return $self;
}
```

**Step 4: Update `connect()` to skip when external Redis provided**

```perl
async sub connect {
    my ($self) = @_;

    return 1 if $self->{_external_redis} && $self->{_redis};

    require Async::Redis;

    my ($host, $port) = $self->_parse_uri($self->{uri});

    $self->{_redis} = Async::Redis->new(
        host      => $host,
        port      => $port,
        reconnect => 1,
    );

    await $self->{_redis}->connect();
    $self->{_connected} = 1;

    return 1;
}
```

**Step 5: Update `_ensure_connected` to handle external Redis**

```perl
async sub _ensure_connected {
    my ($self) = @_;
    return if $self->{_connected};
    if ($self->{_external_redis}) {
        await $self->{_redis}->connect() unless $self->{_redis}->is_connected;
        $self->{_connected} = 1;
    }
    else {
        await $self->connect();
    }
}
```

**Step 6: Update `disconnect()` to not disconnect external Redis**

```perl
async sub disconnect {
    my ($self) = @_;

    if ($self->{_redis} && !$self->{_external_redis}) {
        $self->{_redis}->disconnect();
        $self->{_redis} = undef;
    }
    $self->{_connected} = 0;

    return 1;
}
```

**Step 7: Run tests**

Run: `prove -lr t/30-redis/ -v`
Expected: All pass including new subtest

**Step 8: Run full suite, dead code check, doc check**

Run: `prove -lr t/ -v`
Verify: `_parse_uri` still used (for URI mode). No dead code. POD not yet updated (Task 3).

**Step 9: Commit**

```bash
git add lib/PAGI/Channels/Backend/Redis.pm t/30-redis/01-core.t
git commit -m "feat: accept Async::Redis instance in Redis backend constructor

When a pre-configured Async::Redis instance is passed via the 'redis'
option, the backend uses it directly instead of creating its own
connection. This enables reconnect, prefix, fork safety, and
connection pooling via Async::Redis's built-in features.

Backward compatible: URI-based construction still works."
```

---

### Task 2: Delegate key prefixing to Async::Redis

**Files:**
- Modify: `lib/PAGI/Channels/Backend/Redis.pm` (key helpers, constructor)
- Modify: `t/30-redis/01-core.t` (add prefix test)

**Context:** When an external Async::Redis instance is provided with its own `prefix`, the backend's `_queue_key`, `_group_key` etc. should NOT add a second prefix. Async::Redis's `KeyExtractor` already prefixes all key operations. But there's a subtlety: the backend uses custom key naming (`q:channel`, `g:topic`) that Async::Redis's `prefix` doesn't know about — it just prepends a string. So our key helpers still add `q:`, `g:`, etc. but skip the `pagi:` part when using external Redis with its own prefix.

**Step 1: Write the failing test**

Add to `t/30-redis/01-core.t`:

```perl
subtest 'external Redis prefix not doubled' => sub {
    require Async::Redis;
    my $redis = Async::Redis->new(
        host      => redis_host(),
        port      => redis_port(),
        prefix    => 'myapp:channels:',
    );
    run { $redis->connect };

    my $backend = PAGI::Channels::Backend::Redis->new(
        redis => $redis,
    );

    run { $backend->send('ch1', { type => 'test' }) };

    # The key in Redis should be "myapp:channels:q:ch1", NOT "myapp:channels:pagi:q:ch1"
    # Verify by polling — if prefix is doubled, poll won't find the message
    my $msg = run { $backend->poll('ch1') };
    ok($msg, 'message found (prefix not doubled)');

    run { $backend->flush() };
    $redis->disconnect;
};
```

**Step 2: Update constructor to skip backend prefix when external Redis has its own**

When an external Async::Redis is provided, set `$self->{prefix}` to `''` (empty string) — let Async::Redis handle the top-level prefix. The key helpers still add `q:`, `g:`, `p:`, etc. as structure markers.

In the constructor's `if ($args{redis})` block, add:

```perl
# When using external Redis with its own prefix,
# don't add our default prefix on top
$self->{prefix} = $args{prefix} // '';
```

**Step 3: Run tests, verify, commit**

Run: `prove -lr t/30-redis/ -v`

```bash
git add lib/PAGI/Channels/Backend/Redis.pm t/30-redis/01-core.t
git commit -m "fix: avoid double-prefixing when external Async::Redis has prefix

When an Async::Redis instance with its own prefix is provided,
the backend prefix defaults to empty string. The structural
markers (q:, g:, p:, etc.) are still added by key helpers."
```

---

### Task 3: Enable reconnect for URI-based construction

**Files:**
- Modify: `lib/PAGI/Channels/Backend/Redis.pm:44-61` (`connect` method)
- Modify: `t/30-redis/01-core.t` (add reconnect test)

**Context:** When the backend creates its own Async::Redis from URI, it should enable `reconnect => 1` by default. Currently it creates a bare `Async::Redis->new(host, port)` with no reconnect. Also use Async::Redis's URI parsing instead of our manual `_parse_uri`.

**Step 1: Update `connect()` to use URI and enable reconnect**

Replace the `connect()` method:

```perl
async sub connect {
    my ($self) = @_;

    return 1 if $self->{_external_redis} && $self->{_redis};

    require Async::Redis;

    $self->{_redis} = Async::Redis->new(
        uri       => $self->{uri},
        reconnect => 1,
        prefix    => $self->{prefix},
    );

    await $self->{_redis}->connect();
    $self->{_connected} = 1;

    return 1;
}
```

**Step 2: Remove `_parse_uri` method (now dead code)**

Delete the `_parse_uri` method entirely (lines 63-73). Async::Redis handles URI parsing via `Async::Redis::URI`.

**Step 3: When using URI mode, don't double-prefix**

Since we're now passing `prefix` to Async::Redis, the key helpers should use empty prefix to avoid doubling. Update the constructor's else branch:

```perl
else {
    $self->{uri} = $args{uri} // 'redis://localhost:6379';
    $self->{_redis} = undef;
    $self->{_connected} = 0;
    $self->{_external_redis} = 0;
    # prefix will be passed to Async::Redis in connect()
    # key helpers use empty prefix to avoid doubling
}
```

Wait — this changes the key format. Currently keys are `pagi:q:channel`. With this change, Async::Redis prefixes `pagi:` and key helpers add `q:channel`, giving `pagi:q:channel` — same result. But we need to make sure the key helpers DON'T add `pagi:` again. The simplest fix: always set `$self->{prefix}` to the structural-only prefix (empty or user-provided), and let Async::Redis handle the top-level namespace.

Actually, the cleanest approach: store the full prefix for key helpers, but when creating our own Async::Redis, DON'T pass prefix to it (let our key helpers handle it). Only when using external Redis do we let Async::Redis handle prefixing.

Let me simplify. Keep the current key helper behavior as-is:

```perl
sub _queue_key    { shift->{prefix} . 'q:' . shift }
```

For URI mode: `$self->{prefix}` = `'pagi:'` (default). Don't pass `prefix` to Async::Redis. Key helpers produce `pagi:q:channel`. Works as before.

For external mode: `$self->{prefix}` = `''`. Async::Redis has its own prefix. Key helpers produce `q:channel`. Async::Redis prepends `myapp:channels:`. Final key: `myapp:channels:q:channel`. Correct.

So `connect()` should NOT pass `prefix` to Async::Redis:

```perl
async sub connect {
    my ($self) = @_;
    return 1 if $self->{_external_redis} && $self->{_redis};

    require Async::Redis;

    $self->{_redis} = Async::Redis->new(
        uri       => $self->{uri},
        reconnect => 1,
    );

    await $self->{_redis}->connect();
    $self->{_connected} = 1;
    return 1;
}
```

**Step 4: Remove `_parse_uri` (dead code)**

**Step 5: Run tests, verify no regressions, commit**

Run: `prove -lr t/ -v`
Verify: `_parse_uri` is gone. `connect()` uses Async::Redis URI parsing.

```bash
git add lib/PAGI/Channels/Backend/Redis.pm
git commit -m "refactor: use Async::Redis URI parsing, enable reconnect by default

Removes manual _parse_uri in favor of Async::Redis's built-in URI
support. Enables reconnect => 1 for resilience against Redis restarts."
```

---

### Task 4: Update POD documentation

**Files:**
- Modify: `lib/PAGI/Channels/Backend/Redis.pm` (POD section)

**Step 1: Replace the CONSTRUCTOR OPTIONS POD**

Update to document both construction patterns:

```pod
=head1 SYNOPSIS

    # Option 1: URI-based (creates its own connection)
    my $backend = PAGI::Channels::Backend::Redis->new(
        uri      => 'redis://localhost:6379',
        prefix   => 'myapp:',
        capacity => 100,
    );

    # Option 2: Pass a pre-configured Async::Redis instance
    use Async::Redis;
    my $redis = Async::Redis->new(
        uri       => 'redis://localhost:6379',
        reconnect => 1,
        prefix    => 'myapp:channels:',
    );
    await $redis->connect;

    my $backend = PAGI::Channels::Backend::Redis->new(
        redis => $redis,
    );

=head1 CONSTRUCTOR OPTIONS

=over 4

=item redis => $async_redis_instance

An L<Async::Redis> instance. When provided, the backend uses this
connection directly instead of creating its own. This enables
Async::Redis features like reconnect, fork safety, and connection
pooling. The backend will not disconnect this instance — the caller
manages its lifecycle.

=item uri => $redis_uri

Redis connection URI (e.g., C<redis://localhost:6379>). Used only when
C<redis> is not provided. Default: C<redis://localhost:6379>.

=item prefix => $string

Key prefix for all Redis keys. Default: C<pagi:>. When using an
external C<redis> instance that has its own prefix, this defaults
to empty string to avoid double-prefixing.

=item capacity => $int

Maximum messages per channel queue. Default: 100.

=item expiry => $seconds

Message TTL in seconds. Default: 60.

=item group_expiry => $seconds

Subscription group membership TTL. Default: 86400 (1 day).

=item history_size => $int

Messages retained for history. Default: 0 (disabled).

=back
```

**Step 2: Run full test suite**

Run: `prove -lr t/ -v`

**Step 3: Commit**

```bash
git add lib/PAGI/Channels/Backend/Redis.pm
git commit -m "docs: update Redis backend POD for external Async::Redis instance"
```

---

### Task 5: Update Channels facade to pass redis option through

**Files:**
- Modify: `lib/PAGI/Channels.pm:28-42` (`_init_backend` method)
- Modify: `t/20-facade/01-basic.t` (add test)

**Context:** The Channels facade currently only passes `uri` to the Redis backend. It should also support passing a `redis` instance for the external Async::Redis pattern.

**Step 1: Write the failing test**

Add to `t/20-facade/01-basic.t`:

```perl
subtest 'facade accepts redis option' => sub {
    SKIP: {
        skip_without_redis();
        require Async::Redis;
        my $redis = Async::Redis->new(
            host => redis_host(),
            port => redis_port(),
        );
        run { $redis->connect };

        my $channels = PAGI::Channels->new(redis => $redis);
        ok($channels->backend->connected, 'backend connected via facade redis option');

        $redis->disconnect;
    }
};
```

Note: `t/20-facade/01-basic.t` may need to import `skip_without_redis`, `redis_host`, `redis_port` from the test helper.

**Step 2: Update `_init_backend` and `new`**

In `lib/PAGI/Channels.pm`, update `new()`:

```perl
sub new {
    my ($class, %args) = @_;

    my $self = bless {
        _backend => undef,
        _counter => 0,
    }, $class;

    if ($args{redis}) {
        # External Async::Redis instance
        require PAGI::Channels::Backend::Redis;
        $self->{_backend} = PAGI::Channels::Backend::Redis->new(
            redis    => $args{redis},
            capacity => $args{capacity},
            expiry   => $args{expiry},
            (defined $args{prefix} ? (prefix => $args{prefix}) : ()),
        );
    }
    else {
        my $backend_uri = $args{backend}
            // $ENV{PAGI_CHANNELS_BACKEND}
            // 'memory://';
        $self->_init_backend($backend_uri, %args);
    }

    return $self;
}
```

And update `_init_backend` to pass through options:

```perl
sub _init_backend {
    my ($self, $uri, %args) = @_;

    if ($uri =~ /^memory:/) {
        require PAGI::Channels::Backend::Memory;
        $self->{_backend} = PAGI::Channels::Backend::Memory->new(
            capacity => $args{capacity},
            expiry   => $args{expiry},
        );
    }
    elsif ($uri =~ /^redis:/) {
        require PAGI::Channels::Backend::Redis;
        $self->{_backend} = PAGI::Channels::Backend::Redis->new(
            uri      => $uri,
            capacity => $args{capacity},
            expiry   => $args{expiry},
            (defined $args{prefix} ? (prefix => $args{prefix}) : ()),
        );
    }
    else {
        die "Unknown backend: $uri";
    }
}
```

**Step 3: Remove `backend_uri` from $self** (no longer stored — dead state)

**Step 4: Run tests, dead code check, commit**

Run: `prove -lr t/ -v`
Verify: `backend_uri` removed from $self. `_init_backend` signature updated.

```bash
git add lib/PAGI/Channels.pm t/20-facade/01-basic.t
git commit -m "feat: facade accepts redis option for external Async::Redis instance

Channels->new(redis => $redis) passes the instance through to the
Redis backend. Also passes capacity/expiry/prefix options through
to both Memory and Redis backends from the facade."
```

---

### Task 6: Final verification and cleanup

**Files:** None (verification only)

**Step 1: Start Docker Redis**

```bash
docker compose -f t/docker-compose.yml up -d
```

**Step 2: Run full test suite including Redis tests**

```bash
source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && \
  prove -lr t/ -v
```
Expected: ALL tests pass (memory + facade + redis).

**Step 3: Dead code audit**

Check all modified files for:
- Unused imports (especially `Async::Redis` if only `require`d)
- Orphaned methods (e.g., `_parse_uri` should be gone)
- Stale comments referencing old URI-based connection flow

**Step 4: Doc/code consistency**

Verify POD in Redis.pm and Channels.pm matches actual behavior:
- `redis` option documented
- `uri` option documented as fallback
- `prefix` behavior documented for both modes
- No references to `_parse_uri` or manual host/port parsing

**Step 5: Stop Docker Redis**

```bash
docker compose -f t/docker-compose.yml down
```

**Step 6: Commit if any fixes needed**

---

## Summary

| Task | What | Lines Changed (est) |
|------|------|---------------------|
| 1 | Accept Async::Redis instance in constructor | ~40 |
| 2 | Delegate prefix handling for external Redis | ~10 |
| 3 | Use Async::Redis URI parsing + reconnect, remove _parse_uri | ~15 |
| 4 | Update POD documentation | ~40 |
| 5 | Update facade to pass redis option through | ~30 |
| 6 | Final verification and cleanup | 0 |

Total: ~135 lines changed across 4 files, 6 tasks.
