# Backend Capability Roles — Design and Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement Phase 1, 2, and 3 task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. The full design context is in the sections above the "Implementation Plan" header — read it before starting any task.

**Goal:** Replace the monolithic 18-method `PAGI::Middleware::Channels::Backend` abstract class with a small required core (8 methods) plus optional capability roles (`Presence`, `History`, `Delayed`, `PatternSubs`) declared via `Role::Tiny`. Move shared logic (validation, defaults, regex utilities, presence-event construction, exclude normalization) into a populated base class. Add a parameterized compliance test suite that runs against any backend factory. Fix the latent `set_channel_id` race as a side benefit.

**Non-goals:**
- No public `PAGI::Channel` API breakage. The user-facing facade keeps every method it has today.
- No rewrite of the middleware's receive/wrap loop. That work landed in 2026-04-15-event-driven-receive.
- No structured exception classes. Capability misuse `croak`s with a clear string message; structured exceptions can come later.
- No new backends in this work. PostgreSQL ships separately, using the implementer's guide produced here.

**Tech stack:** Perl 5.40, Future::AsyncAwait, `Role::Tiny` (new dependency), `Role::Tiny::With`, Test2::V0.

---

## Background — Why This Design

The current `PAGI::Middleware::Channels::Backend` is a 35-line abstract class whose only behavior is "every method croaks." Subclasses (`Memory`, `Redis`) implement 18 methods each, totaling ~600 + ~750 lines.

Three concrete problems with the status quo:

1. **The contract is too big.** Every peer library has a 4–8 method backend contract. Django Channels' `BaseChannelLayer` is 7 abstract methods. Phoenix.PubSub's adapter is 4. ActionCable's is 4. Centrifuge splits its broker into Broker (8) + PresenceManager (4) + history-on-Broker. PAGI's 18-method monolith is the outlier, and the inflation comes from features (`presence`, `history`, `delayed`, `pattern subs`) that no peer library puts in the backend contract.

2. **Cross-cutting code is duplicated.** `_pattern_to_regex` is verbatim identical in Memory and Redis. So is `set_channel_id`/`channel_id`, the `DEFAULT_*` constants block, the constructor's defaults assignment, the `exclude` normalization in `publish`, the presence-event hashref shape, and the `subscribe_with_history` composition. Django Channels avoids this by putting validation and capacity helpers in `BaseChannelLayer`.

3. **The empty base class hides a contract-violation bug.** `Memory` enforces `_validate_channel`/`_validate_message`; `Redis` does not. Same nominal contract, different actual behavior. A populated base class with shared validation makes this divergence impossible.

A fourth, weaker reason: **the test suite duplicates the contract per backend.** `t/10-memory/*.t` and `t/30-redis/*.t` are line-for-line parallel files with the same subtest names. A single compliance suite parameterized by a backend factory halves the test code, catches drift bugs automatically, and makes adding PostgreSQL/NATS test wiring cheap.

The decisive consideration: **PostgreSQL is the next backend** and a SaaS option (likely NATS or Ably) is on the longer-term roadmap. Some of those backends *cannot* honestly implement every method in the current contract — NATS has no presence primitive, AWS SNS+SQS has no presence and only retention-based history, PostgreSQL `LISTEN/NOTIFY` has an 8000-byte payload limit and no built-in history. Forcing every backend to fake every feature is the wrong shape; capability-splitting via roles lets each backend declare honestly what it supports.

---

## Architectural Shape

```
                 PAGI::Channel  (facade)
                       |
                       | composes options:
                       |   delay   -> Delayed capability
                       |   history -> History capability
                       |   presence-> Presence capability
                       |
                       v
   PAGI::Middleware::Channels::Backend  (abstract base class)
   - constructor + common config (capacity, expiry, ...)
   - shared validation methods
   - shared utilities (_pattern_to_regex, _deliver default, exclude norm)
   - declares the CORE 8 methods as abstract (croak if not overridden)
                       ^
                       | use parent 'Backend';
                       |
   +-------------------+-------------------+
   |                                       |
Backend::Memory                       Backend::Redis        Backend::PostgreSQL (future)
                       +
                       | with 'Backend::Role::Presence',
                       |      'Backend::Role::History',
                       |      'Backend::Role::Delayed',
                       |      'Backend::Role::PatternSubs';
```

**Two namespaces, two purposes:**

- `PAGI::Middleware::Channels::Backend` is the **abstract base class**. Every backend `use parent` from it. It owns the constructor, common configuration defaults, validation, shared utility functions, and the 8 abstract core methods that croak when not overridden. It is *not* a role.
- `PAGI::Middleware::Channels::Backend::Role::*` are **`Role::Tiny` capability roles**. A backend declares which optional capabilities it supports by `with`-ing the relevant roles. Each role specifies the methods it requires the backend to implement, and may provide shared helper methods. Capability presence is checked at the facade layer with `$backend->does('PAGI::Middleware::Channels::Backend::Role::Presence')`.

Why split parent class vs roles this way: the core 8 methods are mandatory for every backend — modeling them as `requires` in a role is fine, but the existing abstract-class-with-croaking-methods pattern is already in the codebase and works well for that purpose. Capability methods are *optional and discoverable*, which is what `Role::Tiny`'s `does` gives us cleanly. Using both tools where each is most natural keeps the code honest.

---

## Capability Matrix

This table is the user-facing answer to "if I want X, which backend do I need?" It also tells future implementers what's expected of a new backend.

| Backend                    | Core | Presence | History | Delayed | PatternSubs |
|----------------------------|:----:|:--------:|:-------:|:-------:|:-----------:|
| **Memory**                 |  ✓   |    ✓     |    ✓    |    ✓    |      ✓      |
| **Redis**                  |  ✓   |    ✓     |    ✓    |    ✓    |      ✓      |
| **PostgreSQL** (planned)   |  ✓   |    ✓     |    ✓    |    ✓    |      ✓      |
| **NATS** (hypothetical)    |  ✓   | aux only | JetStr. |    ✗    |      ✓      |
| **AWS SNS+SQS** (hypoth.)  |  ✓   |    ✗     | ret.only|    ✗    |      ✗      |
| **Ably** (hypothetical)    |  ✓   |    ✓     |    ✓    |    ✗    |      ✓      |

Notes on the planned/hypothetical entries:

- **PostgreSQL** uses `LISTEN/NOTIFY` for the pub/sub plumbing and an auxiliary `pagi_channels_messages` table for queues, with `pagi_channels_presence`, `pagi_channels_history`, `pagi_channels_delayed` for the optional capabilities. All four capabilities are achievable; they just require explicit table support. Pattern subscriptions need either a separate registry table (the natural design) or per-pattern `LISTEN`s (less elegant). See the implementer's guide.
- **NATS** has native pub/sub and pattern subscriptions (subject wildcards `*` and `>`). JetStream provides durable streams that map to History. Presence has no native primitive — would require an auxiliary KV (NATS KV is fine for this) or skipping the capability entirely. No native delayed delivery.
- **AWS SNS+SQS** is mostly transport. SNS topics fan out to SQS queues. Message retention exists but is not the same as random-access history. No presence. No delayed delivery (SQS has visibility timeouts but they're a different concept).
- **Ably** is the closest semantic match in the SaaS world — channels, presence, history, and message TTLs are first-class. The mapping is nearly 1:1, and Ably users get those features without auxiliary state. The audience is smaller than NATS but the implementation is conceptually the cleanest.

A backend MUST implement Core. Anything else is optional.

---

## Core Backend Contract

Required for every backend. Defined as abstract methods on `PAGI::Middleware::Channels::Backend` that croak by default. Backends `use parent` and override.

### Methods

```perl
async sub send {
    my ($self, $channel, $message) = @_;
    # Enqueue $message on $channel.
    # Returns Future(1) on success.
    # Returns Future->fail('ChannelFull', 'channel', $channel) if $channel
    #   is at capacity (the failure category MUST be 'ChannelFull').
    # MUST validate $channel via $self->_validate_channel($channel).
    # MUST validate $message via $self->_validate_message($message).
    # MUST notify any registered next_message waiters after enqueue.
}

async sub poll {
    my ($self, $channel) = @_;
    # Non-blocking dequeue.
    # Returns Future($message) if a message is immediately available.
    # Returns Future(undef) if $channel is empty.
    # MUST process expired/due-delayed entries before returning (see Delayed).
}

sub next_message {
    my ($self, $channel) = @_;
    # Async wait for the next message on $channel.
    # Returns a Future that resolves with the message when available.
    # The returned Future MUST be cancellable; cancel cleans up registration.
    # Implementation may register a waiter that send() and _deliver() resolve.
}

async sub subscribe {
    my ($self, $channel, $topic, %opts) = @_;
    # Add $channel to the membership group for $topic.
    # %opts is reserved for backend-internal use; the FACADE handles
    #   user-facing options (presence, history) and dispatches accordingly.
    # MUST be idempotent.
    # Returns Future(1).
}

async sub unsubscribe {
    my ($self, $channel, $topic) = @_;
    # Remove $channel from $topic's membership group.
    # Returns Future(1) whether or not $channel was a member.
}

async sub publish {
    my ($self, $topic, $message, %opts) = @_;
    # Fan out $message to all subscribers of $topic via _deliver().
    # %opts:
    #   exclude => $channel | [$channel, ...]  - skip these channels
    # MUST normalize 'exclude' via $self->_normalize_exclude(\%opts).
    # MUST call $self->_record_history($topic, $message) (no-op default;
    #   the History role overrides this hook).
    # MUST call $self->_dispatch_pattern_subscribers($topic, $message,
    #   \%delivered, \%excluded) (no-op default; the PatternSubs role
    #   overrides this hook).
    # Slow/full subscribers MUST NOT cause publish to fail — _deliver
    #   silently drops on capacity.
    # Returns Future(1).
}

async sub flush {
    my ($self) = @_;
    # Clear ALL backend state — queues, groups, AND any capability state
    #   (presence, history, delayed, patterns). Cancel all pending
    #   next_message waiters. Capability roles do NOT wrap flush; the
    #   backend's flush is expected to know its own substrate well enough
    #   to drop everything (e.g., Redis: KEYS prefix:* + DEL; PostgreSQL:
    #   TRUNCATE the four tables).
    # Returns Future(1).
    # NOTE: production code should not call flush; it exists for
    #   tests and ops. Kept in core because it's universally cheap.
}

async sub cleanup {
    my ($self, $channel) = @_;
    # Tear down core state for one channel:
    #   - remove from all subscription groups
    #   - clear queue
    #   - cancel pending next_message waiters
    # Capability roles wrap cleanup via `around` to tear down their own
    #   state (Presence broadcasts presence.leave and removes entries;
    #   Delayed removes pending entries targeting $channel; PatternSubs
    #   removes pattern registrations).
    # Returns Future(1).
}
```

### Internal helpers provided by the base class

These are not part of the contract a backend overrides; they are utility functions backends call into.

```perl
sub _validate_channel { ... }    # checks length, charset; dies with InvalidChannelName
sub _validate_message { ... }    # checks hashref + has 'type'; dies with InvalidMessage
sub _validate_topic   { ... }    # alias for _validate_channel (same rules apply)
sub _pattern_to_regex { ... }    # glob -> regex utility (used by PatternSubs role)
sub _normalize_exclude { ... }   # normalizes scalar/arrayref to %excluded hash
sub _make_presence_event { ... } # constructs { type=>'presence.join'|... } hashref
```

### Hook methods (default no-ops; capability roles override)

```perl
async sub _record_history { return }                  # History role overrides
async sub _dispatch_pattern_subscribers { return }    # PatternSubs role overrides
```

The hook pattern keeps `publish` polymorphic without needing each capability role to wrap `publish` with method modifiers. A backend that does not declare History simply calls the no-op base, and the work is skipped at the cost of one method call per publish.

### Configuration accepted by the base constructor

| Option         | Default     | Meaning                                               |
|----------------|-------------|-------------------------------------------------------|
| `capacity`     | 100         | Max messages per channel queue                        |
| `expiry`       | 60          | Message TTL in seconds                                |
| `group_expiry` | 86400       | Subscription group membership TTL                     |
| `max_size`     | 1_048_576   | Max serialized message size (bytes)                   |
| `history_size` | 0           | Number of messages to retain per topic (History only) |

Backend-specific options (e.g., `redis => $client` for Redis, `dbh => $dbh` for PostgreSQL) are validated by each backend's `BUILDARGS`-equivalent / constructor.

### Validation rules (lowest common denominator)

- **Channel/topic name**: `^[\w.\-:]+$`, max 100 characters, defined and non-empty. Rationale: this set is safe across Redis (binary-safe), PostgreSQL (`NOTIFY` accepts these without quoting up to 63 bytes — the 100-char limit accommodates quoted identifiers but a future PostgreSQL backend may need to tighten or escape), and Pusher-style services. A backend may NOT loosen these rules. A backend MAY tighten them by overriding `_validate_channel` if its substrate is more restrictive (Pusher-class services), but this should be rare.
- **Message**: must be a HASH reference. Must have a `type` key with a defined value. The `type` value is opaque to the backend — applications interpret it.

These rules live in the base class. Both Memory and Redis inherit them. Today's silent divergence (Memory enforces; Redis does not) is fixed by this consolidation.

---

## Capability Roles

Each role lives in its own file under `lib/PAGI/Middleware/Channels/Backend/Role/`. Each role declares the methods it requires the backend to implement (`requires 'foo'`) and may provide shared helper methods (e.g., the History role provides a default `subscribe_with_history` composition).

### Role::Presence

**Purpose:** Track which connections are currently subscribed to a topic, with arbitrary per-subscriber metadata. Broadcasts `presence.join` and `presence.leave` events automatically when subscribers come and go.

**Required methods:**

```perl
async sub track {
    my ($self, $topic, $channel, $presence_data) = @_;
    # Record that $channel is present on $topic with $presence_data (hashref).
    # Idempotent. Returns Future(1).
    # NOTE: $channel is now passed explicitly — see the set_channel_id removal
    #   note below.
}

async sub untrack {
    my ($self, $topic, $channel) = @_;
    # Remove $channel's presence entry from $topic. Returns Future(1) whether
    # or not the entry existed.
}

async sub list_presence {
    my ($self, $topic, %opts) = @_;
    # Returns Future(@presence_data_hashrefs) for all current subscribers.
    # Honors %opts{limit} - croak with a "use scan_presence" message if the
    # number of members exceeds limit. (See Memory backend for the message
    # format to match.)
}

async sub count_presence {
    my ($self, $topic) = @_;
    # Returns Future($integer). MUST be O(1)-ish at the storage layer.
    # (Redis: HLEN. Postgres: SELECT COUNT(*) on indexed column.)
}

async sub scan_presence {
    my ($self, $topic, %opts) = @_;
    # Cursor-based iteration. %opts{cursor} starts at 0; %opts{count} is a hint.
    # Returns Future(($next_cursor, @presence_data)).
    # Returns 0 as $next_cursor when iteration is complete.
    # Semantics match Redis SCAN: pages may overlap or skip if data changes
    # mid-iteration.
}
```

**Hooks the role overrides:**

```perl
# Override the base's no-op cleanup hook so presence entries are removed
# and presence.leave events broadcast when a channel cleans up.
around cleanup => sub {
    my ($orig, $self, $channel) = @_;
    # ... walk presence entries for $channel, broadcast leave, remove ...
    return $self->$orig($channel);
};
```

**Removal of `set_channel_id` / `channel_id`:**

The current implementation has the backend store `_channel_id` as mutable state, with the middleware calling `$backend->set_channel_id($channel_name)` per request. **This is racy**: the backend instance is shared across all in-flight requests, and `_channel_id` is overwritten by every new connection. If two connections interleave (await yields between `set_channel_id` and `track`), the wrong channel gets tracked.

Under this design, `track` and `untrack` take `$channel` explicitly. The facade (`PAGI::Channel`) holds `channel_name` as instance state and passes it to the backend on each call. No mutable per-connection state lives on the shared backend. The race goes away.

**Shared role helpers:**

```perl
sub _presence_event {
    my ($self, $topic, $type, $presence_data) = @_;
    return { type => $type, topic => $topic, presence => $presence_data };
}
```

Used internally to construct the event hashrefs broadcast on subscribe-with-presence and unsubscribe-with-presence.

### Role::History

**Purpose:** Retain the most recent N messages per topic so newly-subscribing connections can replay them.

**Required methods:**

```perl
async sub _record_history {
    my ($self, $topic, $message) = @_;
    # Called by core publish() (overrides the no-op hook in the base class).
    # MUST skip messages whose type matches /^presence\./ - those are
    # ephemeral notifications, not application data.
    # MUST trim to $self->{history_size} messages.
    # Returns Future(1).
}

async sub read_history {
    my ($self, $topic, $count) = @_;
    # Returns Future(@messages) - the most recent $count messages, oldest first.
    # Empty list if no history. $count > history_size yields at most history_size.
}
```

**Provided default method:**

```perl
async sub subscribe_with_history {
    my ($self, $channel, $topic, $count, %opts) = @_;
    my @history = await $self->read_history($topic, $count);
    for my $msg (@history) {
        await $self->_deliver($channel, $msg);
    }
    await $self->subscribe($channel, $topic, %opts);
    return 1;
}
```

Backends may override for performance (e.g., a single transaction in PostgreSQL) but the default composition is correct for any backend that has History + Core.

### Role::Delayed

**Purpose:** Schedule a `send` or `publish` for a future time.

**Required methods:**

```perl
async sub schedule_delayed {
    my ($self, $type, $target, $message, $delivery_time) = @_;
    # $type is 'send' or 'publish'.
    # $target is a channel name (for send) or topic name (for publish).
    # $delivery_time is an absolute epoch timestamp (Time::HiRes-precision).
    # Returns Future(1).
}

async sub process_delayed {
    my ($self) = @_;
    # Drain all entries with delivery_time <= now. For each:
    #   - if type='send': call $self->send($target, $message)
    #   - if type='publish': call $self->publish($target, $message)
    # Returns Future($count_processed).
    # MUST be idempotent and safe to call concurrently (e.g., guarded by
    # WATCH/MULTI in Redis or SELECT...FOR UPDATE in Postgres).
}
```

**Provided default methods:**

The role provides default `send_delayed` and `publish_delayed` that compute the absolute delivery time and call `schedule_delayed`. Backends only need to implement the storage primitive.

```perl
async sub send_delayed {
    my ($self, $channel, $message, $delay_seconds) = @_;
    return $self->schedule_delayed(
        'send', $channel, $message, Time::HiRes::time() + $delay_seconds
    );
}

async sub publish_delayed {
    my ($self, $topic, $message, $delay_seconds) = @_;
    return $self->schedule_delayed(
        'publish', $topic, $message, Time::HiRes::time() + $delay_seconds
    );
}
```

**Hook the role overrides:**

The role wraps `poll` and `cleanup`:

```perl
around poll => sub {
    my ($orig, $self, $channel) = @_;
    await $self->process_delayed if $self->_has_due_delayed;
    return $self->$orig($channel);
};

around cleanup => sub {
    my ($orig, $self, $channel) = @_;
    # Remove delayed entries whose target == $channel (for type=send only)
    return $self->$orig($channel);
};
```

`_has_due_delayed` is a cheap check the role requires the backend to implement (Memory: `@{$self->{delayed}} && $self->{delayed}[0]{deliver_at} <= time()`; Redis: `ZCARD` cached or `EXISTS` + `ZRANGEBYSCORE LIMIT 0 1`). For backends where the check itself is non-trivial, returning `1` unconditionally is acceptable — the cost is one extra `process_delayed` call per `poll`.

### Role::PatternSubs

**Purpose:** Subscribe a channel to all topics matching a glob pattern. `*` matches one segment (no dots); `**` matches zero or more segments.

**Required methods:**

```perl
async sub psubscribe {
    my ($self, $channel, $pattern) = @_;
    # Register $channel as receiving messages for any topic matching $pattern.
    # Idempotent. Returns Future(1).
}

async sub punsubscribe {
    my ($self, $channel, $pattern) = @_;
    # Remove a pattern registration. If $pattern is undef, remove all of
    # $channel's pattern registrations. Returns Future(1).
}

async sub _list_pattern_subscribers {
    my ($self, $topic) = @_;
    # Returns Future(@channels) - all channels with at least one pattern that
    # matches $topic. Should NOT include channels already subscribed via the
    # exact-match group (caller dedupes).
    # Implementations can use $self->_pattern_to_regex($pattern) for the match.
}
```

**Hook the role overrides:**

```perl
async sub _dispatch_pattern_subscribers {
    my ($self, $topic, $message, $delivered, $excluded) = @_;
    my @candidates = await $self->_list_pattern_subscribers($topic);
    for my $channel (@candidates) {
        next if $excluded->{$channel};
        next if $delivered->{$channel};
        await $self->_deliver($channel, $message);
        $delivered->{$channel} = 1;
    }
}

# And the cleanup hook:
around cleanup => sub {
    my ($orig, $self, $channel) = @_;
    await $self->punsubscribe($channel, undef);  # remove all patterns
    return $self->$orig($channel);
};
```

The `_pattern_to_regex` utility lives on the base class so any backend can call it without `with`-ing the PatternSubs role. (PatternSubs uses it; History conceivably could too if a topic-pattern history feature is ever added.)

---

## Validation Policy

Single source of truth in `PAGI::Middleware::Channels::Backend`. Three methods, all called by the public `send`/`subscribe`/`unsubscribe`/`publish` methods at their entry points. They `die` (not return-as-Future) on bad input — validation failure is a programmer error, not a runtime condition to handle. Backends MUST NOT bypass these calls.

```perl
sub _validate_channel {
    my ($self, $channel) = @_;
    die "InvalidChannelName: empty" unless defined $channel && length $channel;
    die "InvalidChannelName: too long" if length $channel > 100;
    die "InvalidChannelName: bad chars" unless $channel =~ /^[\w.\-:]+$/;
}

sub _validate_topic { goto &_validate_channel }   # same rules

sub _validate_message {
    my ($self, $message) = @_;
    die "InvalidMessage: not a hashref" unless ref $message eq 'HASH';
    die "InvalidMessage: missing type" unless defined $message->{type};
}
```

A backend with stricter substrate constraints (a hypothetical Pusher backend with its own naming rules) MAY override `_validate_channel` to add more checks. It MUST NOT loosen the base rules — code that worked on Memory/Redis must keep working when it's swapped to the new backend.

`max_size` is a config value in the base but is not currently enforced. Enforcement will be added as part of `_validate_message` in the implementation, with a `MessageTooLarge` failure category.

---

## Facade Composition

`PAGI::Channel` keeps the same public API. Its responsibility grows: it composes capability calls and gates capability methods on `does` checks.

### Option dispatch (already partially done)

```perl
async sub send {
    my ($self, $channel, $message, %opts) = @_;
    if (my $delay = delete $opts{delay}) {
        $self->_require_capability('Delayed');
        return await $self->{backend}->send_delayed($channel, $message, $delay);
    }
    return await $self->{backend}->send($channel, $message);
}

async sub subscribe {
    my ($self, $topic, %opts) = @_;
    my $history_count = delete $opts{history};
    my $presence     = delete $opts{presence};

    if ($history_count) {
        $self->_require_capability('History');
        return await $self->{backend}->subscribe_with_history(
            $self->{channel_name}, $topic, $history_count, %opts
        );
    }

    await $self->{backend}->subscribe($self->{channel_name}, $topic, %opts);

    if ($presence) {
        $self->_require_capability('Presence');
        await $self->{backend}->track($topic, $self->{channel_name}, $presence);
        await $self->{backend}->publish($topic,
            $self->{backend}->_make_presence_event($topic, 'presence.join', $presence),
            exclude => $self->{channel_name},
        );
    }

    return 1;
}

# Same pattern for publish/$opts{delay}, unsubscribe/presence.leave, etc.
```

### Capability gating

```perl
sub _require_capability {
    my ($self, $capability) = @_;
    my $role = "PAGI::Middleware::Channels::Backend::Role::$capability";
    return if $self->{backend}->does($role);
    Carp::croak(
        "Backend " . ref($self->{backend}) . " does not support the "
      . "$capability capability. Required for this operation. "
      . "See PAGI::Middleware::Channels documentation for the capability matrix."
    );
}
```

The croak message names the backend class, the capability, and points users at the docs. Per agreed scope: no structured exception class for now — string `croak` is consistent with the rest of the codebase.

### Bare capability methods on the facade

`track`, `untrack`, `list_presence`, `count_presence`, `scan_presence`, `psubscribe`, `punsubscribe`, `next_message` keep their existing facade methods, each gated by `_require_capability` at the top.

---

## Compliance Test Suite

The single highest-ROI deliverable in this work. A new module `Test::PAGI::Channels::Contract` exports a `run_contract_tests($name, $factory, %opts)` function. The factory returns a fresh, flushed backend instance on each call.

```perl
package Test::PAGI::Channels::Contract;
use strict;
use warnings;
use parent 'Exporter';
use Test2::V0;
use PAGI::Middleware::Channels::Backend;

our @EXPORT_OK = qw(run_contract_tests);

sub run_contract_tests {
    my ($label, $factory, %opts) = @_;

    subtest "$label - core" => sub {
        _test_core($factory);
    };

    my $b = $factory->();
    if ($b->does('PAGI::Middleware::Channels::Backend::Role::Presence')) {
        subtest "$label - presence" => sub { _test_presence($factory) };
    }
    if ($b->does('PAGI::Middleware::Channels::Backend::Role::History')) {
        subtest "$label - history" => sub { _test_history($factory) };
    }
    if ($b->does('PAGI::Middleware::Channels::Backend::Role::Delayed')) {
        subtest "$label - delayed" => sub { _test_delayed($factory) };
    }
    if ($b->does('PAGI::Middleware::Channels::Backend::Role::PatternSubs')) {
        subtest "$label - pattern subs" => sub { _test_pattern_subs($factory) };
    }
}
```

Each `_test_*` sub contains the existing assertions, ported once. Memory wires it up like:

```perl
# t/10-memory/contract.t
use Test2::V0;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop);
use Test::PAGI::Channels::Contract qw(run_contract_tests);
use PAGI::Middleware::Channels::Backend::Memory;

init_loop();
run_contract_tests('Memory', sub {
    PAGI::Middleware::Channels::Backend::Memory->new;
});
done_testing;
```

Redis is identical except the factory builds a Redis backend with a fresh prefix and calls `flush`. PostgreSQL, when it lands, is one more test file.

The existing per-backend `.t` files are kept ONLY for backend-specific tests that don't fit the contract (e.g., the Redis-isolation-via-prefix test, the `subscriber` connection tests). Most of `t/10-memory/*.t` and `t/30-redis/*.t` is deleted.

---

## Implementing a New Backend (Implementer's Guide)

This section is the heaviest part of the doc. It is the reference future work — including the PostgreSQL backend — uses to know what to build.

### Step 1: Decide which capabilities you'll support

Look at the capability matrix. For each capability you plan to support, your backend must `with` that role and implement its required methods. Anything you skip becomes a `croak` for users that try to use it through the facade.

For PostgreSQL specifically: all four capabilities (Presence, History, Delayed, PatternSubs) are achievable. The implementer guide assumes all four are in scope.

### Step 2: Create the backend module

```perl
package PAGI::Middleware::Channels::Backend::PostgreSQL;
use strict;
use warnings;
use parent 'PAGI::Middleware::Channels::Backend';
use Role::Tiny::With;
use Future::AsyncAwait;
use Future;
use namespace::clean;

with 'PAGI::Middleware::Channels::Backend::Role::Presence',
     'PAGI::Middleware::Channels::Backend::Role::History',
     'PAGI::Middleware::Channels::Backend::Role::Delayed',
     'PAGI::Middleware::Channels::Backend::Role::PatternSubs';

sub new {
    my ($class, %args) = @_;
    my $dbh = $args{dbh}
        or die __PACKAGE__ . ": 'dbh' argument required (DBI handle)";
    my $self = $class->SUPER::new(%args);  # populates capacity/expiry/etc.
    $self->{_dbh} = $dbh;
    $self->{_prefix} = $args{prefix} // 'pagi_channels_';
    return $self;
}

# Override the 8 core methods + the role-required methods.
# Use base class helpers: _validate_channel, _validate_message,
#                        _normalize_exclude, _make_presence_event,
#                        _pattern_to_regex.
```

### Step 3: Implement Core (8 methods)

For each core method, see "Core Backend Contract" above. Specific PostgreSQL hints:

- **send / poll**: use a `pagi_channels_messages(channel, message_json, expires_at)` table with an index on `(channel, id)`. `send` is `INSERT`; `poll` is `DELETE ... RETURNING` on the oldest row.
- **next_message**: combine `LISTEN/NOTIFY` (PostgreSQL pub/sub) with the message table. On `send`, emit `NOTIFY pagi_$channel`. The `next_message` waiter awaits the notification, then `poll`s. Maintain one persistent listener connection (separate from the command connection) — same architecture as Redis. Watch out for: `LISTEN` is per-session, not per-database.
- **subscribe / unsubscribe**: a `pagi_channels_groups(topic, channel, expires_at)` table with `PRIMARY KEY (topic, channel)`. `subscribe` is `INSERT ... ON CONFLICT DO UPDATE` for idempotence.
- **publish**: `SELECT channel FROM pagi_channels_groups WHERE topic = ? AND expires_at > NOW()` then for each, call `_deliver`. Then call `_record_history` and `_dispatch_pattern_subscribers`.
- **flush**: `TRUNCATE` the four tables under your prefix.
- **cleanup**: `DELETE` from queues, groups, presence, patterns where channel = $channel. Wrap in a transaction.

Watch outs specific to PostgreSQL:
- **`NOTIFY` payload limit is 8000 bytes**. Don't put the message in the notification — just emit the channel name as a "ping" and let the consumer `SELECT` the actual message.
- **`LISTEN` channel names** are PostgreSQL identifiers — max 63 bytes unquoted, must start with letter/underscore. The 100-char `_validate_channel` rule may produce names too long for unquoted `NOTIFY`. Either use quoted identifiers (`NOTIFY "long.name.with.dots"`) or hash channel names to a fixed-length identifier and store the mapping. Recommendation: quote.
- **Connection lifecycle**: the listener connection must be persistent and separate. If using `DBD::Pg` async (which is the gating dependency for this work), the listener connection's `pg_notifies` is checked on socket-readable events.

### Step 4: Implement Presence (5 methods)

- `track`: `INSERT ... ON CONFLICT DO UPDATE` into `pagi_channels_presence(topic, channel, presence_json, expires_at)`.
- `untrack`: `DELETE FROM pagi_channels_presence WHERE topic = ? AND channel = ?`.
- `list_presence`: `SELECT presence_json FROM ... WHERE expires_at > NOW()`. Honor `limit` per the contract.
- `count_presence`: `SELECT COUNT(*) ... WHERE expires_at > NOW()`.
- `scan_presence`: pagination via `OFFSET/LIMIT` ordered by `(topic, channel)`. Cursor is the offset; return 0 when done.

The role's `cleanup` hook removes presence entries and broadcasts `presence.leave` automatically — your core `cleanup` should call `$self->$orig` last so the role's `around` runs.

### Step 5: Implement History (2 methods)

- `_record_history`: `INSERT` into `pagi_channels_history(topic, message_json, recorded_at)`. Trim to `history_size` via a window function or `DELETE` of overflow rows. Skip messages whose type matches `^presence\.`.
- `read_history`: `SELECT message_json FROM pagi_channels_history WHERE topic = ? ORDER BY id DESC LIMIT ?`, then reverse to return oldest-first.

The role provides `subscribe_with_history` for free.

### Step 6: Implement Delayed (3 methods)

- `schedule_delayed`: `INSERT INTO pagi_channels_delayed(deliver_at, type, target, message_json)`.
- `process_delayed`: in a transaction, `SELECT ... WHERE deliver_at <= NOW() FOR UPDATE SKIP LOCKED`, then for each row call `send` or `publish` and `DELETE` it. `SKIP LOCKED` makes this safe to call from multiple workers concurrently.
- `_has_due_delayed`: `SELECT 1 FROM pagi_channels_delayed WHERE deliver_at <= NOW() LIMIT 1`.

### Step 7: Implement PatternSubs (3 methods)

- `psubscribe`: `INSERT ... ON CONFLICT DO NOTHING` into `pagi_channels_patterns(channel, pattern)`.
- `punsubscribe`: `DELETE` matching rows; if `$pattern` is undef, delete all rows for `$channel`.
- `_list_pattern_subscribers`: `SELECT DISTINCT channel, pattern FROM pagi_channels_patterns`, then in Perl filter via `$self->_pattern_to_regex($pattern)`. For very large pattern sets, you can move match logic into PostgreSQL (`SIMILAR TO` or a `regexp_match` UDF), but the in-Perl filter is correct and the `_pattern_to_regex` utility is shared.

### Step 8: Wire into the compliance test suite

```perl
# t/40-postgresql/contract.t
use Test2::V0;
use Test::PAGI::Channels qw(init_loop skip_without_postgres make_dbh);
use Test::PAGI::Channels::Contract qw(run_contract_tests);
use PAGI::Middleware::Channels::Backend::PostgreSQL;

init_loop();
SKIP: { skip_without_postgres();
    run_contract_tests('PostgreSQL', sub {
        my $backend = PAGI::Middleware::Channels::Backend::PostgreSQL->new(
            dbh    => make_dbh(),
            prefix => "test_$$\_",
        );
        $backend->flush->get;
        $backend;
    });
}
done_testing;
```

That's the entire test wiring. Every test in the contract suite runs against PostgreSQL; capability-gated subtests skip themselves automatically based on the role declarations.

### Step 9: Add backend-specific tests as needed

For things that don't fit the contract:
- LISTEN/NOTIFY connection lifecycle, reconnect behavior
- Schema migration / table creation
- Transaction semantics under concurrent process_delayed
- `prefix` isolation across multiple PostgreSQL backends sharing a database

### Step 10: Document in the capability matrix

Update the capability matrix in `PAGI::Middleware::Channels` POD and in this design doc.

---

## Migration Plan (Sequencing)

Three phases, each independently committable and shippable. Each phase makes the next one safer.

### Phase 1: Compliance test suite (no production code change)

- Create `Test::PAGI::Channels::Contract`
- Port the existing per-backend tests into `_test_core`, `_test_presence`, `_test_history`, `_test_delayed`, `_test_pattern_subs`
- Wire Memory and Redis to it via `t/10-memory/contract.t` and `t/30-redis/contract.t`
- Delete (or trim to backend-specific concerns only) the existing per-feature `.t` files
- Verify both backends pass the full contract suite under existing code

This phase reveals the validation drift bug as a real failing test (Redis fails the validation subtests). That failure is the proof point.

### Phase 2: Populate the base class (no public API change)

- Move `_pattern_to_regex`, validation methods, `DEFAULT_*` constants, `_normalize_exclude`, `_make_presence_event` into `Backend.pm`
- Have `Memory` and `Redis` `use parent 'Backend'` (already do) and call `SUPER::new(%args)` from their constructors to pick up shared config
- Delete the duplicates from both backends
- Add validation calls to the Redis backend's entry methods (closes the drift bug)
- Re-run compliance suite — now Redis passes the validation tests too

### Phase 3: Capability roles + `set_channel_id` removal

- Add `Role::Tiny` to `cpanfile`
- Create the four role modules under `lib/PAGI/Middleware/Channels/Backend/Role/`
- For each role: define the required methods, the `around` hooks, the provided defaults
- Refactor `Memory` and `Redis` to `with` all four roles and to no longer carry `_channel_id` state
- Refactor `PAGI::Channel` to (a) pass `channel_name` explicitly to `track`/`untrack` and (b) gate capability calls with `_require_capability`
- Remove `set_channel_id` / `channel_id` from the middleware's `_create_channel_interface`
- Remove `subscribe_with_history`, `send_delayed`, `publish_delayed` from the abstract method list (they're either provided defaults on the History/Delayed roles, or facade-composed)
- Update `t/10-memory/08-abstract-backend.t` (the contract-pinning test) to reflect the new shape — fewer abstract methods, presence of optional roles
- Update POD across `Backend.pm`, `Channel.pm`, `Memory.pm`, `Redis.pm`, the four role modules, and the user-facing capability matrix in `Channels.pm`

### After phase 3

- The README/POD includes the capability matrix and a "How to add a backend" link to this design doc
- `cpanfile` has `Role::Tiny` as a runtime dep
- The implementer's guide section above is the reference for the PostgreSQL backend, written separately

---

## Open Questions / Deferred Decisions

These are intentionally not decided in this work and are flagged for later:

- **Structured exception classes.** Today `croak` everywhere. A `PAGI::Channels::Error::*` hierarchy would be more correct but adds a dep (`Throwable` or hand-rolled) and a refactor. Defer.
- **`max_size` enforcement.** Currently configured but not checked. The implementation should add a check in `_validate_message` (`die "MessageTooLarge"` if the JSON-encoded form exceeds `max_size`). Confirm this lands in Phase 2.
- **A `Testable` capability.** `flush` is in the core today. If a future backend has a substrate where flushing is destructive at the substrate level (a hypothetical "PAGI on top of someone else's Kafka cluster"), `flush` may need to move to its own role. Not now.
- **History TTL versus history_size.** Today history is bounded by count only. Some backends (Redis with key TTL, PostgreSQL with `expires_at`) would naturally support time-bound history too. If a `history_ttl` config arrives later, the History role's contract gains `_record_history` accepting an expiry hint.
- **Pattern subscriptions with Redis-native KEYSPACE NOTIFICATIONS.** The current Redis backend uses a `keys pat:*` scan in `publish`, which is `O(N)` over the keyspace. A future optimization could use Redis's built-in `PSUBSCRIBE` for keyspace notifications; this is a backend-internal change and doesn't affect the role contract.
- **A "broker" capability for cluster coordination.** Phoenix.PubSub's `node_name` and `direct_broadcast` model isn't currently in scope. If a future use case wants to address messages to a specific worker in a multi-worker setup, a `Cluster` capability role might emerge. Not now.

---

## Files Touched (preview)

| File                                                                                | Change |
|-------------------------------------------------------------------------------------|--------|
| `cpanfile`                                                                          | + `requires 'Role::Tiny'` |
| `lib/PAGI/Middleware/Channels/Backend.pm`                                           | Populate: validation, constants, utilities, hooks, reduced abstract list |
| `lib/PAGI/Middleware/Channels/Backend/Role/Presence.pm`                             | New |
| `lib/PAGI/Middleware/Channels/Backend/Role/History.pm`                              | New |
| `lib/PAGI/Middleware/Channels/Backend/Role/Delayed.pm`                              | New |
| `lib/PAGI/Middleware/Channels/Backend/Role/PatternSubs.pm`                          | New |
| `lib/PAGI/Middleware/Channels/Backend/Memory.pm`                                    | Refactor: `with` roles, drop duplicated code, drop `_channel_id` |
| `lib/PAGI/Middleware/Channels/Backend/Redis.pm`                                     | Same as Memory + add validation calls |
| `lib/PAGI/Middleware/Channels.pm`                                                   | Drop `set_channel_id` call in `_create_channel_interface`; capability matrix in POD |
| `lib/PAGI/Channel.pm`                                                               | Add `_require_capability`; pass `channel_name` to track/untrack; compose history+presence in subscribe |
| `t/lib/Test/PAGI/Channels/Contract.pm`                                              | New: parameterized compliance suite |
| `t/10-memory/contract.t`                                                            | New: wires Memory to compliance suite |
| `t/30-redis/contract.t`                                                             | New: wires Redis to compliance suite |
| `t/10-memory/01-core.t` ... `t/10-memory/06-history.t`                              | Delete (covered by contract suite) |
| `t/30-redis/01-core.t` ... `t/30-redis/06-history.t`                                | Delete (covered by contract suite) |
| `t/10-memory/07-cleanup.t`, `t/30-redis/07-next-message.t`, etc.                    | Audit: keep what's backend-specific, delete what's contract-covered |
| `t/10-memory/08-abstract-backend.t`                                                 | Update: smaller abstract list + role-presence assertions |

---

# Implementation Plan

This plan implements the design above in three sequenced phases. Each phase produces working, fully-tested software at every commit. A fresh session resuming mid-stream should: (1) read the entire design above this point first, (2) check `git log --oneline` to see which tasks have been committed, (3) resume from the next task in order.

## Conventions used by every task

**Perl environment.** Every Perl command MUST be wrapped per the project's CLAUDE.md:
```bash
bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && <command>'
```
Use the perlbrew wrapper for every `prove`, `perl`, and `cpanm` invocation. The shorthand `<perl-env>` below means this prefix.

**Test commands.**
- Run full Memory suite: `<perl-env> prove -lr t/10-memory/`
- Run full Redis suite: `<perl-env> REDIS_HOST=localhost prove -lr t/30-redis/`
- Run full facade suite: `<perl-env> prove -lr t/20-facade/`
- Run all tests: `<perl-env> REDIS_HOST=localhost prove -lr t/`
- Single test file: `<perl-env> prove -lv t/10-memory/01-core.t`

**Redis prerequisite.** `cd t && docker compose up -d && cd ..` before running Redis tests.

**Commit cadence.** One commit per task (each task ends with a Commit step). Commit messages match the project's existing style: `feat:`, `refactor:`, `chore:`, `test:`. Use HEREDOC for multi-line bodies. The trailing `Co-Authored-By` line should be included per the existing project pattern (see `git log` for examples).

**Self-recovery checkpoint.** Between tasks, a fresh session can recover state with:
```bash
git log --oneline -20                  # which tasks landed
git status                             # any uncommitted work
<perl-env> REDIS_HOST=localhost prove -lr t/   # current test state
```
The next task is the first unchecked task in this document for the active phase. If you find half-done work, complete the in-progress task before starting the next one.

---

## Phase 1: Compliance Test Suite (no production code change)

**Objective:** Build a single parameterized test suite that runs against any backend factory. Both backends pass this suite under existing code, *except* validation tests will fail for Redis (because Redis doesn't validate). That failure is the proof point for Phase 2 and is left as a pending xfail/SKIP in Task 1.7.

**Why first:** It's pure additive code, has zero risk of regressing existing behavior, gives us the safety net we need to refactor confidently in Phases 2 and 3, and surfaces the validation drift bug as a concrete test failure.

### Task 1.1: Create `Test::PAGI::Channels::Contract` skeleton

**Files:**
- Create: `t/lib/Test/PAGI/Channels/Contract.pm`

- [ ] **Step 1: Write the module skeleton**

Create `t/lib/Test/PAGI/Channels/Contract.pm` with:

```perl
package Test::PAGI::Channels::Contract;
use strict;
use warnings;
use parent 'Exporter';
use Test2::V0;
use Future::AsyncAwait;
use Future;
use Future::IO;
use Time::HiRes ();

our @EXPORT_OK = qw(run_contract_tests);

# run_contract_tests($label, $factory, %opts)
#
# $label   - human-readable backend name, used in subtest names
# $factory - coderef returning a fresh, flushed backend on each call
# %opts    - reserved for future per-backend skips
#
# The factory MUST return a backend that has been flushed of any state
# left from prior runs. Memory: just `->new`. Redis: `->new` + `->flush->get`.
sub run_contract_tests {
    my ($label, $factory, %opts) = @_;

    subtest "$label - core (send/poll/FIFO/capacity)" => sub {
        _test_core_send_poll($factory);
        _test_core_fifo($factory);
        _test_core_capacity($factory);
    };

    subtest "$label - core (pubsub)" => sub {
        _test_pubsub_basic($factory);
        _test_pubsub_exclude($factory);
        _test_pubsub_full_drops($factory);
        _test_pubsub_unsubscribe($factory);
        _test_pubsub_idempotent($factory);
    };

    subtest "$label - core (next_message)" => sub {
        _test_next_message($factory);
    };

    subtest "$label - core (cleanup/flush)" => sub {
        _test_cleanup($factory);
        _test_flush($factory);
    };

    subtest "$label - core (validation)" => sub {
        _test_validation($factory);
    };

    # Capability subtests follow in subsequent tasks.
    # In Phase 1 we run them unconditionally because Memory and Redis
    # both implement everything. In Phase 3 we add `->does(...)` gating.
    subtest "$label - presence" => sub {
        _test_presence($factory);
    };

    subtest "$label - history" => sub {
        _test_history($factory);
    };

    subtest "$label - delayed" => sub {
        _test_delayed($factory);
    };

    subtest "$label - pattern subs" => sub {
        _test_pattern_subs($factory);
    };
}

# Subtest implementations are filled in by Tasks 1.2 - 1.6.
# Stub them so the module loads:
sub _test_core_send_poll  { ok(1, 'placeholder - Task 1.2') }
sub _test_core_fifo       { ok(1, 'placeholder - Task 1.2') }
sub _test_core_capacity   { ok(1, 'placeholder - Task 1.2') }
sub _test_pubsub_basic    { ok(1, 'placeholder - Task 1.2') }
sub _test_pubsub_exclude  { ok(1, 'placeholder - Task 1.2') }
sub _test_pubsub_full_drops { ok(1, 'placeholder - Task 1.2') }
sub _test_pubsub_unsubscribe { ok(1, 'placeholder - Task 1.2') }
sub _test_pubsub_idempotent { ok(1, 'placeholder - Task 1.2') }
sub _test_next_message    { ok(1, 'placeholder - Task 1.2') }
sub _test_cleanup         { ok(1, 'placeholder - Task 1.2') }
sub _test_flush           { ok(1, 'placeholder - Task 1.2') }
sub _test_validation      { ok(1, 'placeholder - Task 1.2') }
sub _test_presence        { ok(1, 'placeholder - Task 1.3') }
sub _test_history         { ok(1, 'placeholder - Task 1.4') }
sub _test_delayed         { ok(1, 'placeholder - Task 1.5') }
sub _test_pattern_subs    { ok(1, 'placeholder - Task 1.6') }

# Helper used by every subtest: synchronously run a Future-returning coderef.
sub _run(&) {
    my ($code) = @_;
    my $f = $code->();
    return $f->get if $f && ref($f) && $f->can('isa') && $f->isa('Future');
    return $f;
}

1;
```

- [ ] **Step 2: Verify the module loads**

```bash
<perl-env> perl -Ilib -It/lib -MTest::PAGI::Channels::Contract -e 'print "loaded\n"'
```
Expected output: `loaded`

- [ ] **Step 3: Commit**

```bash
git add t/lib/Test/PAGI/Channels/Contract.pm
git commit -m "$(cat <<'EOF'
test: add compliance test suite skeleton (Test::PAGI::Channels::Contract)

Parameterized suite that runs the full backend contract against any
factory. Subtest bodies are stubbed; subsequent tasks port the per-backend
test files into the shared subs.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 1.2: Port core tests (send/poll/pubsub/cleanup/next_message/validation)

**Files:**
- Modify: `t/lib/Test/PAGI/Channels/Contract.pm`

This task ports the bodies of `t/10-memory/01-core.t`, `t/10-memory/02-pubsub.t`, parts of `t/10-memory/07-cleanup.t` (the non-presence subtests), and `t/10-memory/09-next-message.t` into the named `_test_*` subs in `Contract.pm`. It also adds a brand-new `_test_validation` sub (no equivalent in the existing per-backend suites — this codifies the validation contract).

The porting is mechanical: replace `PAGI::Middleware::Channels::Backend::Memory->new(...)` with `$factory->()` for each `$backend` construction. The factory returns a fresh, already-flushed instance, so any per-test setup like `$backend->flush` from the Redis tests is unnecessary.

- [ ] **Step 0: Add `_run(&)` forward declaration**

Perl prototypes only apply to calls *parsed after* the declaration. The `sub _run(&)` definition lives at the bottom of `Contract.pm`, so the `_run { ... }` block-form calls we're about to paste into the `_test_*` subs above it would fail to parse. Add this line in `Contract.pm` immediately after `our @EXPORT_OK = qw(run_contract_tests);` (around line 12):

```perl
sub _run(&);
```

That's a forward declaration with prototype. With it in place, every subsequent `_run { ... }` call parses correctly regardless of file position.

- [ ] **Step 1: Port `_test_core_send_poll`, `_test_core_fifo`, `_test_core_capacity`**

Replace the placeholder stubs with the contents of the three subtests in `t/10-memory/01-core.t:11-58`. Each ported sub takes `($factory)` and reads:

```perl
sub _test_core_send_poll {
    my ($factory) = @_;
    my $backend = $factory->();

    my $msg = _run { $backend->poll('test.channel') };
    is($msg, undef, 'poll on empty channel returns undef');

    _run { $backend->send('test.channel', { type => 'test', data => 1 }) };

    $msg = _run { $backend->poll('test.channel') };
    is($msg, { type => 'test', data => 1 }, 'poll returns sent message');

    $msg = _run { $backend->poll('test.channel') };
    is($msg, undef, 'poll after consume returns undef');
}

sub _test_core_fifo {
    my ($factory) = @_;
    my $backend = $factory->();

    _run { $backend->send('ch', { type => 'msg', n => 1 }) };
    _run { $backend->send('ch', { type => 'msg', n => 2 }) };
    _run { $backend->send('ch', { type => 'msg', n => 3 }) };

    is(_run { $backend->poll('ch') }->{n}, 1, 'first message');
    is(_run { $backend->poll('ch') }->{n}, 2, 'second message');
    is(_run { $backend->poll('ch') }->{n}, 3, 'third message');
    is(_run { $backend->poll('ch') }, undef, 'queue empty');
}

sub _test_core_capacity {
    my ($factory) = @_;
    my $backend = $factory->(capacity => 3);

    _run { $backend->send('ch', { type => 'msg', n => 1 }) };
    _run { $backend->send('ch', { type => 'msg', n => 2 }) };
    _run { $backend->send('ch', { type => 'msg', n => 3 }) };

    my $result = _run {
        $backend->send('ch', { type => 'msg', n => 4 })->catch(sub {
            my ($cat) = @_;
            return { error => $cat };
        });
    };
    is($result->{error}, 'ChannelFull', 'send to full channel fails');
}
```

**Note on `$factory->(capacity => 3)`:** the factory contract from Task 1.1 needs amending — it must accept optional constructor overrides. Update the contract comment in `Contract.pm`:

```perl
# $factory - coderef returning a fresh, flushed backend on each call.
#            Accepts optional %args that are passed to the backend
#            constructor (e.g., $factory->(capacity => 3, history_size => 10))
```

- [ ] **Step 2: Port pubsub subs**

Port the subtests from `t/10-memory/02-pubsub.t:11-82` into `_test_pubsub_basic`, `_test_pubsub_exclude`, `_test_pubsub_full_drops`, `_test_pubsub_unsubscribe`, `_test_pubsub_idempotent`. Use the same factory-substitution rule. For `_test_pubsub_full_drops`, use `$factory->(capacity => 1)`.

- [ ] **Step 3: Port `_test_next_message`**

Port the seven subtests from `t/10-memory/09-next-message.t:14-110` into a single `_test_next_message($factory)` sub. Each subtest becomes a `subtest` inside this function. Preserve the `(async sub { ... })->()` patterns verbatim — they're correct for both backends.

- [ ] **Step 4: Port `_test_cleanup` and `_test_flush`**

Port the subtests from `t/10-memory/07-cleanup.t` *except* the presence-related and delayed-message-internal-state ones (those go in `_test_presence` and `_test_delayed`). Specifically port:
- `'cleanup removes channel from all groups'` (lines 11-26)
- `'cleanup removes pending messages'` (lines 28-37)
- `'cleanup removes pattern subscriptions'` (lines 39-47)

For `_test_flush`, port `'flush clears everything'` (lines 77-92) but **drop the `$backend->{groups}` / `$backend->{patterns}` / etc. internal-state assertions** — those reach into Memory's specific data structures. Replace them with observable-behavior assertions:

```perl
sub _test_flush {
    my ($factory) = @_;
    my $backend = $factory->(history_size => 10);

    _run { $backend->subscribe('ch1', 'room') };
    _run { $backend->psubscribe('ch2', 'events.*') };
    _run { $backend->send('ch1', { type => 'msg' }) };
    _run { $backend->publish('room', { type => 'msg' }) };

    _run { $backend->flush() };

    # Observable behavior after flush: queues empty, subscriptions gone
    is(_run { $backend->poll('ch1') }, undef, 'queues cleared');
    _run { $backend->publish('room', { type => 'after-flush' }) };
    is(_run { $backend->poll('ch1') }, undef, 'subscription cleared');
    _run { $backend->publish('events.test', { type => 'after-flush' }) };
    is(_run { $backend->poll('ch2') }, undef, 'pattern subscription cleared');
}
```

- [ ] **Step 5: Add `_test_validation`**

This is new — there's no existing equivalent. Codifies the validation contract from the design doc:

```perl
sub _test_validation {
    my ($factory) = @_;
    my $backend = $factory->();

    # Channel name validation
    like(
        dies { _run { $backend->send('', { type => 'x' }) } },
        qr/InvalidChannelName/,
        'empty channel name rejected'
    );
    like(
        dies { _run { $backend->send('a' x 101, { type => 'x' }) } },
        qr/InvalidChannelName/,
        'over-length channel name rejected'
    );
    like(
        dies { _run { $backend->send('bad name with spaces', { type => 'x' }) } },
        qr/InvalidChannelName/,
        'channel name with disallowed chars rejected'
    );

    # Message validation
    like(
        dies { _run { $backend->send('ch', 'not-a-hashref') } },
        qr/InvalidMessage/,
        'non-hashref message rejected'
    );
    like(
        dies { _run { $backend->send('ch', { no_type => 1 }) } },
        qr/InvalidMessage/,
        'message missing type rejected'
    );

    # Topic validation (subscribe and publish use the same rules)
    like(
        dies { _run { $backend->subscribe('ch', '') } },
        qr/InvalidChannelName/,
        'subscribe rejects empty topic'
    );
    like(
        dies { _run { $backend->publish('', { type => 'x' }) } },
        qr/InvalidChannelName/,
        'publish rejects empty topic'
    );
}
```

- [ ] **Step 6: Verify the module loads with the new content**

```bash
<perl-env> perl -Ilib -It/lib -MTest::PAGI::Channels::Contract -e 'print "loaded\n"'
```

- [ ] **Step 7: Commit**

```bash
git add t/lib/Test/PAGI/Channels/Contract.pm
git commit -m "$(cat <<'EOF'
test: port core/pubsub/cleanup/next_message tests into compliance suite

Adds a new _test_validation sub that codifies the channel/topic/message
validation contract — currently enforced by Memory but not Redis.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 1.3: Port presence tests

**Files:**
- Modify: `t/lib/Test/PAGI/Channels/Contract.pm` — replace `_test_presence` placeholder

- [ ] **Step 1: Port the ten subtests from `t/10-memory/04-presence.t:11-173`**

Substitute `$factory->()` for `PAGI::Middleware::Channels::Backend::Memory->new()`. Each existing `subtest` from the file becomes a `subtest` inside `_test_presence($factory)`. The `$backend->set_channel_id('...')` calls remain in Phase 1 — they will be removed in Phase 3 (Task 3.8) when `track`/`untrack` take channel explicitly.

Skeleton:

```perl
sub _test_presence {
    my ($factory) = @_;

    subtest 'explicit track/untrack' => sub {
        my $backend = $factory->();
        $backend->set_channel_id('worker.1');
        # ... port lines 12-25 verbatim ...
    };

    subtest 'subscribe with presence option' => sub {
        my $backend = $factory->();
        $backend->set_channel_id('user.alice');
        # ... port lines 28-39 ...
    };

    # Continue for all ten subtests in 04-presence.t
}
```

- [ ] **Step 2: Verify the module still loads**

```bash
<perl-env> perl -Ilib -It/lib -MTest::PAGI::Channels::Contract -e 'print "loaded\n"'
```

- [ ] **Step 3: Commit**

```bash
git add t/lib/Test/PAGI/Channels/Contract.pm
git commit -m "test: port presence tests into compliance suite

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.4: Port history tests

**Files:**
- Modify: `t/lib/Test/PAGI/Channels/Contract.pm` — replace `_test_history` placeholder

- [ ] **Step 1: Port the four subtests from `t/10-memory/06-history.t:11-78`**

Each `$backend = PAGI::Middleware::Channels::Backend::Memory->new(history_size => N)` becomes `$backend = $factory->(history_size => N)`.

- [ ] **Step 2: Verify the module loads**

```bash
<perl-env> perl -Ilib -It/lib -MTest::PAGI::Channels::Contract -e 'print "loaded\n"'
```

- [ ] **Step 3: Commit**

```bash
git add t/lib/Test/PAGI/Channels/Contract.pm
git commit -m "test: port history tests into compliance suite

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.5: Port delayed tests

**Files:**
- Modify: `t/lib/Test/PAGI/Channels/Contract.pm` — replace `_test_delayed` placeholder

- [ ] **Step 1: Port the four subtests from `t/10-memory/05-delayed.t:11-95`**

Mechanical port. Add `use Future::IO;` to the top of `Contract.pm` if not already present (the `Future::IO->sleep(...)` calls in the delayed tests need it).

Also port the delayed-cleanup subtest from `t/10-memory/07-cleanup.t:94-118`. Drop the `$backend->{delayed}` internal-state assertions (lines 104-105, 111-112) and replace with observable behavior:

```perl
subtest 'cleanup removes delayed messages targeting the channel' => sub {
    my $backend = $factory->();

    _run { $backend->send_delayed('ch1', { type => 'delayed1' }, 0.1) };
    _run { $backend->send_delayed('ch2', { type => 'delayed2' }, 0.1) };
    _run { $backend->publish_delayed('topic1', { type => 'pub' }, 0.1) };

    # Subscribe ch3 to topic1 so we can observe the publish-delayed survives
    _run { $backend->subscribe('ch3', 'topic1') };

    _run { $backend->cleanup('ch1') };

    # Wait past delay, pump
    _run { Future::IO->sleep(0.2) };
    _run { $backend->process_delayed() };

    is(_run { $backend->poll('ch1') }, undef, 'ch1 delayed removed by cleanup');
    is(_run { $backend->poll('ch2') }->{type}, 'delayed2', 'ch2 delayed survives');
    is(_run { $backend->poll('ch3') }->{type}, 'pub', 'publish_delayed survives');
};
```

- [ ] **Step 2: Verify the module loads**

```bash
<perl-env> perl -Ilib -It/lib -MTest::PAGI::Channels::Contract -e 'print "loaded\n"'
```

- [ ] **Step 3: Commit**

```bash
git add t/lib/Test/PAGI/Channels/Contract.pm
git commit -m "test: port delayed tests into compliance suite

Replaces internal-state assertions on \$backend->{delayed} with
observable behavior assertions, so the same tests work for backends
with different storage layouts.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.6: Port pattern subscription tests

**Files:**
- Modify: `t/lib/Test/PAGI/Channels/Contract.pm` — replace `_test_pattern_subs` placeholder

- [ ] **Step 1: Port the four subtests from `t/10-memory/03-patterns.t:11-67`**

Mechanical port. Same factory substitution.

- [ ] **Step 2: Verify the module loads**

```bash
<perl-env> perl -Ilib -It/lib -MTest::PAGI::Channels::Contract -e 'print "loaded\n"'
```

- [ ] **Step 3: Commit**

```bash
git add t/lib/Test/PAGI/Channels/Contract.pm
git commit -m "test: port pattern subscription tests into compliance suite

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.7: Wire Memory backend to compliance suite

**Files:**
- Create: `t/10-memory/00-contract.t`

- [ ] **Step 1: Write the wiring test**

```perl
# t/10-memory/00-contract.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop);
use Test::PAGI::Channels::Contract qw(run_contract_tests);
use PAGI::Middleware::Channels::Backend::Memory;
use Test2::V0;

init_loop();

run_contract_tests('Memory', sub {
    my (%args) = @_;
    return PAGI::Middleware::Channels::Backend::Memory->new(%args);
});

done_testing;
```

- [ ] **Step 2: Run it**

```bash
<perl-env> prove -lv t/10-memory/00-contract.t
```

Expected: ALL subtests PASS. The validation tests will pass (Memory enforces validation already). If anything else fails, the port in Tasks 1.2-1.6 is wrong; fix it before continuing.

- [ ] **Step 3: Commit**

```bash
git add t/10-memory/00-contract.t
git commit -m "test: wire Memory backend to compliance suite

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.8: Wire Redis backend to compliance suite

**Files:**
- Create: `t/30-redis/00-contract.t`

- [ ] **Step 1: Write the wiring test**

```perl
# t/30-redis/00-contract.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop skip_without_redis make_redis);
use Test::PAGI::Channels::Contract qw(run_contract_tests);
use Test2::V0;

init_loop();

SKIP: {
    skip_without_redis();

    require PAGI::Middleware::Channels::Backend::Redis;

    my $counter = 0;
    run_contract_tests('Redis', sub {
        my (%args) = @_;
        # Use a per-instance prefix so capacity-overriding tests with
        # different config get isolated keyspaces.
        $counter++;
        my $redis = make_redis(prefix => "test:contract:$$:$counter:");
        my $backend = PAGI::Middleware::Channels::Backend::Redis->new(
            redis => $redis,
            %args,
        );
        $backend->flush->get;
        return $backend;
    });
}

done_testing;
```

- [ ] **Step 2: Make sure Redis is running**

```bash
cd t && docker compose up -d && cd ..
```

- [ ] **Step 3: Run it**

```bash
<perl-env> REDIS_HOST=localhost prove -lv t/30-redis/00-contract.t
```

Expected: most subtests PASS, but the **validation subtests in `_test_validation` FAIL** (Redis doesn't enforce validation). Capture the failures — they prove the drift bug exists. Phase 2 Task 2.5 fixes them.

- [ ] **Step 4: Mark validation as expected-to-fail temporarily**

In `t/lib/Test/PAGI/Channels/Contract.pm`, wrap the body of `_test_validation` in a `todo` block guarded by an env var so the suite stays green for now:

```perl
sub _test_validation {
    my ($factory) = @_;

    if ($ENV{PAGI_CONTRACT_TODO_VALIDATION}) {
        todo "Backend does not yet enforce validation (fixed in Phase 2 Task 2.5)" => sub {
            _run_validation_assertions($factory);
        };
    } else {
        _run_validation_assertions($factory);
    }
}

sub _run_validation_assertions {
    my ($factory) = @_;
    my $backend = $factory->();
    # ... move the existing _test_validation body here ...
}
```

Then update `t/30-redis/00-contract.t` to set `$ENV{PAGI_CONTRACT_TODO_VALIDATION} = 1` before calling `run_contract_tests`. Add a comment in the Redis file:

```perl
# Validation is not yet enforced by the Redis backend. Phase 2 Task 2.5
# adds it; this env var marks the failures as TODO until then.
$ENV{PAGI_CONTRACT_TODO_VALIDATION} = 1;
```

- [ ] **Step 5: Re-run both contract tests, all green**

```bash
<perl-env> prove -lv t/10-memory/00-contract.t
<perl-env> REDIS_HOST=localhost prove -lv t/30-redis/00-contract.t
```

Both files: all subtests PASS (Redis's validation as TODO).

- [ ] **Step 6: Commit**

```bash
git add t/lib/Test/PAGI/Channels/Contract.pm t/30-redis/00-contract.t
git commit -m "$(cat <<'EOF'
test: wire Redis backend to compliance suite

Validation tests are TODO-gated for Redis pending Phase 2 Task 2.5,
which adds the validation calls to the Redis backend's entry methods.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 1.9: Trim now-redundant per-backend test files

**Files:**
- Delete: `t/10-memory/01-core.t`, `t/10-memory/02-pubsub.t`, `t/10-memory/03-patterns.t`, `t/10-memory/04-presence.t`, `t/10-memory/05-delayed.t`, `t/10-memory/06-history.t`, `t/10-memory/09-next-message.t`
- Delete: `t/30-redis/01-core.t`, `t/30-redis/02-pubsub.t`, `t/30-redis/03-patterns.t`, `t/30-redis/04-presence.t`, `t/30-redis/05-delayed.t`, `t/30-redis/06-history.t`, `t/30-redis/07-next-message.t`
- Modify: `t/10-memory/07-cleanup.t` — keep only Memory-internal-state subtests if any (e.g., the flush test that pokes `$backend->{groups}`)
- Keep: `t/10-memory/08-abstract-backend.t` (updated in Task 3.10)
- Keep: `t/30-redis/00-contract.t` (Task 1.8)
- Keep: any Redis backend-specific tests like the prefix-isolation subtest that lives in `t/30-redis/01-core.t:16-39` — extract it to a new file `t/30-redis/01-redis-specific.t` before deleting `01-core.t`

- [ ] **Step 1: Extract Redis-specific tests before deletion**

The test "backend uses the passed Async::Redis instance" at `t/30-redis/01-core.t:16-39` is Redis-specific (it tests prefix isolation across Async::Redis instances). Move it to a new file `t/30-redis/01-redis-specific.t`:

```perl
# t/30-redis/01-redis-specific.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run skip_without_redis make_redis);
use Test2::V0;

init_loop();

SKIP: {
    skip_without_redis();
    require PAGI::Middleware::Channels::Backend::Redis;

    subtest 'backend uses the passed Async::Redis instance' => sub {
        # ... port lines 17-38 verbatim ...
    };
}

done_testing;
```

- [ ] **Step 2: Audit `t/10-memory/07-cleanup.t`**

Check the file for assertions that reach into Memory's internal state (`$backend->{groups}`, etc.). Those subtests are Memory-implementation-specific and should remain. The functional behavior is already covered by the contract suite. If after extraction nothing remains, delete the file. If only the flush-internal-state subtest remains, rename the file to `t/10-memory/01-memory-internals.t`.

- [ ] **Step 3: Delete the redundant per-backend files**

```bash
rm t/10-memory/01-core.t t/10-memory/02-pubsub.t t/10-memory/03-patterns.t \
   t/10-memory/04-presence.t t/10-memory/05-delayed.t t/10-memory/06-history.t \
   t/10-memory/09-next-message.t
rm t/30-redis/01-core.t t/30-redis/02-pubsub.t t/30-redis/03-patterns.t \
   t/30-redis/04-presence.t t/30-redis/05-delayed.t t/30-redis/06-history.t \
   t/30-redis/07-next-message.t
```

- [ ] **Step 4: Run the full test suite**

```bash
<perl-env> REDIS_HOST=localhost prove -lr t/
```

Expected: all green. Test count is much lower; coverage is the same.

- [ ] **Step 5: Commit**

```bash
git add -A t/
git commit -m "$(cat <<'EOF'
test: remove per-backend duplicates now covered by compliance suite

Extracts Redis-specific prefix-isolation test to t/30-redis/01-redis-specific.t.
Audits Memory cleanup test, keeping only Memory-internals assertions.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

**Phase 1 done.** Compliance suite covers the full contract; both backends pass it (Redis with one TODO). Net change: ~1700 lines of duplicated test code reduced to ~600 lines of shared contract + ~30 lines per-backend wiring.

---

## Phase 2: Populate the Base Class (no public API change)

**Objective:** Move shared code (validation, constants, utilities) from each backend into `PAGI::Middleware::Channels::Backend`. Both backends call `SUPER::new` and inherit the helpers. Add validation to Redis, closing the drift bug captured by the Phase 1 TODO.

**Why second:** It eliminates the verbatim duplication that Phase 3's role split would otherwise have to deal with. After this phase, the backends are leaner and easier to refactor for roles.

### Task 2.1: Move constants and constructor to base class

**Files:**
- Modify: `lib/PAGI/Middleware/Channels/Backend.pm` — add constants, fill `new`
- Modify: `lib/PAGI/Middleware/Channels/Backend/Memory.pm` — call `SUPER::new`, drop duplicates
- Modify: `lib/PAGI/Middleware/Channels/Backend/Redis.pm` — call `SUPER::new`, drop duplicates

- [ ] **Step 1: Run the contract suite (baseline)**

```bash
<perl-env> REDIS_HOST=localhost prove -lr t/
```

Note the test count and the green status. Phase 2 must keep all of these green.

- [ ] **Step 2: Add constants and constructor to `Backend.pm`**

Modify `lib/PAGI/Middleware/Channels/Backend.pm`. Replace the existing `new` (which currently only croaks on direct instantiation) with:

```perl
package PAGI::Middleware::Channels::Backend;
use strict;
use warnings;
use Carp ();

# Shared default configuration values. Subclasses inherit these.
use constant {
    DEFAULT_CAPACITY     => 100,
    DEFAULT_EXPIRY       => 60,
    DEFAULT_GROUP_EXPIRY => 86400,
    DEFAULT_MAX_SIZE     => 1_048_576,
    DEFAULT_HISTORY_SIZE => 0,
};

sub new {
    my ($class, %args) = @_;
    Carp::croak("$class is abstract and cannot be instantiated directly")
        if $class eq __PACKAGE__;

    return bless {
        capacity     => $args{capacity}     // DEFAULT_CAPACITY,
        expiry       => $args{expiry}       // DEFAULT_EXPIRY,
        group_expiry => $args{group_expiry} // DEFAULT_GROUP_EXPIRY,
        max_size     => $args{max_size}     // DEFAULT_MAX_SIZE,
        history_size => $args{history_size} // DEFAULT_HISTORY_SIZE,
        # subclass-specific args remain in %args; subclasses store what they need
        %args,
    }, $class;
}

# Existing abstract method list and croak loop remain below this point.
my @ABSTRACT = qw(
    send poll subscribe unsubscribe publish flush cleanup
    psubscribe punsubscribe track untrack list_presence
    count_presence scan_presence
    send_delayed publish_delayed subscribe_with_history
    next_message
);

for my $method (@ABSTRACT) {
    no strict 'refs';
    *{__PACKAGE__ . "::$method"} = sub {
        my $self = shift;
        my $class = ref($self) || $self;
        Carp::croak("$class must implement abstract method '$method'");
    };
}

1;
```

POD update: in `Backend.pm`, add a `=head1 SHARED CONFIGURATION` section listing the five config keys and their defaults.

- [ ] **Step 3: Update `Memory.pm` to use the base constructor**

Modify `lib/PAGI/Middleware/Channels/Backend/Memory.pm`. Replace the `use constant` block (lines 12-18) and the `sub new` (lines 20-45) with:

```perl
sub new {
    my ($class, %args) = @_;
    my $self = $class->SUPER::new(%args);

    # Memory-specific state initialization
    $self->{queues}      = {};
    $self->{groups}      = {};
    $self->{patterns}    = {};
    $self->{presence}    = {};
    $self->{history}     = {};
    $self->{delayed}     = [];
    $self->{_waiters}    = {};
    $self->{_channel_id} = undef;

    return $self;
}
```

Delete the `use constant` block — it's now in the base.

- [ ] **Step 4: Update `Redis.pm` to use the base constructor**

Modify `lib/PAGI/Middleware/Channels/Backend/Redis.pm`. Replace the `use constant` block (lines 14-20) and the `sub new` (lines 22-46) with:

```perl
sub new {
    my ($class, %args) = @_;

    my $redis = $args{redis}
        or die "PAGI::Middleware::Channels::Backend::Redis: 'redis' argument required "
             . "(Async::Redis instance or compatible)";

    my $self = $class->SUPER::new(%args);

    # Redis-specific state
    $self->{_redis}          = $redis;
    $self->{_channel_id}     = undef;
    $self->{_subscriber}     = undef;
    $self->{_subscription}   = undef;
    $self->{_listener_f}     = undef;
    $self->{_waiters}        = {};
    $self->{_active_fs}      = {};
    $self->{_notify_poll_fs} = [];

    return $self;
}
```

Delete the `use constant` block.

- [ ] **Step 5: Run the contract suite**

```bash
<perl-env> REDIS_HOST=localhost prove -lr t/
```

Expected: all green, same count as the baseline.

- [ ] **Step 6: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend.pm \
        lib/PAGI/Middleware/Channels/Backend/Memory.pm \
        lib/PAGI/Middleware/Channels/Backend/Redis.pm
git commit -m "$(cat <<'EOF'
refactor: move shared constants and constructor defaults to Backend base

Backend.pm now defines DEFAULT_CAPACITY, DEFAULT_EXPIRY, etc., and a
SUPER::new that handles common config dispatch. Memory and Redis backends
call SUPER::new and only initialize their backend-specific state.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2.2: Move validation methods to base class

**Files:**
- Modify: `lib/PAGI/Middleware/Channels/Backend.pm` — add `_validate_*` methods
- Modify: `lib/PAGI/Middleware/Channels/Backend/Memory.pm` — delete duplicated `_validate_*`

- [ ] **Step 1: Add validation methods to `Backend.pm`**

Insert immediately after the `new` method:

```perl
# Validation helpers. Backends MUST call these from their public entry
# methods. A backend with stricter substrate constraints (e.g., a Pusher
# backend with its own naming rules) MAY override _validate_channel to
# add more checks; it MUST NOT loosen them.

sub _validate_channel {
    my ($self, $channel) = @_;
    die "InvalidChannelName: empty"     unless defined $channel && length $channel;
    die "InvalidChannelName: too long"  if length $channel > 100;
    die "InvalidChannelName: bad chars" unless $channel =~ /^[\w.\-:]+$/;
}

# Topics use the same naming rules as channels.
sub _validate_topic { goto &_validate_channel }

sub _validate_message {
    my ($self, $message) = @_;
    die "InvalidMessage: not a hashref" unless ref $message eq 'HASH';
    die "InvalidMessage: missing type"  unless defined $message->{type};
}
```

- [ ] **Step 2: Delete duplicated `_validate_*` from `Memory.pm`**

Remove `_validate_channel` and `_validate_message` from `lib/PAGI/Middleware/Channels/Backend/Memory.pm` (current lines 94-107). They're inherited from the base now.

- [ ] **Step 3: Run the contract suite**

```bash
<perl-env> REDIS_HOST=localhost prove -lr t/
```

Expected: all green, same count.

- [ ] **Step 4: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend.pm \
        lib/PAGI/Middleware/Channels/Backend/Memory.pm
git commit -m "refactor: move validation methods to Backend base class

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 2.3: Move shared utilities (`_pattern_to_regex`, `_normalize_exclude`, `_make_presence_event`)

**Files:**
- Modify: `lib/PAGI/Middleware/Channels/Backend.pm` — add the three helpers
- Modify: `lib/PAGI/Middleware/Channels/Backend/Memory.pm` — delete `_pattern_to_regex`; refactor `publish` to use `_normalize_exclude`; refactor presence-event hashref construction
- Modify: `lib/PAGI/Middleware/Channels/Backend/Redis.pm` — same

- [ ] **Step 1: Add the three helpers to `Backend.pm`**

```perl
# Glob-pattern to regex compilation. Used by PatternSubs-capable backends
# in both psubscribe (compile) and publish (match). Lives on the base so
# any backend can call it without depending on the PatternSubs role.
#
# Pattern syntax:
#   *  matches exactly one segment (no dots)
#   ** matches zero or more segments (including dots)
#   When ** follows a dot, the dot is optional, so "foo.**" matches
#   "foo", "foo.bar", "foo.bar.baz".
sub _pattern_to_regex {
    my ($self, $pattern) = @_;
    my $regex = quotemeta($pattern);
    $regex =~ s/\\\.\\\*\\\*/(\\..*)?\$/g;   # ".**" - dot optional
    $regex =~ s/\\\*\\\*/.*/g;               # "**" anywhere else
    $regex =~ s/\\\*/[^.]+/g;                # "*" - one segment
    return qr/^$regex$/;
}

# Normalize the publish-time `exclude` option to a hash for fast lookup.
# Accepts undef, scalar, or arrayref.
sub _normalize_exclude {
    my ($self, $exclude) = @_;
    return {} unless defined $exclude;
    $exclude = [$exclude] unless ref $exclude eq 'ARRAY';
    return { map { $_ => 1 } @$exclude };
}

# Construct the standard presence-event hashref used in subscribe-with-presence
# and unsubscribe-with-presence broadcasts.
sub _make_presence_event {
    my ($self, $topic, $type, $presence_data) = @_;
    return {
        type     => $type,             # 'presence.join' or 'presence.leave'
        topic    => $topic,
        presence => $presence_data,
    };
}
```

- [ ] **Step 2: Delete `_pattern_to_regex` from `Memory.pm` and `Redis.pm`**

Remove the `_pattern_to_regex` sub from both files (Memory: lines 124-143; Redis: lines 137-155). Inherited now.

- [ ] **Step 3: Refactor `publish` in `Memory.pm` to use `_normalize_exclude`**

In `Memory.pm`'s `publish` (currently around lines 226-278), replace these lines:

```perl
my $exclude = $opts{exclude} // [];
$exclude = [$exclude] unless ref $exclude eq 'ARRAY';
my %excluded = map { $_ => 1 } @$exclude;
```

with:

```perl
my %excluded = %{ $self->_normalize_exclude($opts{exclude}) };
```

- [ ] **Step 4: Same refactor in `Redis.pm`**

In `Redis.pm`'s `publish` (currently around lines 336-391), replace the same three lines with the same single-line `_normalize_exclude` call.

- [ ] **Step 5: Refactor `Memory.pm` `_broadcast_presence_event` to use `_make_presence_event`**

In `Memory.pm` (currently lines 197-208):

```perl
async sub _broadcast_presence_event {
    my ($self, $topic, $event_type, $presence_data, $exclude_channel) = @_;
    my $event = $self->_make_presence_event($topic, $event_type, $presence_data);
    await $self->publish($topic, $event, exclude => $exclude_channel);
}
```

- [ ] **Step 6: Refactor `Redis.pm` to use `_make_presence_event`**

In `Redis.pm`, the presence-event hashrefs are constructed inline in `subscribe`, `unsubscribe`, and `cleanup`. Replace each of these inline constructions:

```perl
{
    type     => 'presence.join',  # or 'presence.leave'
    topic    => $topic,
    presence => $presence_data,
}
```

with `$self->_make_presence_event($topic, 'presence.join', $presence_data)` (or `'presence.leave'` as appropriate). Three sites in `Redis.pm`.

- [ ] **Step 7: Run the contract suite**

```bash
<perl-env> REDIS_HOST=localhost prove -lr t/
```

Expected: all green, same count.

- [ ] **Step 8: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend.pm \
        lib/PAGI/Middleware/Channels/Backend/Memory.pm \
        lib/PAGI/Middleware/Channels/Backend/Redis.pm
git commit -m "$(cat <<'EOF'
refactor: move _pattern_to_regex, _normalize_exclude, _make_presence_event to base

Both backends were carrying verbatim copies of the pattern-to-regex
utility, exclude-list normalization, and presence-event hashref
construction. Consolidate to the base class so capability roles in
Phase 3 inherit them too.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2.4: Add validation calls to Redis (close the drift bug) + max_size enforcement

**Files:**
- Modify: `lib/PAGI/Middleware/Channels/Backend.pm` — add `max_size` enforcement to `_validate_message`
- Modify: `lib/PAGI/Middleware/Channels/Backend/Redis.pm` — add `_validate_*` calls to `send`, `subscribe`, `unsubscribe`, `publish`, `psubscribe`, `punsubscribe`, `track`, `untrack`, `list_presence`, `count_presence`, `scan_presence`, `send_delayed`, `publish_delayed`, `subscribe_with_history`
- Modify: `t/30-redis/00-contract.t` — remove the `PAGI_CONTRACT_TODO_VALIDATION` env var

- [ ] **Step 1: Add `max_size` enforcement to `_validate_message`**

In `lib/PAGI/Middleware/Channels/Backend.pm`, expand `_validate_message`:

```perl
sub _validate_message {
    my ($self, $message) = @_;
    die "InvalidMessage: not a hashref" unless ref $message eq 'HASH';
    die "InvalidMessage: missing type"  unless defined $message->{type};

    if ($self->{max_size}) {
        require JSON::MaybeXS;
        my $size = length(JSON::MaybeXS::encode_json($message));
        die "MessageTooLarge: $size bytes exceeds max_size $self->{max_size}"
            if $size > $self->{max_size};
    }
}
```

- [ ] **Step 2: Add a `MessageTooLarge` test to the compliance suite**

In `t/lib/Test/PAGI/Channels/Contract.pm`, in `_run_validation_assertions`, add:

```perl
my $tiny = $factory->(max_size => 100);
like(
    dies { _run { $tiny->send('ch', { type => 'big', payload => 'x' x 1000 }) } },
    qr/MessageTooLarge/,
    'oversized message rejected'
);
```

- [ ] **Step 3: Add validation calls to Redis backend's entry methods**

In `lib/PAGI/Middleware/Channels/Backend/Redis.pm`, add validation as the first action of each public method that takes a channel/topic/message:

```perl
async sub send {
    my ($self, $channel, $message) = @_;
    $self->_validate_channel($channel);
    $self->_validate_message($message);
    # ... existing body ...
}

async sub subscribe {
    my ($self, $channel, $topic, %opts) = @_;
    $self->_validate_channel($channel);
    $self->_validate_topic($topic);
    # ... existing body ...
}

async sub unsubscribe {
    my ($self, $channel, $topic) = @_;
    $self->_validate_channel($channel);
    $self->_validate_topic($topic);
    # ... existing body ...
}

async sub publish {
    my ($self, $topic, $message, %opts) = @_;
    $self->_validate_topic($topic);
    $self->_validate_message($message);
    # ... existing body ...
}

async sub psubscribe {
    my ($self, $channel, $pattern) = @_;
    $self->_validate_channel($channel);
    # patterns themselves are not channel-named (they contain * and **),
    # so $pattern is not validated as a channel name.
    # ... existing body ...
}

async sub punsubscribe {
    my ($self, $channel, $pattern) = @_;
    $self->_validate_channel($channel);
    # ... existing body ...
}

async sub track {
    my ($self, $topic, $presence_data, $channel) = @_;
    $self->_validate_topic($topic);
    # ... existing body ...
}

async sub untrack {
    my ($self, $topic) = @_;
    $self->_validate_topic($topic);
    # ... existing body ...
}

async sub list_presence {
    my ($self, $topic, %opts) = @_;
    $self->_validate_topic($topic);
    # ... existing body ...
}

async sub count_presence {
    my ($self, $topic) = @_;
    $self->_validate_topic($topic);
    # ... existing body ...
}

async sub scan_presence {
    my ($self, $topic, %opts) = @_;
    $self->_validate_topic($topic);
    # ... existing body ...
}

async sub send_delayed {
    my ($self, $channel, $message, $delay_seconds) = @_;
    $self->_validate_channel($channel);
    $self->_validate_message($message);
    # ... existing body ...
}

async sub publish_delayed {
    my ($self, $topic, $message, $delay_seconds) = @_;
    $self->_validate_topic($topic);
    $self->_validate_message($message);
    # ... existing body ...
}

async sub subscribe_with_history {
    my ($self, $channel, $topic, $count, %opts) = @_;
    $self->_validate_channel($channel);
    $self->_validate_topic($topic);
    # ... existing body ...
}
```

- [ ] **Step 4: Remove the TODO env var from `t/30-redis/00-contract.t`**

Delete the line `$ENV{PAGI_CONTRACT_TODO_VALIDATION} = 1;` and the comment above it.

- [ ] **Step 5: Run the contract suite**

```bash
<perl-env> REDIS_HOST=localhost prove -lr t/
```

Expected: all green. The Redis validation tests now pass cleanly (no TODO).

- [ ] **Step 6: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend.pm \
        lib/PAGI/Middleware/Channels/Backend/Redis.pm \
        t/lib/Test/PAGI/Channels/Contract.pm \
        t/30-redis/00-contract.t
git commit -m "$(cat <<'EOF'
fix: enforce channel/topic/message validation in Redis backend

The Redis backend silently accepted invalid channel names and malformed
messages because validation calls were never wired in. Memory enforced
the same rules; the divergence was a latent contract violation surfaced
by the new compliance suite.

Also adds max_size enforcement to _validate_message and a corresponding
MessageTooLarge compliance test.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 2.4a: Fix Redis capacity-overflow rejection

**Files:**
- Modify: `lib/PAGI/Middleware/Channels/Backend/Redis.pm` (around line 187 in the `send` method)
- Modify: `t/30-redis/00-contract.t` — remove `$ENV{PAGI_CONTRACT_TODO_CAPACITY_OVERFLOW} = 1;`

**Background:** Memory rejects `send` to a full channel by `await Future->fail('ChannelFull', ...)`. Redis returns `Future->fail('ChannelFull', ...)` directly without `await`, so the caller receives the Future object instead of having the failure propagate through async semantics. The compliance test `_test_core_capacity` is currently TODO-gated; flipping the env var off should make it pass.

- [ ] **Step 1: Confirm the test fails again with env var off**

Edit `t/30-redis/00-contract.t` and comment out the `PAGI_CONTRACT_TODO_CAPACITY_OVERFLOW` line. Run:

```bash
<perl-env> REDIS_HOST=localhost prove -lv t/30-redis/00-contract.t
```

Expected: `_test_core_capacity` fails with "Not a HASH reference".

- [ ] **Step 2: Fix the Redis `send` method**

In `lib/PAGI/Middleware/Channels/Backend/Redis.pm`, find the capacity check in `send` (around line 187). Change:

```perl
return Future->fail('ChannelFull', 'PAGI::Channels');
```

to:

```perl
await Future->fail('ChannelFull', 'PAGI::Channels');
```

- [ ] **Step 3: Verify the test now passes**

```bash
<perl-env> REDIS_HOST=localhost prove -lv t/30-redis/00-contract.t
```

Expected: `_test_core_capacity` PASSES.

- [ ] **Step 4: Delete the TODO env var line entirely from `t/30-redis/00-contract.t`**

- [ ] **Step 5: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend/Redis.pm t/30-redis/00-contract.t
git commit -m "$(cat <<'EOF'
fix(redis): propagate ChannelFull as rejected Future from send()

The capacity-overflow path returned Future->fail(...) directly inside
an async sub, so the caller received the Future object rather than
having its failure propagate through async semantics. await it instead.

Removes the corresponding PAGI_CONTRACT_TODO_CAPACITY_OVERFLOW gate.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2.4b: Fix Redis next_message cancel cleanup

**Files:**
- Modify: `lib/PAGI/Middleware/Channels/Backend/Redis.pm` (the `next_message` method's `on_cancel` handler around lines 239-243)
- Modify: `t/30-redis/00-contract.t` — remove `$ENV{PAGI_CONTRACT_TODO_NEXT_MESSAGE_CANCEL} = 1;`

**Background:** Cancelling a `next_message` Future leaves the Redis subscriber in a stale state. The current `on_cancel` handler removes the cancelled signal future from `_waiters` but doesn't unsubscribe from the underlying Redis pub/sub when no more waiters remain. The next `send`+`next_message` round trip never receives the notification and hangs until `read_timeout`.

- [ ] **Step 1: Confirm test fails with env var off**

Comment out `PAGI_CONTRACT_TODO_NEXT_MESSAGE_CANCEL` in the Redis driver. Run the contract — `'next_message works after prior cancel'` should hang/timeout.

- [ ] **Step 2: Audit the `next_message` + subscriber bookkeeping in `Redis.pm`**

Read the `next_message` method (around line 223), `_start_listener` (around line 123), and any helper that maintains the subscriber connection. Decide between two fix approaches before writing code:

(a) **Reference-count + unsubscribe.** When `on_cancel` fires and `_waiters{$channel}` is now empty, call the appropriate UNSUBSCRIBE/PUNSUBSCRIBE on the subscriber connection so a fresh subscription is created on the next call.

(b) **Keep subscriber, fix the notify path.** Investigate why a cancelled waiter prevents subsequent waiters from being notified — there may be a stale waiter reference in the listener or the listener may have stopped consuming messages.

Option (a) is more localized; option (b) requires understanding the listener loop. Pick (a) unless inspection shows the listener has its own bug.

- [ ] **Step 3: Implement the fix**

Adjust the `on_cancel` closure to do the right cleanup. Make sure the cleanup is idempotent (multiple cancels of the same channel shouldn't cause double-unsubscribe).

- [ ] **Step 4: Verify**

Both contract drivers must stay green. Specifically:

```bash
<perl-env> REDIS_HOST=localhost prove -lv t/30-redis/00-contract.t
```

`'next_message works after prior cancel'` PASSES. No other subtests regress.

- [ ] **Step 5: Delete the TODO env var line**

- [ ] **Step 6: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend/Redis.pm t/30-redis/00-contract.t
git commit -m "$(cat <<'EOF'
fix(redis): clean up subscriber state when next_message is cancelled

Cancelling a next_message Future left the Redis subscriber connection
holding a stale subscription, so the next send+next_message round trip
never received the notification. The on_cancel handler now unsubscribes
when no more waiters remain on a channel.

Removes the corresponding PAGI_CONTRACT_TODO_NEXT_MESSAGE_CANCEL gate.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2.4c: Fix Redis cleanup of pending delayed messages

**Files:**
- Modify: `lib/PAGI/Middleware/Channels/Backend/Redis.pm` (the `cleanup` method around lines 424-473; the delayed-message storage at `_delayed_key`)
- Modify: `t/30-redis/00-contract.t` — remove `$ENV{PAGI_CONTRACT_TODO_DELAYED_CLEANUP} = 1;`

**Background:** Memory's `cleanup($channel)` filters out delayed entries whose `target` matches `$channel`. Redis's `cleanup` never touches the delayed ZSET, so delayed messages targeting a cleaned-up channel still deliver after the delay. The compliance test `'cleanup removes delayed messages targeting the channel'` proves this.

- [ ] **Step 1: Confirm test fails with env var off**

Comment out `PAGI_CONTRACT_TODO_DELAYED_CLEANUP` in the Redis driver, run the contract — the cleanup-delayed subtest fails (ch1 still polls a message).

- [ ] **Step 2: Audit the delayed-store schema**

Read `send_delayed`, `publish_delayed`, and `process_delayed` in `Redis.pm`. The delayed store is a sorted set at `<prefix>:_delayed` keyed by JSON payloads, scored by delivery timestamp. Each payload includes a `target` field (channel name for `send_delayed`, topic for `publish_delayed`) and a `kind` discriminator.

- [ ] **Step 3: Decide schema vs. iterate**

Option (a) — **iterate-and-filter** (small fix): inside `cleanup($channel)`, `ZRANGE _delayed_key 0 -1`, decode each entry, ZREM the ones with `kind = 'send'` and `target eq $channel`. O(n) per cleanup but simple. Use this unless step 4 reveals a problem.

Option (b) — **schema redesign** (larger): split the delayed store into per-channel ZSETs (`<prefix>:_delayed:send:<channel>`) plus a per-topic store. Cleanup becomes a single DEL. Defer to a follow-up task if perf becomes an issue.

- [ ] **Step 4: Implement option (a)**

Add a helper or inline block to `cleanup` that iterates the delayed ZSET, filters by `kind eq 'send' && target eq $channel`, and ZREMs matching entries. Take care not to remove `kind eq 'publish'` entries — those target topics, not channels.

- [ ] **Step 5: Verify**

```bash
<perl-env> REDIS_HOST=localhost prove -lv t/30-redis/00-contract.t
```

`'cleanup removes delayed messages targeting the channel'` PASSES. No other tests regress (especially the delayed subtests that rely on `process_delayed`).

- [ ] **Step 6: Delete the TODO env var line**

- [ ] **Step 7: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend/Redis.pm t/30-redis/00-contract.t
git commit -m "$(cat <<'EOF'
fix(redis): sweep pending delayed messages in cleanup()

cleanup($channel) previously left scheduled send_delayed entries in the
ZSET, so a cleaned-up channel still received its delayed message after
the timer expired. cleanup now scans the delayed store and removes
entries whose kind=send and target matches the channel. publish_delayed
entries (which target topics) are preserved.

Removes the corresponding PAGI_CONTRACT_TODO_DELAYED_CLEANUP gate.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

**Phase 2 done.** Base class owns shared code; both backends use it; the validation drift bug AND the three Redis-backend bugs surfaced by the compliance suite are fixed. After Tasks 2.4 / 2.4a / 2.4b / 2.4c land, all four `PAGI_CONTRACT_TODO_*` gates should be removed from `t/30-redis/00-contract.t`.

---

## Phase 3: Capability Roles + `set_channel_id` Removal

**Objective:** Split the optional features (presence, history, delayed, pattern subs) into `Role::Tiny` capability roles. Backends declare what they support via `with`. Facade gates capability calls. Remove the racy `set_channel_id` mutable state by passing `channel_name` explicitly.

**Why last:** Phase 2 removed the verbatim duplication that would have made this messy. Phase 1's compliance suite catches any regression in the capability behaviors. After this phase, adding the PostgreSQL backend (out of scope here) is a pure additive task.

### Task 3.1: Add `Role::Tiny` dependency

**Files:**
- Modify: `cpanfile`

- [ ] **Step 1: Inspect existing `cpanfile`**

```bash
<perl-env> cat cpanfile
```

- [ ] **Step 2: Add `Role::Tiny` as a runtime dep**

Add to the runtime requires section (or create one if absent):

```perl
requires 'Role::Tiny', '2.000000';
```

- [ ] **Step 3: Install it**

```bash
<perl-env> cpanm --installdeps .
```

- [ ] **Step 4: Verify**

```bash
<perl-env> perl -MRole::Tiny -e 'print "$Role::Tiny::VERSION\n"'
```

Expected: a version >= 2.000000.

- [ ] **Step 5: Commit**

```bash
git add cpanfile
git commit -m "chore: add Role::Tiny dependency for capability roles

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3.2: Create `Role::Presence`

**Files:**
- Create: `lib/PAGI/Middleware/Channels/Backend/Role/Presence.pm`

- [ ] **Step 1: Write the role**

```perl
package PAGI::Middleware::Channels::Backend::Role::Presence;
use strict;
use warnings;
use Role::Tiny;
use Future::AsyncAwait;
use Carp ();

# A backend that does this role MUST implement these five methods.
requires qw(
    track
    untrack
    list_presence
    count_presence
    scan_presence
);

# track($topic, $channel, $presence_data) -> Future
#   Record that $channel is present on $topic with $presence_data (hashref).
#   Idempotent. Channel is passed explicitly (no hidden mutable state).
#
# untrack($topic, $channel) -> Future
#   Remove $channel's presence entry from $topic.
#
# list_presence($topic, %opts) -> Future(@presence_hashrefs)
#   Honors %opts{limit} - croak if exceeded with a "use scan_presence" hint.
#
# count_presence($topic) -> Future($integer)
#   MUST be O(1)-ish at the storage layer.
#
# scan_presence($topic, cursor => N, count => M) -> Future(($next, @batch))
#   Cursor-based iteration. Returns 0 as $next when iteration complete.

# Wraps cleanup so that when a channel is torn down, its presence entries
# are removed and presence.leave events broadcast on every topic where it
# was tracked. Backends MUST implement _presence_topics_for_channel.
requires '_presence_topics_for_channel';

# _presence_topics_for_channel($channel) -> Future(@[$topic, $presence_data])
#   Returns list of [$topic, $presence_data] pairs for every topic where
#   $channel currently has a presence entry. Used by the around-cleanup
#   hook below.

around cleanup => async sub {
    my ($orig, $self, $channel) = @_;
    my @topics_data = await $self->_presence_topics_for_channel($channel);
    for my $pair (@topics_data) {
        my ($topic, $presence_data) = @$pair;
        await $self->untrack($topic, $channel);
        await $self->publish(
            $topic,
            $self->_make_presence_event($topic, 'presence.leave', $presence_data),
            exclude => $channel,
        );
    }
    return await $self->$orig($channel);
};

1;

__END__

=head1 NAME

PAGI::Middleware::Channels::Backend::Role::Presence - Optional presence tracking capability

=head1 DESCRIPTION

A backend that supports presence tracking C<with>'s this role. Users access
presence through L<PAGI::Channel>'s C<track>, C<untrack>, C<list_presence>,
C<count_presence>, and C<scan_presence> methods. The facade checks for
this capability via C<< $backend->does(...) >> and croaks with a clear
"capability not supported" message if absent.

=head1 REQUIRED METHODS

See the comment-block in the source for the contract of each required method.

=cut
```

- [ ] **Step 2: Verify it loads**

```bash
<perl-env> perl -Ilib -MPAGI::Middleware::Channels::Backend::Role::Presence -e 'print "loaded\n"'
```

- [ ] **Step 3: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend/Role/Presence.pm
git commit -m "feat: add Role::Presence capability role

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3.3: Create `Role::History`

**Files:**
- Create: `lib/PAGI/Middleware/Channels/Backend/Role/History.pm`

- [ ] **Step 1: Write the role**

```perl
package PAGI::Middleware::Channels::Backend::Role::History;
use strict;
use warnings;
use Role::Tiny;
use Future::AsyncAwait;

requires qw(
    _record_history
    read_history
    _deliver
);

# _record_history($topic, $message) -> Future
#   Called by the publish hook (see below). MUST skip messages whose type
#   matches /^presence\./ - those are ephemeral and not history.
#   MUST trim to $self->{history_size} entries.
#
# read_history($topic, $count) -> Future(@messages)
#   Most recent $count messages, oldest first. Empty list if no history.

# Override the no-op hook in the base so publish records to history.
sub _record_history_hook {
    my ($self, $topic, $message) = @_;
    return $self->_record_history($topic, $message);
}

# Provided default: subscribe + replay history. Backends may override
# for performance (e.g., transactional batch in PostgreSQL).
async sub subscribe_with_history {
    my ($self, $channel, $topic, $count, %opts) = @_;
    my @history = await $self->read_history($topic, $count);
    for my $msg (@history) {
        await $self->_deliver($channel, $msg);
    }
    await $self->subscribe($channel, $topic, %opts);
    return 1;
}

1;

__END__

=head1 NAME

PAGI::Middleware::Channels::Backend::Role::History - Optional message history capability

=head1 DESCRIPTION

Backends that retain the most recent N messages per topic for replay-on-subscribe
C<with> this role. The facade exposes this through L<PAGI::Channel>'s
C<subscribe(...,history => N)> option.

=cut
```

**Note on `_record_history_hook`:** the base class's `publish` calls `$self->_record_history` (the hook, no-op default). When this role is mixed in, the role's `_record_history_hook` is provided but the base's `publish` actually calls `_record_history`. Two-name approach is awkward — better is to use Role::Tiny's `around` to wrap the no-op:

Replace the `sub _record_history_hook` with:

```perl
# Wrap publish so it records history (the hook in the base is a no-op).
around publish => async sub {
    my ($orig, $self, $topic, $message, %opts) = @_;
    await $self->_record_history($topic, $message);
    return await $self->$orig($topic, $message, %opts);
};
```

Wait — this wraps publish twice (PatternSubs also wraps publish). Both `around`s compose; the order is "last `with`'d, first wrapped." We don't depend on ordering here because History only writes to history and doesn't change the message. Acceptable.

Use the `around publish` approach. Delete the `_record_history_hook` sub.

- [ ] **Step 2: Verify it loads**

```bash
<perl-env> perl -Ilib -MPAGI::Middleware::Channels::Backend::Role::History -e 'print "loaded\n"'
```

- [ ] **Step 3: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend/Role/History.pm
git commit -m "feat: add Role::History capability role

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3.4: Create `Role::Delayed`

**Files:**
- Create: `lib/PAGI/Middleware/Channels/Backend/Role/Delayed.pm`

- [ ] **Step 1: Write the role**

```perl
package PAGI::Middleware::Channels::Backend::Role::Delayed;
use strict;
use warnings;
use Role::Tiny;
use Future::AsyncAwait;
use Time::HiRes ();

requires qw(
    schedule_delayed
    process_delayed
    _has_due_delayed
    _remove_delayed_for_channel
);

# schedule_delayed($type, $target, $message, $delivery_time) -> Future
#   $type: 'send' or 'publish'
#   $target: channel (for send) or topic (for publish)
#   $delivery_time: absolute epoch (Time::HiRes precision)
#
# process_delayed() -> Future($count_processed)
#   Drain entries with delivery_time <= now. Idempotent and safe under
#   concurrent calls (e.g., WATCH/MULTI in Redis, SELECT FOR UPDATE
#   SKIP LOCKED in PostgreSQL).
#
# _has_due_delayed() -> Future($bool)
#   Cheap check: is there at least one entry due now? Returning 1
#   unconditionally is acceptable at the cost of one extra process_delayed
#   call per poll.
#
# _remove_delayed_for_channel($channel) -> Future
#   Remove all delayed entries with type='send' and target=$channel.
#   Used by the around-cleanup hook below.

# Provided default convenience methods.
async sub send_delayed {
    my ($self, $channel, $message, $delay_seconds) = @_;
    $self->_validate_channel($channel);
    $self->_validate_message($message);
    return await $self->schedule_delayed(
        'send', $channel, $message, Time::HiRes::time() + $delay_seconds
    );
}

async sub publish_delayed {
    my ($self, $topic, $message, $delay_seconds) = @_;
    $self->_validate_topic($topic);
    $self->_validate_message($message);
    return await $self->schedule_delayed(
        'publish', $topic, $message, Time::HiRes::time() + $delay_seconds
    );
}

# Pump delayed messages on every poll.
around poll => async sub {
    my ($orig, $self, $channel) = @_;
    if (await $self->_has_due_delayed) {
        await $self->process_delayed;
    }
    return await $self->$orig($channel);
};

# Clean up channel's delayed entries when channel is torn down.
around cleanup => async sub {
    my ($orig, $self, $channel) = @_;
    await $self->_remove_delayed_for_channel($channel);
    return await $self->$orig($channel);
};

1;

__END__

=head1 NAME

PAGI::Middleware::Channels::Backend::Role::Delayed - Optional delayed-delivery capability

=head1 DESCRIPTION

Backends that support scheduling future C<send>s or C<publish>es C<with>
this role. The facade exposes this through L<PAGI::Channel>'s
C<send(..., delay => N)> and C<publish(..., delay => N)> options.

=cut
```

- [ ] **Step 2: Verify it loads**

```bash
<perl-env> perl -Ilib -MPAGI::Middleware::Channels::Backend::Role::Delayed -e 'print "loaded\n"'
```

- [ ] **Step 3: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend/Role/Delayed.pm
git commit -m "feat: add Role::Delayed capability role

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3.5: Create `Role::PatternSubs`

**Files:**
- Create: `lib/PAGI/Middleware/Channels/Backend/Role/PatternSubs.pm`

- [ ] **Step 1: Write the role**

```perl
package PAGI::Middleware::Channels::Backend::Role::PatternSubs;
use strict;
use warnings;
use Role::Tiny;
use Future::AsyncAwait;

requires qw(
    psubscribe
    punsubscribe
    _list_pattern_subscribers
);

# psubscribe($channel, $pattern) -> Future
#   Idempotent. Pattern syntax: see _pattern_to_regex in Backend base.
#
# punsubscribe($channel, $pattern) -> Future
#   If $pattern is undef, removes ALL pattern registrations for $channel.
#
# _list_pattern_subscribers($topic) -> Future(@channels)
#   Returns channels with at least one pattern matching $topic. Caller
#   dedupes against direct-subscriber list, so don't worry about double
#   counting here.

# Hook the publish dispatch: after direct subscribers, also deliver to
# pattern subscribers.
around publish => async sub {
    my ($orig, $self, $topic, $message, %opts) = @_;
    my $result = await $self->$orig($topic, $message, %opts);

    my %excluded = %{ $self->_normalize_exclude($opts{exclude}) };

    # The original publish has already populated direct delivery. We need
    # to know who got direct delivery to dedupe. The cleanest way is to
    # have the backend's publish maintain a per-call %delivered hash that
    # gets passed up through the chain. But Role::Tiny's `around` doesn't
    # let us thread state cleanly.
    #
    # Pragmatic approach: re-query group members so we can dedupe.
    # The cost is one extra read per publish for backends with patterns.
    my @direct = await $self->_group_members($topic);
    my %delivered = map { $_ => 1 } @direct;

    my @pattern_subs = await $self->_list_pattern_subscribers($topic);
    for my $channel (@pattern_subs) {
        next if $excluded{$channel};
        next if $delivered{$channel};
        await $self->_deliver($channel, $message);
        $delivered{$channel} = 1;
    }

    return $result;
};

# Clean up channel's patterns when channel is torn down.
around cleanup => async sub {
    my ($orig, $self, $channel) = @_;
    await $self->punsubscribe($channel, undef);
    return await $self->$orig($channel);
};

1;

__END__

=head1 NAME

PAGI::Middleware::Channels::Backend::Role::PatternSubs - Optional pattern subscription capability

=head1 DESCRIPTION

Backends that support glob-pattern subscriptions C<with> this role.
Pattern syntax: C<*> matches one segment, C<**> matches zero or more.
The facade exposes this through L<PAGI::Channel>'s C<psubscribe>
and C<punsubscribe>.

=head1 NEW REQUIRED PRIMITIVE

This role requires a backend method C<_group_members($topic)> that returns
the list of direct subscribers for a topic. It is used to dedupe pattern
subscribers against direct subscribers without depending on internal state
in the publish call. Backends MUST also implement this method (it's pulled
into the core when this role is mixed in — see Task 3.6 / 3.7 for the
implementation in Memory and Redis).

=cut
```

**Note on the design tradeoff:** the role's `around publish` does an extra read of group members, which adds one query per publish for backends with PatternSubs mixed in. The alternative (threading a `%delivered` hash through `publish` via a hidden second method) is more code for marginal gain. The extra read is acceptable.

- [ ] **Step 2: Verify it loads**

```bash
<perl-env> perl -Ilib -MPAGI::Middleware::Channels::Backend::Role::PatternSubs -e 'print "loaded\n"'
```

- [ ] **Step 3: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend/Role/PatternSubs.pm
git commit -m "feat: add Role::PatternSubs capability role

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3.6: Refactor Memory backend to use roles

**Files:**
- Modify: `lib/PAGI/Middleware/Channels/Backend/Memory.pm`

This is the most substantial refactor. Memory loses its `_channel_id` state, drops the role-provided defaults (`send_delayed`, `publish_delayed`, `subscribe_with_history`), and signatures change for `track`/`untrack`. The role hooks (`around cleanup`, `around poll`, `around publish`) replace inline behavior in Memory's `cleanup`/`poll`/`publish`.

- [ ] **Step 1: Add the role declarations**

At the top of `lib/PAGI/Middleware/Channels/Backend/Memory.pm`, after `use parent`, add:

```perl
use Role::Tiny::With;
with 'PAGI::Middleware::Channels::Backend::Role::Presence',
     'PAGI::Middleware::Channels::Backend::Role::History',
     'PAGI::Middleware::Channels::Backend::Role::Delayed',
     'PAGI::Middleware::Channels::Backend::Role::PatternSubs';
```

- [ ] **Step 2: Remove `_channel_id` state**

In `Memory.pm`'s `new`, delete the line `$self->{_channel_id} = undef;`. Delete `set_channel_id` and `channel_id` methods entirely.

- [ ] **Step 3: Update `track` and `untrack` signatures**

Change `track` to take `($topic, $channel, $presence_data)` (note: channel BEFORE data — different from current Channel.pm calling convention; we update Channel.pm in Task 3.8). Change `untrack` to take `($topic, $channel)`. Replace the `$self->{_channel_id}` reads with the explicit `$channel` parameter:

```perl
async sub track {
    my ($self, $topic, $channel, $presence_data) = @_;
    $self->_validate_topic($topic);

    my $now = time();
    $self->{presence}{$topic} //= {};
    $self->{presence}{$topic}{$channel} = {
        data    => $presence_data,
        expires => $now + $self->{group_expiry},
    };
    return 1;
}

async sub untrack {
    my ($self, $topic, $channel) = @_;
    if ($self->{presence}{$topic}) {
        delete $self->{presence}{$topic}{$channel};
    }
    return 1;
}
```

- [ ] **Step 4: Implement `_presence_topics_for_channel` (required by Role::Presence)**

```perl
async sub _presence_topics_for_channel {
    my ($self, $channel) = @_;
    my @result;
    for my $topic (keys %{$self->{presence}}) {
        if (my $entry = $self->{presence}{$topic}{$channel}) {
            push @result, [ $topic, $entry->{data} ];
        }
    }
    return @result;
}
```

- [ ] **Step 5: Implement `_record_history` (required by Role::History)**

Move the inline history-recording from `publish` (currently lines 240-253) into its own method. Keep the presence-event filter:

```perl
async sub _record_history {
    my ($self, $topic, $message) = @_;
    return 1 unless $self->{history_size} > 0;
    return 1 if $message->{type} =~ /^presence\./;

    $self->{history}{$topic} //= [];
    push @{$self->{history}{$topic}}, {
        message   => $message,
        timestamp => Time::HiRes::time(),
    };
    while (@{$self->{history}{$topic}} > $self->{history_size}) {
        shift @{$self->{history}{$topic}};
    }
    return 1;
}

async sub read_history {
    my ($self, $topic, $count) = @_;
    my @history = @{$self->{history}{$topic} // []};
    @history = @history[-$count..-1] if @history > $count;
    return map { $_->{message} } @history;
}
```

Now strip the inline history block from `publish` (the `if ($self->{history_size} > 0 && $message->{type} !~ /^presence\./)` block). The `around publish` from `Role::History` calls `_record_history` automatically.

- [ ] **Step 6: Implement Role::Delayed primitives**

Replace `send_delayed` and `publish_delayed` (Memory's inline implementations) with the role-required primitives. The role provides default `send_delayed`/`publish_delayed`. Remove Memory's own `send_delayed` and `publish_delayed` methods.

```perl
async sub schedule_delayed {
    my ($self, $type, $target, $message, $delivery_time) = @_;
    push @{$self->{delayed}}, {
        deliver_at => $delivery_time,
        type       => $type,
        target     => $target,
        message    => $message,
    };
    @{$self->{delayed}} = sort { $a->{deliver_at} <=> $b->{deliver_at} } @{$self->{delayed}};
    return 1;
}

async sub _has_due_delayed {
    my ($self) = @_;
    return @{$self->{delayed}} && $self->{delayed}[0]{deliver_at} <= Time::HiRes::time();
}

async sub _remove_delayed_for_channel {
    my ($self, $channel) = @_;
    $self->{delayed} = [
        grep { !($_->{type} eq 'send' && $_->{target} eq $channel) } @{$self->{delayed}}
    ];
    return 1;
}
```

`process_delayed` stays (existing implementation is correct).

Strip the `await $self->process_delayed if @{$self->{delayed}};` line from `poll` — Role::Delayed's `around poll` handles it.

- [ ] **Step 7: Implement Role::PatternSubs primitives**

Add `_list_pattern_subscribers` and `_group_members`:

```perl
async sub _list_pattern_subscribers {
    my ($self, $topic) = @_;
    my @result;
    for my $channel (keys %{$self->{patterns}}) {
        for my $p (@{$self->{patterns}{$channel}}) {
            if ($topic =~ $p->{regex}) {
                push @result, $channel;
                last;
            }
        }
    }
    return @result;
}

async sub _group_members {
    my ($self, $topic) = @_;
    my $members = $self->{groups}{$topic} // {};
    my $now = time();
    return grep { $members->{$_} >= $now } keys %$members;
}
```

Strip the pattern-dispatch block from `publish` (currently lines 263-275) — Role::PatternSubs's `around publish` handles it.

- [ ] **Step 8: Strip role-managed work from `cleanup`**

Memory's `cleanup` currently handles presence (now done by Role::Presence's `around`), patterns (now Role::PatternSubs), and delayed (now Role::Delayed). Reduce `cleanup` to just core state:

```perl
async sub cleanup {
    my ($self, $channel) = @_;

    # Remove from all groups (no presence broadcast - Role::Presence's
    # around-cleanup handles that BEFORE this runs).
    for my $topic (keys %{$self->{groups}}) {
        delete $self->{groups}{$topic}{$channel};
    }

    # Clear message queue
    delete $self->{queues}{$channel};

    # Cancel any pending next_message waiters
    if (my $waiters = delete $self->{_waiters}{$channel}) {
        $_->cancel for grep { !$_->is_ready } @$waiters;
    }

    return 1;
}
```

- [ ] **Step 9: Strip the inline `_broadcast_presence_event` and `subscribe_with_history`**

Delete `_broadcast_presence_event` (no longer needed - the cleanup hook in Role::Presence and the join hook in `subscribe` use `_make_presence_event` directly via the facade in Task 3.8). Delete `subscribe_with_history` (Role::History provides the default).

For `subscribe` itself — the `presence => $data` option handling moves to the facade in Task 3.8. Strip the presence handling from Memory's `subscribe`:

```perl
async sub subscribe {
    my ($self, $channel, $topic, %opts) = @_;
    $self->_validate_channel($channel);
    $self->_validate_topic($topic);

    my $now = time();
    $self->{groups}{$topic} //= {};
    $self->{groups}{$topic}{$channel} = $now + $self->{group_expiry};
    return 1;
}
```

`unsubscribe` similarly loses its presence-leave handling — facade composes that:

```perl
async sub unsubscribe {
    my ($self, $channel, $topic) = @_;
    if ($self->{groups}{$topic}) {
        delete $self->{groups}{$topic}{$channel};
    }
    return 1;
}
```

- [ ] **Step 10: Run the contract suite**

```bash
<perl-env> prove -lv t/10-memory/00-contract.t
```

Expected: failures, because the contract suite is still calling `track`/`untrack` with the OLD signature (and using `set_channel_id`). Note the failures — Task 3.8 and Task 3.12 fix the suite/facade to match.

For now, **commit anyway** and move on. The Redis refactor in 3.7 lands in the same broken state, and 3.8/3.12 close the loop.

- [ ] **Step 11: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend/Memory.pm
git commit -m "$(cat <<'EOF'
refactor: split Memory backend into core + capability roles

Memory now `with`s Presence, History, Delayed, PatternSubs roles.
Drops _channel_id mutable state (track/untrack take channel explicitly).
Strips inline history/pattern/delayed/cleanup logic — provided by role
around hooks.

NOTE: contract suite is broken at this commit because the facade
(PAGI::Channel) still uses the old set_channel_id pattern. Tasks 3.8
(facade refactor) and 3.12 (suite update) restore the green state.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3.7: Refactor Redis backend to use roles

**Files:**
- Modify: `lib/PAGI/Middleware/Channels/Backend/Redis.pm`

Same shape as Task 3.6 but for Redis. Most of the steps mirror Task 3.6; the implementations differ because Redis uses Redis data structures instead of Perl hashes.

- [ ] **Step 1: Add the role declarations**

At the top of `lib/PAGI/Middleware/Channels/Backend/Redis.pm`, after `use parent`, add:

```perl
use Role::Tiny::With;
with 'PAGI::Middleware::Channels::Backend::Role::Presence',
     'PAGI::Middleware::Channels::Backend::Role::History',
     'PAGI::Middleware::Channels::Backend::Role::Delayed',
     'PAGI::Middleware::Channels::Backend::Role::PatternSubs';
```

- [ ] **Step 2: Remove `_channel_id` state and accessors**

Delete `set_channel_id`, `channel_id`, and the `_channel_id` initialization in `new`.

- [ ] **Step 3: Update `track`/`untrack` signatures**

```perl
async sub track {
    my ($self, $topic, $channel, $presence_data) = @_;
    $self->_validate_topic($topic);

    my $key = $self->_presence_key($topic);
    my $json = encode_json($presence_data);
    await $self->{_redis}->hset($key, $channel, $json);
    await $self->{_redis}->expire($key, $self->{group_expiry});
    return 1;
}

async sub untrack {
    my ($self, $topic, $channel) = @_;
    $self->_validate_topic($topic);
    my $key = $self->_presence_key($topic);
    await $self->{_redis}->hdel($key, $channel);
    return 1;
}
```

- [ ] **Step 4: Implement `_presence_topics_for_channel`**

```perl
async sub _presence_topics_for_channel {
    my ($self, $channel) = @_;
    my @result;
    my $keys_ref = await $self->{_redis}->keys($self->_redis_prefix . 'p:*');
    my @keys = ref $keys_ref eq 'ARRAY' ? @$keys_ref : ();
    for my $abs_key (@keys) {
        my ($topic) = $abs_key =~ /p:(.+)$/;
        next unless $topic;
        my $pkey = $self->_presence_key($topic);
        my $json = await $self->{_redis}->hget($pkey, $channel);
        next unless defined $json;
        push @result, [ $topic, decode_json($json) ];
    }
    return @result;
}
```

- [ ] **Step 5: Implement `_record_history` and `read_history`**

```perl
async sub _record_history {
    my ($self, $topic, $message) = @_;
    return 1 unless $self->{history_size} > 0;
    return 1 if $message->{type} =~ /^presence\./;

    my $history_key = $self->_history_key($topic);
    await $self->{_redis}->rpush($history_key, encode_json($message));
    await $self->{_redis}->ltrim($history_key, -$self->{history_size}, -1);
    await $self->{_redis}->expire($history_key, $self->{group_expiry});
    return 1;
}

async sub read_history {
    my ($self, $topic, $count) = @_;
    my $history_key = $self->_history_key($topic);
    my $history_ref = await $self->{_redis}->lrange($history_key, -$count, -1);
    my @history = ref $history_ref eq 'ARRAY' ? @$history_ref : ();
    return map { decode_json($_) } @history;
}
```

Strip the inline history block from Redis's `publish`. Delete `subscribe_with_history` (Role::History provides the default).

- [ ] **Step 6: Implement Role::Delayed primitives**

```perl
async sub schedule_delayed {
    my ($self, $type, $target, $message, $delivery_time) = @_;
    my $entry = encode_json({
        type    => $type,
        target  => $target,
        message => $message,
        id      => rand(),
    });
    await $self->{_redis}->zadd($self->_delayed_key, $delivery_time, $entry);
    return 1;
}

async sub _has_due_delayed {
    my ($self) = @_;
    my $count = await $self->{_redis}->zcount(
        $self->_delayed_key, '-inf', Time::HiRes::time()
    );
    return $count > 0;
}

async sub _remove_delayed_for_channel {
    my ($self, $channel) = @_;
    # Scan all entries, remove those with type='send' and target=$channel.
    # The current Redis backend doesn't have an inline equivalent of this
    # (it relied on poll-time draining); we add it here for the role hook.
    my $entries_ref = await $self->{_redis}->zrange($self->_delayed_key, 0, -1);
    my @entries = ref $entries_ref eq 'ARRAY' ? @$entries_ref : ();
    for my $json (@entries) {
        my $entry = decode_json($json);
        if ($entry->{type} eq 'send' && $entry->{target} eq $channel) {
            await $self->{_redis}->zrem($self->_delayed_key, $json);
        }
    }
    return 1;
}
```

`process_delayed` stays. Strip the `await $self->process_delayed;` from `poll`.

Delete Redis's own `send_delayed` and `publish_delayed` (Role::Delayed provides them).

- [ ] **Step 7: Implement Role::PatternSubs primitives**

```perl
async sub _group_members {
    my ($self, $topic) = @_;
    my $key = $self->_group_key($topic);
    my $members_ref = await $self->{_redis}->smembers($key);
    return ref $members_ref eq 'ARRAY' ? @$members_ref : ();
}

async sub _list_pattern_subscribers {
    my ($self, $topic) = @_;
    my $pattern_keys_ref = await $self->{_redis}->keys($self->_redis_prefix . 'pat:*');
    my @pattern_keys = ref $pattern_keys_ref eq 'ARRAY' ? @$pattern_keys_ref : ();

    my @result;
    for my $pkey (@pattern_keys) {
        my ($channel) = $pkey =~ /pat:(.+)$/;
        next unless $channel;
        my $patterns_ref = await $self->{_redis}->smembers($self->_pattern_key($channel));
        my @patterns = ref $patterns_ref eq 'ARRAY' ? @$patterns_ref : ();
        for my $pattern (@patterns) {
            my $regex = $self->_pattern_to_regex($pattern);
            if ($topic =~ $regex) {
                push @result, $channel;
                last;
            }
        }
    }
    return @result;
}
```

Strip the pattern-dispatch block from Redis's `publish` (it's now in Role::PatternSubs).

- [ ] **Step 8: Strip role-managed work from `cleanup`**

```perl
async sub cleanup {
    my ($self, $channel) = @_;

    # Remove queue
    await $self->{_redis}->del($self->_queue_key($channel));

    # Remove from all groups (no presence broadcast - Role::Presence handles)
    my $group_keys_ref = await $self->{_redis}->keys($self->_redis_prefix . 'g:*');
    my @group_keys = ref $group_keys_ref eq 'ARRAY' ? @$group_keys_ref : ();
    for my $abs_key (@group_keys) {
        my ($topic) = $abs_key =~ /g:(.+)$/;
        next unless $topic;
        await $self->{_redis}->srem($self->_group_key($topic), $channel);
    }

    # Cancel any pending next_message futures for this channel
    if (my $futures = delete $self->{_active_fs}{$channel}) {
        $_->cancel for grep { !$_->is_ready } @$futures;
    }
    if (my $waiters = delete $self->{_waiters}{$channel}) {
        $_->cancel for grep { !$_->is_ready } @$waiters;
    }
    return 1;
}
```

- [ ] **Step 9: Strip presence handling from `subscribe`/`unsubscribe`**

```perl
async sub subscribe {
    my ($self, $channel, $topic, %opts) = @_;
    $self->_validate_channel($channel);
    $self->_validate_topic($topic);

    my $key = $self->_group_key($topic);
    await $self->{_redis}->sadd($key, $channel);
    await $self->{_redis}->expire($key, $self->{group_expiry});
    return 1;
}

async sub unsubscribe {
    my ($self, $channel, $topic) = @_;
    $self->_validate_channel($channel);
    $self->_validate_topic($topic);
    my $key = $self->_group_key($topic);
    await $self->{_redis}->srem($key, $channel);
    return 1;
}
```

- [ ] **Step 10: Commit (still broken until 3.8/3.12)**

```bash
git add lib/PAGI/Middleware/Channels/Backend/Redis.pm
git commit -m "$(cat <<'EOF'
refactor: split Redis backend into core + capability roles

Same shape as the Memory split (Task 3.6). Adds _has_due_delayed and
_remove_delayed_for_channel as new role-required primitives, since
Redis's previous code only handled these inline.

Contract suite remains broken at this commit pending Task 3.8 (facade)
and Task 3.12 (suite update).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3.8: Refactor `PAGI::Channel` facade

**Files:**
- Modify: `lib/PAGI/Channel.pm`

The facade now (a) gates capability calls with `_require_capability`, (b) composes presence handling around `subscribe`/`unsubscribe`, (c) passes `channel_name` explicitly to `track`/`untrack`.

- [ ] **Step 1: Replace `lib/PAGI/Channel.pm`**

Replace the file's body (preserve POD structure but update for new behavior):

```perl
package PAGI::Channel;
use strict;
use warnings;
use Future::AsyncAwait;
use Carp ();

our $VERSION = '0.001';

sub from {
    my ($class, $arg) = @_;
    my $scope = ref($arg) eq 'HASH' ? $arg : $arg->scope;
    my $ch = $scope->{'pagi.channels'}
        or Carp::croak(
            "No channel layer in scope — did you wrap your app with PAGI::Middleware::Channels?"
        );
    return $ch;
}

sub new {
    my ($class, %args) = @_;
    return bless { %args }, $class;
}

sub backend      { shift->{backend} }
sub channel_name { shift->{channel_name} }

sub _require_capability {
    my ($self, $capability) = @_;
    my $role = "PAGI::Middleware::Channels::Backend::Role::$capability";
    return if $self->{backend}->does($role);
    Carp::croak(
        "Backend " . ref($self->{backend}) . " does not support the "
      . "$capability capability. Required for this operation. "
      . "See PAGI::Middleware::Channels documentation for the capability matrix."
    );
}

async sub send {
    my ($self, $channel, $message, %opts) = @_;
    if (my $delay = delete $opts{delay}) {
        $self->_require_capability('Delayed');
        return await $self->{backend}->send_delayed($channel, $message, $delay);
    }
    return await $self->{backend}->send($channel, $message);
}

async sub subscribe {
    my ($self, $topic, %opts) = @_;
    my $history_count = delete $opts{history};
    my $presence     = delete $opts{presence};

    if ($history_count) {
        $self->_require_capability('History');
        await $self->{backend}->subscribe_with_history(
            $self->{channel_name}, $topic, $history_count, %opts
        );
    } else {
        await $self->{backend}->subscribe($self->{channel_name}, $topic, %opts);
    }

    if ($presence) {
        $self->_require_capability('Presence');
        await $self->{backend}->track($topic, $self->{channel_name}, $presence);
        await $self->{backend}->publish(
            $topic,
            $self->{backend}->_make_presence_event($topic, 'presence.join', $presence),
            exclude => $self->{channel_name},
        );
    }

    return 1;
}

async sub unsubscribe {
    my ($self, $topic) = @_;

    # If presence was tracked, broadcast leave and untrack BEFORE
    # removing from the group (so the leave event reaches subscribers).
    my $presence_data;
    if ($self->{backend}->does('PAGI::Middleware::Channels::Backend::Role::Presence')) {
        # We need to know the user's prior presence data. The cleanest
        # path is for the facade to consult list_presence and find self.
        # For now, we can read it indirectly via _presence_topics_for_channel.
        my @entries = await $self->{backend}->_presence_topics_for_channel(
            $self->{channel_name}
        );
        for my $pair (@entries) {
            if ($pair->[0] eq $topic) {
                $presence_data = $pair->[1];
                last;
            }
        }
    }

    await $self->{backend}->unsubscribe($self->{channel_name}, $topic);

    if ($presence_data) {
        await $self->{backend}->untrack($topic, $self->{channel_name});
        await $self->{backend}->publish(
            $topic,
            $self->{backend}->_make_presence_event($topic, 'presence.leave', $presence_data),
            exclude => $self->{channel_name},
        );
    }

    return 1;
}

async sub publish {
    my ($self, $topic, $message, %opts) = @_;
    if (my $delay = delete $opts{delay}) {
        $self->_require_capability('Delayed');
        return await $self->{backend}->publish_delayed($topic, $message, $delay);
    }
    return await $self->{backend}->publish($topic, $message, %opts);
}

async sub psubscribe {
    my ($self, $pattern) = @_;
    $self->_require_capability('PatternSubs');
    return await $self->{backend}->psubscribe($self->{channel_name}, $pattern);
}

async sub punsubscribe {
    my ($self, $pattern) = @_;
    $self->_require_capability('PatternSubs');
    return await $self->{backend}->punsubscribe($self->{channel_name}, $pattern);
}

async sub track {
    my ($self, $topic, $presence_data) = @_;
    $self->_require_capability('Presence');
    return await $self->{backend}->track($topic, $self->{channel_name}, $presence_data);
}

async sub untrack {
    my ($self, $topic) = @_;
    $self->_require_capability('Presence');
    return await $self->{backend}->untrack($topic, $self->{channel_name});
}

async sub list_presence {
    my ($self, $topic, %opts) = @_;
    $self->_require_capability('Presence');
    return await $self->{backend}->list_presence($topic, %opts);
}

async sub count_presence {
    my ($self, $topic) = @_;
    $self->_require_capability('Presence');
    return await $self->{backend}->count_presence($topic);
}

async sub scan_presence {
    my ($self, $topic, %opts) = @_;
    $self->_require_capability('Presence');
    return await $self->{backend}->scan_presence($topic, %opts);
}

async sub next_message {
    my ($self, $channel) = @_;
    return await $self->{backend}->next_message($channel);
}

# Django Channels compatibility aliases
*group_add     = \&subscribe;
*group_discard = \&unsubscribe;
*group_send    = \&publish;

1;

# POD: keep the existing structure but update the synopsis to remove
# any mention of set_channel_id and to mention the capability matrix
# under a new =head1 BACKEND CAPABILITIES section that links to the
# design doc.

__END__
# (existing POD)
```

The POD section above the `__END__` should be updated separately — keep most of the existing content, replace the `=head2 track` and `=head2 untrack` examples to not show channel_name (the facade hides it), and add a paragraph under `=head1 DESCRIPTION` saying:

> Capability methods (C<track>, C<untrack>, C<list_presence>, C<count_presence>, C<scan_presence>, C<psubscribe>, C<punsubscribe>, and the C<delay> / C<history> / C<presence> options) require the backend to declare the corresponding capability role. If the backend does not support a capability, the call croaks with a clear "Backend X does not support the Y capability" message. See L<PAGI::Middleware::Channels/BACKEND CAPABILITIES> for which backend supports what.

- [ ] **Step 2: Run the contract suite (still broken — facade only is not enough)**

```bash
<perl-env> prove -lv t/10-memory/00-contract.t
```

The Memory contract suite still uses `set_channel_id` and the old `track($topic, $data)` signature. Task 3.12 fixes the suite. Don't expect green here — capture the failure list.

- [ ] **Step 3: Commit**

```bash
git add lib/PAGI/Channel.pm
git commit -m "$(cat <<'EOF'
refactor: facade gates capability calls and passes channel_name explicitly

PAGI::Channel now checks $backend->does(...) for each capability call and
croaks with a clear message if absent. Removes the racy set_channel_id
hidden state by passing channel_name to track/untrack on every call.

The facade also composes presence (join/leave broadcast) around subscribe/
unsubscribe — backends only need to register/unregister membership and
maintain presence data; the broadcast wiring is the facade's job.

Contract suite still failing at this commit; Task 3.12 updates it.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3.9: Remove `set_channel_id` from middleware

**Files:**
- Modify: `lib/PAGI/Middleware/Channels.pm`

- [ ] **Step 1: Remove the `set_channel_id` call**

In `lib/PAGI/Middleware/Channels.pm`, find `_create_channel_interface` and remove the `$self->backend->set_channel_id($channel_name);` line. The new body:

```perl
sub _create_channel_interface {
    my ($self, $channel_name) = @_;
    require PAGI::Channel;
    return PAGI::Channel->new(
        backend      => $self->backend,
        channel_name => $channel_name,
    );
}
```

- [ ] **Step 2: Commit**

```bash
git add lib/PAGI/Middleware/Channels.pm
git commit -m "$(cat <<'EOF'
refactor: remove set_channel_id call from middleware

Channel name is now passed explicitly through the facade to backend
methods; no per-request mutable state on the shared backend.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3.10: Reduce abstract method list and update abstract test

**Files:**
- Modify: `lib/PAGI/Middleware/Channels/Backend.pm` — shrink `@ABSTRACT`
- Modify: `t/10-memory/08-abstract-backend.t` — update expectations

- [ ] **Step 1: Shrink `@ABSTRACT` to core methods only**

In `lib/PAGI/Middleware/Channels/Backend.pm`, replace the `@ABSTRACT` list:

```perl
my @ABSTRACT = qw(
    send poll next_message
    subscribe unsubscribe publish
    flush cleanup
    _record_history_hook _dispatch_pattern_subscribers_hook
);
```

Wait — re-reading Tasks 3.3 and 3.5: they don't add `_*_hook` methods. They use `around publish` directly. So no hook methods are needed in the base. The `@ABSTRACT` list is just the eight core methods:

```perl
my @ABSTRACT = qw(
    send poll next_message
    subscribe unsubscribe publish
    flush cleanup
);
```

Methods like `track`, `psubscribe`, etc., are no longer in the base — they're declared `requires` in the relevant role. Backends that don't `with` the role don't need to implement them.

- [ ] **Step 2: Update the abstract test**

Replace `t/10-memory/08-abstract-backend.t`:

```perl
use Test2::V0;
use Test::Lib;
use PAGI::Middleware::Channels::Backend;

# A bare subclass that inherits from the base. Every CORE abstract method
# must croak when called on a subclass that doesn't override.
package TestBackend::Bare {
    use parent -norequire, 'PAGI::Middleware::Channels::Backend';
    # No methods overridden.
}

my $b = TestBackend::Bare->new;

# CORE 8 methods: must croak as abstract on a bare subclass
my @core = qw(
    send poll next_message
    subscribe unsubscribe publish
    flush cleanup
);

for my $m (@core) {
    like
        dies { $b->$m() },
        qr/abstract method/i,
        "core method '$m' croaks as abstract on bare subclass";
}

# Direct instantiation of the abstract base must also croak
like
    dies { PAGI::Middleware::Channels::Backend->new() },
    qr/abstract/i,
    "cannot instantiate abstract base class directly";

# Capability methods (presence/history/delayed/patternsubs) are NOT in the
# core contract. A bare subclass shouldn't be expected to provide them.
ok(!$b->can('track'), 'bare subclass has no track method (Presence-only)');
ok(!$b->can('read_history'), 'bare subclass has no read_history (History-only)');
ok(!$b->can('schedule_delayed'), 'bare subclass has no schedule_delayed (Delayed-only)');
ok(!$b->can('psubscribe'), 'bare subclass has no psubscribe (PatternSubs-only)');

# Memory backend should declare all four capabilities.
require PAGI::Middleware::Channels::Backend::Memory;
my $m = PAGI::Middleware::Channels::Backend::Memory->new;
ok($m->does('PAGI::Middleware::Channels::Backend::Role::Presence'),
   'Memory does Presence');
ok($m->does('PAGI::Middleware::Channels::Backend::Role::History'),
   'Memory does History');
ok($m->does('PAGI::Middleware::Channels::Backend::Role::Delayed'),
   'Memory does Delayed');
ok($m->does('PAGI::Middleware::Channels::Backend::Role::PatternSubs'),
   'Memory does PatternSubs');

done_testing;
```

- [ ] **Step 3: Run the abstract test**

```bash
<perl-env> prove -lv t/10-memory/08-abstract-backend.t
```

Expected: green.

- [ ] **Step 4: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend.pm \
        t/10-memory/08-abstract-backend.t
git commit -m "$(cat <<'EOF'
refactor: shrink core abstract method list to 8 methods

Capability methods (track, read_history, schedule_delayed, psubscribe, etc.)
are no longer in the base abstract list. They are declared as `requires`
in their respective roles. Backends that don't `with` a role don't have
to implement that capability's methods — and the facade gates calls to
those methods cleanly.

The abstract-backend test is updated to assert (a) the 8 core methods
still croak on a bare subclass, (b) capability methods do NOT croak — they
simply don't exist on a bare subclass — and (c) the Memory backend
declares all four capabilities.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3.11: Update compliance suite for new track/untrack signatures

**Files:**
- Modify: `t/lib/Test/PAGI/Channels/Contract.pm` — `_test_presence` and `_test_cleanup`

- [ ] **Step 1: Replace `set_channel_id` calls in `_test_presence`**

The presence subtests call `$backend->set_channel_id('worker.1')` and then `$backend->track('topic', $data)`. Convert each pair to a single explicit call: `$backend->track('topic', 'worker.1', $data)`. Remove all `set_channel_id` calls.

For `untrack`, change `$backend->untrack('topic')` to `$backend->untrack('topic', $channel)` where `$channel` is whatever the test set with the (now-removed) `set_channel_id` call.

This applies to all ten ported subtests in `_test_presence`. Walk each one and update.

- [ ] **Step 2: Replace `set_channel_id` calls in `_test_cleanup`**

The "cleanup removes presence and broadcasts leave" subtest (originally `t/10-memory/07-cleanup.t:49-75`) used `set_channel_id` twice. Update to pass channel explicitly to `subscribe`'s presence option (which the facade now handles, but the contract suite calls the backend directly — so it bypasses the facade composition).

Wait — this is a problem. The compliance suite tests the BACKEND directly. After Phase 3, the backend's `subscribe` no longer accepts `presence => $data`. The presence wiring lives in the facade. So the cleanup-and-presence subtest can't be tested at the backend level anymore.

Two paths:
1. Move the "subscribe with presence + cleanup broadcasts leave" subtest from the contract suite to a new facade-level test in `t/20-facade/`.
2. Have the contract suite call the facade-level wiring: subscribe + track + publish join, then cleanup, then verify leave.

Option 2 is fine; the contract suite is testing observable end-to-end behavior. Update the subtest in `_test_presence` to manually do the facade's job:

```perl
subtest 'cleanup removes presence and broadcasts leave' => sub {
    my $backend = $factory->();

    # ch2 subscribes first to receive events
    _run { $backend->subscribe('ch2', 'room') };
    _run { $backend->track('room', 'ch2', { user => 'bob' }) };

    # ch1 subscribes with presence (manually mimic facade composition)
    _run { $backend->subscribe('ch1', 'room') };
    _run { $backend->track('room', 'ch1', { user => 'alice' }) };
    _run { $backend->publish('room',
        $backend->_make_presence_event('room', 'presence.join', { user => 'alice' }),
        exclude => 'ch1'
    )};

    # Consume join event from ch2
    _run { $backend->poll('ch2') };

    # Cleanup ch1 - the Role::Presence around-cleanup hook handles the leave
    _run { $backend->cleanup('ch1') };

    # ch2 should get leave event
    my $event = _run { $backend->poll('ch2') };
    is($event->{type}, 'presence.leave', 'leave event sent');
    is($event->{presence}{user}, 'alice', 'correct user');

    # Presence list should not include ch1
    my @presence = _run { $backend->list_presence('room') };
    is(scalar @presence, 1, 'only one remaining');
    is($presence[0]->{user}, 'bob', 'bob remains');
};
```

Apply the same pattern to other presence subtests that previously combined subscribe + presence option.

- [ ] **Step 3: Run the contract suite for Memory**

```bash
<perl-env> prove -lv t/10-memory/00-contract.t
```

Expected: green. If failures remain, walk the list — they're either incorrect ports or genuine behavior differences to investigate.

- [ ] **Step 4: Run the contract suite for Redis**

```bash
<perl-env> REDIS_HOST=localhost prove -lv t/30-redis/00-contract.t
```

Expected: green.

- [ ] **Step 5: Run the full test suite**

```bash
<perl-env> REDIS_HOST=localhost prove -lr t/
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add t/lib/Test/PAGI/Channels/Contract.pm
git commit -m "$(cat <<'EOF'
test: update compliance suite for new track/untrack signatures

The backend's track/untrack now take channel explicitly. The presence
option on subscribe is handled by the facade (PAGI::Channel), so backend-
level tests that exercised presence-on-subscribe must mimic the facade
composition manually.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3.12: Update POD across the codebase

**Files:**
- Modify: `lib/PAGI/Middleware/Channels.pm` — add capability matrix to POD
- Modify: `lib/PAGI/Middleware/Channels/Backend.pm` — update POD for shared methods + reduced abstract list
- Modify: `lib/PAGI/Middleware/Channels/Backend/Memory.pm` — update POD to mention role declarations
- Modify: `lib/PAGI/Middleware/Channels/Backend/Redis.pm` — same
- Modify: `lib/PAGI/Channel.pm` — update POD to reference capability matrix and remove set_channel_id mentions

- [ ] **Step 1: Add a `=head1 BACKEND CAPABILITIES` section to `Channels.pm` POD**

Insert after the existing `=head1 BACKENDS` section:

```pod
=head1 BACKEND CAPABILITIES

PAGI's channel layer splits backend behavior into a small required core
and four optional capability roles. Backends declare which capabilities
they support; user calls that require an unsupported capability croak
with a clear "not supported" message.

=head2 Capability Matrix

  Backend     Core  Presence  History  Delayed  PatternSubs
  ---------  ----- --------- -------- -------- ------------
  Memory       Y       Y         Y       Y          Y
  Redis        Y       Y         Y       Y          Y
  PostgreSQL   Y       Y         Y       Y          Y     (planned, not yet shipped)

=head2 Choosing a backend

If you need presence (real-time online-user tracking), history (replay-on-subscribe),
delayed delivery, or pattern subscriptions, all current backends support these.
Future backends (NATS, AWS SNS+SQS, etc.) may support only a subset; the capability
matrix above will be updated as those land.

See F<docs/plans/2026-04-16-backend-capability-roles-design.md> for the
full design document, including the implementer's guide for adding a new
backend.
```

- [ ] **Step 2: Update `Backend.pm` POD**

Replace the `=head1 REQUIRED METHODS` section with one that lists only the 8 core methods. Add new sections describing the shared helpers (`_validate_channel`, `_validate_message`, `_pattern_to_regex`, `_normalize_exclude`, `_make_presence_event`) as protected utilities for backend authors. Add a `=head1 CAPABILITY ROLES` section pointing to each role's module.

- [ ] **Step 3: Update Memory and Redis POD**

In each backend's POD, add a sentence in the SYNOPSIS or DESCRIPTION:

> This backend implements the core C<PAGI::Middleware::Channels::Backend> contract and declares the C<Presence>, C<History>, C<Delayed>, and C<PatternSubs> capability roles.

- [ ] **Step 4: Update `Channel.pm` POD**

Remove any mention of `set_channel_id`. Add a sentence under `=head1 DESCRIPTION`:

> Capability-gated methods (track/untrack/list_presence/count_presence/scan_presence,
> psubscribe/punsubscribe, and the delay/history/presence options on send/publish/subscribe)
> croak if the backend does not declare the required role. See L<PAGI::Middleware::Channels/BACKEND CAPABILITIES> for the matrix.

Update the `=head2 track` and `=head2 untrack` examples to NOT show `channel_name`.

- [ ] **Step 5: Run the full test suite one more time**

```bash
<perl-env> REDIS_HOST=localhost prove -lr t/
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add lib/
git commit -m "$(cat <<'EOF'
docs: update POD across modules with capability matrix and role references

Adds a BACKEND CAPABILITIES section to PAGI::Middleware::Channels POD
with the capability matrix. Updates each module's POD to reference the
roles, the shared utilities, and the design document.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

**Phase 3 done.** Backend interface is split into core + four capability roles. The validation drift bug is closed. The `set_channel_id` race is gone. Adding PostgreSQL is now a pure additive task using the implementer's guide above.

---

## Self-Review Checklist (run before declaring this plan executable)

Before treating this plan as ready-to-execute, walk this list:

1. **Spec coverage.** Every section in the design (Capability Matrix, Core Contract, each Role, Validation Policy, Facade Composition, Compliance Suite, Implementer's Guide) is implemented by at least one task. The Implementer's Guide is the design's reference output, not something the implementation tasks need to "build" — it stands as documentation for the future PostgreSQL work.
2. **Task ordering.** Phase 1 → Phase 2 → Phase 3. Within Phase 3, the role files (3.2-3.5) come before backend refactors (3.6-3.7) which come before facade refactor (3.8) and compliance suite update (3.11). This is the only order that keeps each commit safe-to-ship-on-its-own.
3. **Acceptance per phase.** Phase 1 ends with both backends green on the contract suite (Redis with TODO). Phase 2 ends with Redis no longer needing TODO. Phase 3 ends with the full suite green and the abstract-backend test asserting the new shape.
4. **No placeholders.** Each step has either: explicit code, an explicit reference to an existing file with line numbers, or an explicit shell command with expected output. No "TBD" / "implement appropriately" / "similar to above".
5. **Cross-task consistency.** Method names (`_record_history`, `read_history`, `schedule_delayed`, `_has_due_delayed`, `_remove_delayed_for_channel`, `_list_pattern_subscribers`, `_group_members`, `_presence_topics_for_channel`, `_make_presence_event`, `_normalize_exclude`, `_pattern_to_regex`, `_require_capability`) are used consistently between role definitions, backend implementations, and the facade.

---

# Session Handoff (2026-04-16) — work paused before execution

**State at handoff:**

- This file (`docs/plans/2026-04-16-backend-capability-roles-design.md`) is **uncommitted**. John explicitly asked it not be committed yet; he wants to review before commit.
- No production code or test code has been touched. Phase 1, 2, and 3 above are all pending.
- Branch: `main`. The four other untracked files in `docs/plans/2026-04-15-*.md` predate this conversation and are unrelated.
- The most recent commits (`git log --oneline`): `ea375f1 refactor: update all call sites from from_scope to from`, then the v0.001 work documented in the other April-15 plan files.

**Decisions reached in this conversation that are not derivable from the doc itself:**

1. **Role::Tiny was confirmed (not Moo or duck-typing).** John explicitly approved it as the role library.
2. **John approved the three architectural calls before the spec was written:**
   - `set_channel_id` / `channel_id` move into Role::Presence (then in the spec they were further reduced to "removed entirely" — the facade now passes `channel_name` explicitly to `track`/`untrack`. This goes beyond what John literally approved; flag it for him before executing Task 3.6/3.8 in case he wants to scope it back.)
   - `flush` stays in core (not split into a `Testable` capability).
   - Capability misuse uses plain `croak` with a string, not a structured exception class. Structured exceptions are explicitly deferred.
3. **John wants PostgreSQL as the next backend** once `DBD::Pg` async support lands. He also wants at least one popular SaaS pubsub eventually; the design doc lists Ably/Pusher/NATS/SNS+SQS as candidates with a recommendation toward NATS first.
4. **John wants the spec doubly purposed:** user-facing capability docs ("if you want X, you need Y") AND a future-PostgreSQL implementer's guide. The "Implementing a New Backend" section (10-step PostgreSQL walkthrough) is the implementer's guide — it must stay comprehensive.
5. **John's CLAUDE.md rule on commits:** never commit unless explicitly asked. Each task in the implementation plan ends with a commit step; those are part of the plan but a future executor must NOT execute the commits unless John has authorized the execution session.

**Where to resume:**

- John has the spec + plan ready to read. The next user-facing action is for John to review the document and say either:
  - "Approved, commit it" → commit the design doc with a `docs:` message, then ask whether to start Phase 1 Task 1.1 inline or via subagent-driven-development.
  - "Change X" → revise the doc, ask if commit is OK now.
- The brainstorming skill flow is currently between step 8 (user reviews spec) and step 9 (transition to writing-plans). Writing-plans was already invoked in this session and the implementation plan is appended to the file. The next skill, when execution starts, is `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans`.

**Open issue to raise with John before executing:**

- The spec proposes removing `set_channel_id` entirely (not just moving it to Role::Presence as he literally approved). The motivation is fixing a real latent race: the backend instance is shared across requests, and `_channel_id` is per-instance mutable state that gets overwritten between connections. Confirm John is OK with the broader scope before Task 3.6 / Task 3.8 are executed; the alternative is to keep `set_channel_id` on Role::Presence as a backward-compatible accessor and continue using it (which would not fix the race).

**Files relevant to picking back up:**

- This file: `docs/plans/2026-04-16-backend-capability-roles-design.md`
- Existing pattern reference: `docs/plans/2026-04-15-event-driven-receive.md` (similar combined design + plan format)
- Backend code under change: `lib/PAGI/Middleware/Channels/Backend{,Memory,Redis}.pm`, `lib/PAGI/Channel.pm`, `lib/PAGI/Middleware/Channels.pm`
- Test code under change: `t/lib/Test/PAGI/Channels.pm` (existing helpers), to be created `t/lib/Test/PAGI/Channels/Contract.pm`, plus per-backend test files in `t/10-memory/` and `t/30-redis/`.

