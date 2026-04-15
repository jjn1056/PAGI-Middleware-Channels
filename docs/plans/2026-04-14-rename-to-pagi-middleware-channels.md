# Rename to PAGI-Middleware-Channels

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize the distribution to follow the `PAGI::Middleware::Session` / `PAGI::Session` precedent: middleware lives at `PAGI::Middleware::Channels`, the handler-facing helper takes the freed-up `PAGI::Channels` slot, and the storage backends move under `PAGI::Middleware::Channels::Backend::*`. Distribution renames from `PAGI-Channels` to `PAGI-Middleware-Channels`.

**Architecture:** Pure rename + split — no behavior changes. The current `PAGI::Channels` facade splits into two top-level files: `PAGI::Middleware::Channels` (the factory with `new` / `wrap` / `backend`) and `PAGI::Channels` (what was `PAGI::Channels::Interface` nested inside the old facade — the per-connection helper users get via `$scope->{'pagi.channels'}`). Backends move package + path. The `Backend` role's contract (15 required methods) is unchanged; the `with` declarations in the two impls update to the new role name. Distribution metadata (`dist.ini`, `README`, `cpanfile`) follows.

**Tech stack:** Perl 5.18+, Future::AsyncAwait, Role::Tiny, Test2::V0. Pre-CPAN — no backwards-compat shims.

**Perlbrew:** `source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default`

**Test setup:** `docker compose -f t/docker-compose.yml up -d` (Redis on `localhost:6379`).

**Test command:** `REDIS_HOST=localhost prove -lr t/`

**Verification gates (REQUIRED at end of every task):**

1. `prove -lr t/` — ALL pass.
2. Grep audit appropriate to the task — no stale references to the old names.
3. Doc/code consistency — POD package names match actual `package` declarations.

---

## File structure

| Old | New |
|---|---|
| `lib/PAGI/Channels.pm` (factory + nested `Interface` helper) | **split into two files** — see below |
| — *(new file)* | `lib/PAGI/Middleware/Channels.pm` (factory: `new`, `wrap`, `backend`, internal helpers) |
| `lib/PAGI/Channels.pm` *(rewritten)* | `lib/PAGI/Channels.pm` — now contains only the helper (was nested as `PAGI::Channels::Interface`) |
| `lib/PAGI/Channels/Backend.pm` | `lib/PAGI/Middleware/Channels/Backend.pm` |
| `lib/PAGI/Channels/Backend/Memory.pm` | `lib/PAGI/Middleware/Channels/Backend/Memory.pm` |
| `lib/PAGI/Channels/Backend/Redis.pm` | `lib/PAGI/Middleware/Channels/Backend/Redis.pm` |
| `dist.ini` `name = PAGI-Channels` | `name = PAGI-Middleware-Channels` |

**Test helper (`t/lib/Test/PAGI/Channels.pm`) stays put.** It's test infrastructure, not part of the distribution's public surface. The package name `Test::PAGI::Channels` describes what it helps test, no need to drag it through the rename.

---

### Task 1: move backends under `PAGI::Middleware::Channels::Backend::*`

**Files:**
- Move: `lib/PAGI/Channels/Backend.pm` → `lib/PAGI/Middleware/Channels/Backend.pm`
- Move: `lib/PAGI/Channels/Backend/Memory.pm` → `lib/PAGI/Middleware/Channels/Backend/Memory.pm`
- Move: `lib/PAGI/Channels/Backend/Redis.pm` → `lib/PAGI/Middleware/Channels/Backend/Redis.pm`
- Modify: every test file under `t/10-memory/*.t` and `t/30-redis/*.t` (`require` line)
- Modify: `t/00-load.t` (any backend `use`/`require`)

**Context:** Mechanical move + `package` rename + `with` rename in the impls. The facade's `_create_channel_interface` does NOT reference backend package names — it operates on the instance the user passed in — so no changes there until Task 2.

- [ ] **Step 1: Make the new directory and move the role.**

```bash
mkdir -p lib/PAGI/Middleware/Channels/Backend
git mv lib/PAGI/Channels/Backend.pm lib/PAGI/Middleware/Channels/Backend.pm
```

- [ ] **Step 2: Edit the role's `package` declaration and POD NAME.**

In `lib/PAGI/Middleware/Channels/Backend.pm`, change:

```perl
package PAGI::Channels::Backend;
```

to

```perl
package PAGI::Middleware::Channels::Backend;
```

And update the POD `=head1 NAME` line:

```pod
PAGI::Middleware::Channels::Backend - Role for channel layer backends
```

- [ ] **Step 3: Move the Memory backend.**

```bash
git mv lib/PAGI/Channels/Backend/Memory.pm lib/PAGI/Middleware/Channels/Backend/Memory.pm
```

- [ ] **Step 4: Update Memory backend package + `with` + POD.**

In `lib/PAGI/Middleware/Channels/Backend/Memory.pm`:

- `package PAGI::Channels::Backend::Memory;` → `package PAGI::Middleware::Channels::Backend::Memory;`
- `with 'PAGI::Channels::Backend';` → `with 'PAGI::Middleware::Channels::Backend';`
- POD `=head1 NAME` → `PAGI::Middleware::Channels::Backend::Memory - In-memory channel backend for single-process use`
- SYNOPSIS `use PAGI::Channels::Backend::Memory;` → `use PAGI::Middleware::Channels::Backend::Memory;`

- [ ] **Step 5: Move the Redis backend.**

```bash
git mv lib/PAGI/Channels/Backend/Redis.pm lib/PAGI/Middleware/Channels/Backend/Redis.pm
```

- [ ] **Step 6: Update Redis backend package + `with` + POD.**

In `lib/PAGI/Middleware/Channels/Backend/Redis.pm`:

- `package PAGI::Channels::Backend::Redis;` → `package PAGI::Middleware::Channels::Backend::Redis;`
- `with 'PAGI::Channels::Backend';` → `with 'PAGI::Middleware::Channels::Backend';`
- POD `=head1 NAME` → `PAGI::Middleware::Channels::Backend::Redis - Redis-backed channel backend for multi-process use`
- SYNOPSIS `use PAGI::Channels::Backend::Redis;` → `use PAGI::Middleware::Channels::Backend::Redis;`

- [ ] **Step 7: Remove the now-empty old backend directory.**

```bash
rmdir lib/PAGI/Channels/Backend
```

(Should be empty after the moves; `rmdir` will fail if anything's left, in which case investigate.)

- [ ] **Step 8: Update test `require` / `use` lines.**

Files to edit:
- `t/00-load.t`
- `t/10-memory/01-core.t` through `t/10-memory/07-cleanup.t` — each has `use PAGI::Channels::Backend::Memory;`
- `t/30-redis/01-core.t` through `t/30-redis/06-history.t` — each has `require PAGI::Channels::Backend::Redis;`

Do a search-and-replace across the test tree:
- `PAGI::Channels::Backend::Memory` → `PAGI::Middleware::Channels::Backend::Memory`
- `PAGI::Channels::Backend::Redis`  → `PAGI::Middleware::Channels::Backend::Redis`
- `PAGI::Channels::Backend`         → `PAGI::Middleware::Channels::Backend` (the role — only matches in t/00-load.t if anywhere)

- [ ] **Step 9: Run the test suite.**

Run: `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && REDIS_HOST=localhost prove -lr t/ 2>&1 | tail -10'`

Expected: 16 files, 69 tests, all pass.

(Note: the facade still references `PAGI::Channels::Backend` in its POD SEE ALSO until Task 4 — that doesn't affect tests.)

- [ ] **Step 10: Grep audit and commit.**

Run:

```bash
grep -rn 'PAGI::Channels::Backend' lib/ t/ examples/
```

Expected: zero hits. (POD will still reference the old names in `lib/PAGI/Channels.pm` — Task 4 cleans that up; for now Task 1's audit is constrained to the backend identifiers in code.)

Then:

```bash
git add lib/PAGI/Middleware/ t/
git rm -r lib/PAGI/Channels/Backend  # confirms the old dir is gone
git commit -m "refactor(namespace): move Backend role + impls under PAGI::Middleware::Channels::Backend"
```

---

### Task 2: split `PAGI::Channels` into facade + helper

**Files:**
- Create: `lib/PAGI/Middleware/Channels.pm` (the factory — was the outer `package PAGI::Channels` in `lib/PAGI/Channels.pm`)
- Rewrite: `lib/PAGI/Channels.pm` (now only the helper — was `package PAGI::Channels::Interface` nested inside the old file)
- Modify: `t/20-facade/01-basic.t`
- Modify: `t/20-facade/02-wrap.t`

**Context:** The current `lib/PAGI/Channels.pm` defines two packages in one file: `PAGI::Channels` (the facade, lines 1–115) and `PAGI::Channels::Interface` (the helper, lines 118–192). We're splitting them into two top-level files. The Interface helper becomes the new `PAGI::Channels`. The factory becomes `PAGI::Middleware::Channels`.

The facade's `_create_channel_interface` method currently calls `PAGI::Channels::Interface->new(...)`. After the split, it calls `PAGI::Channels->new(...)`.

- [ ] **Step 1: Create `lib/PAGI/Middleware/Channels.pm`.**

```perl
package PAGI::Middleware::Channels;
use strict;
use warnings;
use Future::AsyncAwait;
use Future;
use Future::IO;

our $VERSION = '0.001';

sub new {
    my ($class, %args) = @_;

    my $backend = $args{backend}
        or die "PAGI::Middleware::Channels: 'backend' argument required "
             . "(a PAGI::Middleware::Channels::Backend instance)";

    return bless {
        _backend => $backend,
        _counter => 0,
    }, $class;
}

sub backend { shift->{_backend} }

sub wrap {
    my ($self, $inner_app) = @_;

    return async sub {
        my ($scope, $receive, $send) = @_;

        my $channel_name = $self->_generate_channel_name();

        $scope->{'pagi.channels'} = $self->_create_channel_interface($channel_name);
        $scope->{'pagi.channel'}  = $channel_name;

        my $wrapped_receive = async sub {
            if (my $msg = await $self->{_backend}->poll($channel_name)) {
                return $msg;
            }

            my $protocol_f = $receive->();

            while (!$protocol_f->is_ready) {
                await Future::IO->sleep(0.1);

                if (my $msg = await $self->{_backend}->poll($channel_name)) {
                    return $msg;
                }
            }

            return $protocol_f->get;
        };

        my $err;
        eval { await $inner_app->($scope, $wrapped_receive, $send) };
        $err = $@;

        await $self->{_backend}->cleanup($channel_name);

        die $err if $err;
    };
}

sub _generate_channel_name {
    my ($self) = @_;
    $self->{_counter}++;
    return sprintf("conn.%d.%d.%d", $$, time(), $self->{_counter});
}

sub _create_channel_interface {
    my ($self, $channel_name) = @_;

    require PAGI::Channels;
    $self->{_backend}->set_channel_id($channel_name);

    return PAGI::Channels->new(
        backend      => $self->{_backend},
        channel_name => $channel_name,
    );
}

1;

__END__

=encoding utf-8

=head1 NAME

PAGI::Middleware::Channels - Cross-process messaging middleware for PAGI applications

=head1 SYNOPSIS

    use PAGI::Middleware::Channels;
    use PAGI::Middleware::Channels::Backend::Memory;

    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    # Prod: Redis-backed, with a caller-owned Async::Redis client
    use Async::Redis;
    use PAGI::Middleware::Channels::Backend::Redis;

    my $redis = Async::Redis->new(
        uri       => 'redis://localhost:6379',
        prefix    => 'myapp:channels:',
        reconnect => 1,
    );
    $redis->connect->get;

    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis),
    );

    # Wrap your PAGI app
    my $app = $channels->wrap(async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = $scope->{'pagi.channels'};   # PAGI::Channels instance
        my $my_channel = $scope->{'pagi.channel'};

        await $ch->subscribe("chat.room1",
            presence => { user => 'alice', status => 'online' }
        );

        while (1) {
            my $event = await $receive->();
            # Handle chat messages, presence events, etc.
        }
    });

=head1 DESCRIPTION

Provides cross-process and cross-server messaging for PAGI applications.

The middleware wraps your PAGI app and injects two scope keys:

=over 4

=item * C<< $scope->{'pagi.channels'} >> — a L<PAGI::Channels> helper for this connection.

=item * C<< $scope->{'pagi.channel'} >> — this connection's unique channel name.

=back

It also wraps the C<$receive> callable so channel-delivered messages
interleave with protocol events, and runs cleanup on the backend when
the inner app exits.

=head2 LOOP AGNOSTICISM

This module uses L<Future::IO> only. It works with any event loop —
L<IO::Async>, L<Mojo::IOLoop>, or any other implementation of the
Future::IO interface.

=head1 METHODS

=head2 new

    my $channels = PAGI::Middleware::Channels->new(
        backend => $backend_instance,
    );

The C<backend> argument is B<required> and must be a
L<PAGI::Middleware::Channels::Backend> instance (e.g.,
L<PAGI::Middleware::Channels::Backend::Memory> or
L<PAGI::Middleware::Channels::Backend::Redis>).

This module does no backend construction of its own — callers wire
up the backend (and, for Redis, the underlying L<Async::Redis>
client) explicitly. The distribution has no runtime dependency on
any Redis client.

=head2 wrap

    my $app = $channels->wrap($inner_app);

Wraps a PAGI application, returning a new app that:

=over 4

=item * Generates a unique channel name per request and injects
C<< $scope->{'pagi.channel'} >>.

=item * Constructs a L<PAGI::Channels> helper bound to that channel and
the configured backend, and injects it as C<< $scope->{'pagi.channels'} >>.

=item * Wraps C<$receive> so channel messages are interleaved with
protocol events.

=item * Calls C<< $backend->cleanup($channel) >> when the inner app exits.

=back

=head2 backend

    my $backend = $channels->backend;

Returns the backend instance passed to the constructor.

=head1 BACKENDS

=head2 L<PAGI::Middleware::Channels::Backend::Memory>

Single-process, in-memory. Good for development and testing.

=head2 L<PAGI::Middleware::Channels::Backend::Redis>

Multi-process, multi-server. Takes a caller-owned L<Async::Redis>
instance; this distribution itself has no runtime dependency on any
Redis client — any object that ducks the Async::Redis interface works.

=head1 SEE ALSO

L<PAGI::Channels>, L<Async::Redis>, L<Future::IO>, L<Future::AsyncAwait>

=head1 AUTHOR

John Napiorkowski

=head1 LICENSE

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
```

- [ ] **Step 2: Rewrite `lib/PAGI/Channels.pm` as the helper-only file.**

```perl
package PAGI::Channels;
use strict;
use warnings;
use Future::AsyncAwait;

our $VERSION = '0.001';

sub new {
    my ($class, %args) = @_;
    return bless { %args }, $class;
}

sub backend      { shift->{backend} }
sub channel_name { shift->{channel_name} }

async sub send {
    my ($self, $channel, $message, %opts) = @_;
    my $delay = delete $opts{delay};
    if ($delay) {
        return await $self->{backend}->send_delayed($channel, $message, $delay);
    }
    return await $self->{backend}->send($channel, $message);
}

async sub subscribe {
    my ($self, $topic, %opts) = @_;
    my $history = delete $opts{history};
    if ($history) {
        return await $self->{backend}->subscribe_with_history(
            $self->{channel_name}, $topic, $history, %opts
        );
    }
    return await $self->{backend}->subscribe($self->{channel_name}, $topic, %opts);
}

async sub unsubscribe {
    my ($self, $topic) = @_;
    return await $self->{backend}->unsubscribe($self->{channel_name}, $topic);
}

async sub publish {
    my ($self, $topic, $message, %opts) = @_;
    my $delay = delete $opts{delay};
    if ($delay) {
        return await $self->{backend}->publish_delayed($topic, $message, $delay);
    }
    return await $self->{backend}->publish($topic, $message, %opts);
}

async sub psubscribe {
    my ($self, $pattern) = @_;
    return await $self->{backend}->psubscribe($self->{channel_name}, $pattern);
}

async sub punsubscribe {
    my ($self, $pattern) = @_;
    return await $self->{backend}->punsubscribe($self->{channel_name}, $pattern);
}

async sub track {
    my ($self, $topic, $presence_data) = @_;
    return await $self->{backend}->track($topic, $presence_data);
}

async sub untrack {
    my ($self, $topic) = @_;
    return await $self->{backend}->untrack($topic);
}

async sub list_presence {
    my ($self, $topic) = @_;
    return await $self->{backend}->list_presence($topic);
}

# Django Channels compatibility aliases
*group_add     = \&subscribe;
*group_discard = \&unsubscribe;
*group_send    = \&publish;

1;

__END__

=encoding utf-8

=head1 NAME

PAGI::Channels - Per-connection helper for the PAGI channels middleware

=head1 SYNOPSIS

    # Normally constructed by PAGI::Middleware::Channels and injected
    # into the request scope:
    my $ch = $scope->{'pagi.channels'};
    my $my_channel = $scope->{'pagi.channel'};

    await $ch->subscribe("chat.room1",
        presence => { user => 'alice', status => 'online' }
    );

    await $ch->publish("chat.room1", { type => 'msg', text => 'hi' });

    my @users = await $ch->list_presence("chat.room1");

    # For unit tests / scripts you can construct one directly:
    use PAGI::Channels;
    use PAGI::Middleware::Channels::Backend::Memory;

    my $ch = PAGI::Channels->new(
        backend      => PAGI::Middleware::Channels::Backend::Memory->new,
        channel_name => 'test.conn',
    );

=head1 DESCRIPTION

A handler-facing helper bound to a single connection's channel name
and the configured backend. Created by L<PAGI::Middleware::Channels>'s
C<wrap()> per request and exposed via C<< $scope->{'pagi.channels'} >>.

=head1 CONSTRUCTOR

=head2 new

    PAGI::Channels->new(
        backend      => $backend,
        channel_name => $channel_name,
    );

=head1 METHODS

=head2 send

    await $ch->send($channel, { type => 'msg', ... });
    await $ch->send($channel, $msg, delay => 300);

Send a message directly to a specific channel. Options:

=over 4

=item * C<delay> — Delay delivery by N seconds.

=back

=head2 subscribe

    await $ch->subscribe($topic);
    await $ch->subscribe($topic, presence => { user => 'alice' });
    await $ch->subscribe($topic, history => 10);

Subscribe this connection's channel to a topic. Options:

=over 4

=item * C<presence> — Hash of presence data to track for this subscriber.

=item * C<history> — Number of recent messages to receive immediately on subscribe.

=back

=head2 unsubscribe

    await $ch->unsubscribe($topic);

Broadcasts C<presence.leave> if presence was tracked.

=head2 publish

    await $ch->publish($topic, { type => 'msg', ... });
    await $ch->publish($topic, $msg, exclude => $my_channel);
    await $ch->publish($topic, $msg, delay => 60);

Options:

=over 4

=item * C<exclude> — Channel or arrayref of channels to exclude.

=item * C<delay> — Delay delivery by N seconds.

=back

=head2 psubscribe

    await $ch->psubscribe("chat.*");      # Matches chat.room1
    await $ch->psubscribe("events.**");   # Matches events.user.123

C<*> matches exactly one segment; C<**> matches zero or more.

=head2 punsubscribe

    await $ch->punsubscribe("chat.*");
    await $ch->punsubscribe();  # Remove all patterns

=head2 track

    await $ch->track($topic, { worker_id => $$, status => 'idle' });

Explicitly track presence without subscribing — useful for workers.

=head2 untrack

    await $ch->untrack($topic);

=head2 list_presence

    my @users = await $ch->list_presence($topic);

Returns the array of presence hashes for a topic.

=head1 DJANGO CHANNELS COMPATIBILITY

For familiarity, these aliases are provided:

    group_add     => subscribe
    group_discard => unsubscribe
    group_send    => publish

=head1 SEE ALSO

L<PAGI::Middleware::Channels>

=head1 AUTHOR

John Napiorkowski

=head1 LICENSE

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
```

- [ ] **Step 3: Update facade tests.**

In `t/20-facade/01-basic.t`:

- `use PAGI::Channels;` → `use PAGI::Middleware::Channels;`
- `use PAGI::Channels::Backend::Memory;` → `use PAGI::Middleware::Channels::Backend::Memory;`
- `PAGI::Channels->new(` → `PAGI::Middleware::Channels->new(`
- `PAGI::Channels::Backend::Memory->new` → `PAGI::Middleware::Channels::Backend::Memory->new`
- The `like(dies { ... }, qr/backend/)` regex still matches the new die message.

In `t/20-facade/02-wrap.t`: same pattern of replacements.

- [ ] **Step 4: Run the full suite.**

Run: `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && REDIS_HOST=localhost prove -lr t/ 2>&1 | tail -10'`

Expected: 16 files, 69 tests, all pass.

- [ ] **Step 5: Grep audit + POD check + commit.**

Audits:

```bash
grep -rn 'PAGI::Channels::Interface' lib/ t/ examples/    # expected: 0
grep -rn 'package PAGI::Channels;' lib/                   # expected: 1 (lib/PAGI/Channels.pm only)
podchecker lib/PAGI/Channels.pm lib/PAGI/Middleware/Channels.pm
```

Then:

```bash
git add lib/PAGI/Channels.pm lib/PAGI/Middleware/Channels.pm t/20-facade/
git commit -m "refactor(namespace): split PAGI::Channels into Middleware::Channels (factory) + Channels (helper)"
```

---

### Task 3: update the two examples

**Files:**
- Modify: `examples/chat/app.pl`
- Modify: `examples/task-queue/server.pl`

**Context:** Both examples currently `use PAGI::Channels;` and `use PAGI::Channels::Backend::Redis;`, then call `PAGI::Channels->new(backend => PAGI::Channels::Backend::Redis->new(redis => $redis))`. Update both to the new names.

- [ ] **Step 1: Edit `examples/chat/app.pl`.**

Replace:
- `use PAGI::Channels;` → `use PAGI::Middleware::Channels;`
- `use PAGI::Channels::Backend::Redis;` → `use PAGI::Middleware::Channels::Backend::Redis;`
- `PAGI::Channels->new(` → `PAGI::Middleware::Channels->new(`
- `PAGI::Channels::Backend::Redis->new(` → `PAGI::Middleware::Channels::Backend::Redis->new(`

- [ ] **Step 2: Edit `examples/task-queue/server.pl`.** Same four replacements.

- [ ] **Step 3: Syntax-check both.**

Run:

```bash
bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && perl -Ilib -c examples/chat/app.pl && perl -Ilib -c examples/task-queue/server.pl'
```

Expected: each prints `… syntax OK` (the pre-existing void-context warning on `examples/chat/app.pl`'s `$app;` line is unrelated and stays).

- [ ] **Step 4: Commit.**

```bash
git add examples/
git commit -m "refactor(examples): use PAGI::Middleware::Channels naming"
```

---

### Task 4: distribution metadata + top-level docs

**Files:**
- Modify: `dist.ini`
- Modify: `cpanfile` (header comment only)
- Modify: `README.md`
- Modify: `FEATURE-channels-parity.md`

**Context:** Distribution renames from `PAGI-Channels` to `PAGI-Middleware-Channels`. Dist::Zilla picks the main module from the dist name by default (`PAGI-Middleware-Channels` → `lib/PAGI/Middleware/Channels.pm`) — no `[MainModule]` override needed.

- [ ] **Step 1: Update `dist.ini`.**

```ini
name    = PAGI-Middleware-Channels
```

(Just the `name = ` line. Author, license, copyright_holder, plugins all stay.)

- [ ] **Step 2: Update `cpanfile` header comment.**

```perl
# cpanfile - PAGI-Middleware-Channels dependencies
```

(Only the first comment line; everything below stays.)

- [ ] **Step 3: Rewrite `README.md` Quick Start + Backends sections.**

Replace every `PAGI::Channels` → `PAGI::Middleware::Channels` for the *factory*. The `pagi.channels` scope key stays as-is. Backend references update to `PAGI::Middleware::Channels::Backend::Redis` / `…::Memory`. Adjust the title:

```markdown
# PAGI-Middleware-Channels

Cross-process messaging middleware for PAGI applications. Exceeds Django Channels.
```

…and the install command:

```bash
cpanm PAGI::Middleware::Channels
```

Quick Start example uses `PAGI::Middleware::Channels->new(backend => PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis))` and `use PAGI::Middleware::Channels::Backend::Redis;`. Backends section heads become `### Memory — PAGI::Middleware::Channels::Backend::Memory` and same for Redis.

- [ ] **Step 4: Update `FEATURE-channels-parity.md`.**

Replace every textual reference to `PAGI::Channels` (the facade) with `PAGI::Middleware::Channels`, and every backend reference (`PAGI::Channels::Backend::*`) with `PAGI::Middleware::Channels::Backend::*`. The `Channels.pm:189-191` line in the appendix that mentions the Django alias location updates to `lib/PAGI/Channels.pm:NN` (the helper's new home — recompute the line).

- [ ] **Step 5: Run the full suite once more.**

`REDIS_HOST=localhost prove -lr t/` — all 69 still pass.

- [ ] **Step 6: Commit.**

```bash
git add dist.ini cpanfile README.md FEATURE-channels-parity.md
git commit -m "deps: rename distribution to PAGI-Middleware-Channels; update README and FEATURE doc"
```

---

### Task 5: final verification

**Files:** none — verification only.

- [ ] **Step 1: Tests.**

```bash
bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && REDIS_HOST=localhost prove -lr t/ 2>&1 | tail -5'
```

Expected: `Files=16, Tests=69, … Result: PASS`.

- [ ] **Step 2: Examples syntax.**

```bash
bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && perl -Ilib -c examples/chat/app.pl && perl -Ilib -c examples/task-queue/server.pl'
```

Expected: both syntax OK.

- [ ] **Step 3: Stale-name grep over code.**

```bash
grep -rnE 'PAGI::Channels::(Backend|Interface)|package PAGI::Channels[^:]' lib/ t/ examples/
```

Expected: zero matches *for code*. The new `lib/PAGI/Channels.pm` declares `package PAGI::Channels;` (with semicolon, not `::`); the negative-lookahead regex above filters it out via `PAGI::Channels[^:]`. POD inside `lib/PAGI/Middleware/Channels.pm` and `lib/PAGI/Channels.pm` legitimately mentions both names — that's expected.

- [ ] **Step 4: POD check.**

```bash
podchecker lib/PAGI/Channels.pm \
            lib/PAGI/Middleware/Channels.pm \
            lib/PAGI/Middleware/Channels/Backend.pm \
            lib/PAGI/Middleware/Channels/Backend/Memory.pm \
            lib/PAGI/Middleware/Channels/Backend/Redis.pm
```

Expected: each `pod syntax OK`.

- [ ] **Step 5: Distribution-name check.**

```bash
grep -E '^name' dist.ini
head -1 cpanfile
head -1 README.md
```

Expected: `dist.ini` reports `name    = PAGI-Middleware-Channels`; `cpanfile` header mentions `PAGI-Middleware-Channels`; `README.md` title is `# PAGI-Middleware-Channels`.

- [ ] **Step 6: Old-tree-empty check.**

```bash
ls lib/PAGI/Channels/ 2>/dev/null
```

Expected: nothing (the directory should not exist any more — the only remaining `PAGI::Channels` thing is the helper file `lib/PAGI/Channels.pm`, which lives at the `PAGI/` level, not inside `Channels/`).

- [ ] **Step 7: If anything fails, fix and commit; otherwise this task is complete.**

---

## Summary

| Task | Scope | Est lines changed |
|------|-------|-------------------|
| 1 | Move backends under `PAGI::Middleware::Channels::Backend::*` | ~30 across 3 modules + ~30 across 14 test files |
| 2 | Split facade into `PAGI::Middleware::Channels` + `PAGI::Channels` (helper) | ~440 (rewrite of two files; mostly copied content with package + POD updates), ~10 in 2 facade tests |
| 3 | Update both examples | ~10 |
| 4 | `dist.ini` + `cpanfile` + `README.md` + `FEATURE-channels-parity.md` | ~60 |
| 5 | Final verification | 0 |

Total: ~580 lines touched across ~25 files, 4 commits + verification.
