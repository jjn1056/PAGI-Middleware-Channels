# PAGI-Channels v1 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Django Channels-exceeding messaging layer with Presence, Patterns, Delays, and History features.

**Architecture:** Middleware wrapper pattern injecting `pagi.channels` into PAGI scopes. Memory backend for single-process, Redis backend for multi-process/multi-server. All library code uses Future::IO only (loop agnostic). Docker Compose provides Redis for tests and examples.

**Tech Stack:** Future::AsyncAwait, Future::IO, Role::Tiny, JSON::MaybeXS, Async::Redis (from CPAN), Test2::V0, IO::Async (tests only), Docker Compose (Redis)

---

## Critical Constraints

| Constraint | Rule |
|------------|------|
| **Loop Agnosticism** | `lib/` uses Future::IO ONLY. No IO::Async, Mojo, etc. |
| **Tests** | `t/` uses IO::Async via Future::IO backend |
| **Redis Testing** | Docker Compose for Redis (REDIS_HOST env var) |
| **Dependencies** | Use `Async::Redis` from CPAN (latest) |
| **Min Perl** | 5.018+ (signatures, postderef) |

---

## Task 1: Project Scaffolding

**Files:**
- Create: `lib/PAGI/Channels.pm`
- Create: `lib/PAGI/Channels/Backend.pm`
- Create: `t/lib/Test/PAGI/Channels.pm`
- Create: `t/00-load.t`
- Create: `cpanfile`
- Create: `dist.ini`
- Create: `t/docker-compose.yml`

**Step 1: Create cpanfile**

```perl
# cpanfile - PAGI-Channels dependencies

# Core
requires 'perl', '5.018';
requires 'Future::AsyncAwait', '0.66';
requires 'Future::IO', '0.15';
requires 'Role::Tiny', '2.002004';
requires 'JSON::MaybeXS', '1.004005';
requires 'namespace::clean';

# Optional Redis backend
recommends 'Async::Redis', '0.001003';  # Latest from CPAN
recommends 'Sereal::Encoder', '5.004';
recommends 'Sereal::Decoder', '5.004';

# Testing
on 'test' => sub {
    requires 'Test2::V0';
    requires 'Test::Lib';
    requires 'IO::Async', '0.802';
    requires 'Future::IO::Impl::IOAsync';
    requires 'Devel::Cover';
};
```

**Step 2: Create dist.ini**

```ini
name    = PAGI-Channels
author  = John Napiorkowski <jnapiorkowski@cpan.org>
license = Perl_5
copyright_holder = John Napiorkowski

[@Basic]
[AutoPrereqs]
[MetaJSON]
[PodSyntaxTests]

[Prereqs]
perl = 5.018
Future::AsyncAwait = 0.66
Future::IO = 0.15
Role::Tiny = 2.002004
JSON::MaybeXS = 1.004005
namespace::clean = 0

[Prereqs / TestRequires]
Test2::V0 = 0
Test::Lib = 0
IO::Async = 0.802
```

**Step 3: Create t/docker-compose.yml**

```yaml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5
```

**Step 4: Create test helper t/lib/Test/PAGI/Channels.pm**

```perl
package Test::PAGI::Channels;
use strict;
use warnings;
use parent 'Exporter';
use Test2::V0;
use Future::IO;

our @EXPORT_OK = qw(
    init_loop
    get_loop
    run
    skip_without_redis
    redis_host
    redis_port
);
our %EXPORT_TAGS = (all => \@EXPORT_OK);

my $loop;

sub init_loop {
    require IO::Async::Loop;
    require Future::IO::Impl::IOAsync;
    $loop = IO::Async::Loop->new;
    Future::IO->override_impl(Future::IO::Impl::IOAsync->new(loop => $loop));
    return $loop;
}

sub get_loop { $loop }

sub run(&) {
    my ($code) = @_;
    my $f = $code->();
    return $f->get if $f && $f->isa('Future');
    return $f;
}

sub redis_host { $ENV{REDIS_HOST} // 'localhost' }
sub redis_port { $ENV{REDIS_PORT} // 6379 }

sub skip_without_redis {
    my $host = redis_host();
    my $port = redis_port();

    require IO::Socket::INET;
    my $sock = IO::Socket::INET->new(
        PeerHost => $host,
        PeerPort => $port,
        Timeout  => 2,
    );

    unless ($sock) {
        skip_all("Redis not available at $host:$port");
        return;
    }
    close $sock;
    return 1;
}

1;
```

**Step 5: Create Backend role lib/PAGI/Channels/Backend.pm**

```perl
package PAGI::Channels::Backend;
use strict;
use warnings;
use Role::Tiny;

# Core operations
requires qw(
    send
    poll
    subscribe
    unsubscribe
    publish
    flush
    cleanup
);

# Advanced features (v1)
requires qw(
    psubscribe
    punsubscribe
    track
    untrack
    list_presence
    send_delayed
    publish_delayed
    subscribe_with_history
);

1;

__END__

=head1 NAME

PAGI::Channels::Backend - Role for channel layer backends

=head1 REQUIRED METHODS

=head2 Core

=over 4

=item send($channel, $message) -> Future

=item poll($channel) -> $message | undef

=item subscribe($channel, $topic, %opts) -> Future

=item unsubscribe($channel, $topic) -> Future

=item publish($topic, $message, %opts) -> Future

=item flush() -> Future

=item cleanup($channel) -> Future

=back

=head2 Pattern Subscriptions

=over 4

=item psubscribe($channel, $pattern) -> Future

=item punsubscribe($channel, $pattern) -> Future

=back

=head2 Presence

=over 4

=item track($topic, $presence_data) -> Future

=item untrack($topic) -> Future

=item list_presence($topic) -> Future[@presences]

=back

=head2 Delayed Messages

=over 4

=item send_delayed($channel, $message, $delay_seconds) -> Future

=item publish_delayed($topic, $message, $delay_seconds) -> Future

=back

=head2 History

=over 4

=item subscribe_with_history($channel, $topic, $history_count, %opts) -> Future

=back

=cut
```

**Step 6: Create stub lib/PAGI/Channels.pm**

```perl
package PAGI::Channels;
use strict;
use warnings;
use Future::AsyncAwait;

our $VERSION = '0.001';

sub new {
    my ($class, %args) = @_;

    my $backend_uri = $args{backend}
        // $ENV{PAGI_CHANNELS_BACKEND}
        // 'memory://';

    my $self = bless {
        backend_uri => $backend_uri,
        _backend    => undef,
        _counter    => 0,
    }, $class;

    $self->_init_backend($backend_uri);

    return $self;
}

sub _init_backend {
    my ($self, $uri) = @_;

    if ($uri =~ /^memory:/) {
        require PAGI::Channels::Backend::Memory;
        $self->{_backend} = PAGI::Channels::Backend::Memory->new();
    }
    elsif ($uri =~ /^redis:/) {
        require PAGI::Channels::Backend::Redis;
        $self->{_backend} = PAGI::Channels::Backend::Redis->new(uri => $uri);
    }
    else {
        die "Unknown backend: $uri";
    }
}

sub backend { shift->{_backend} }

1;

__END__

=head1 NAME

PAGI::Channels - Cross-process messaging for PAGI applications

=head1 SYNOPSIS

    use PAGI::Channels;

    my $channels = PAGI::Channels->new(
        backend => 'memory://',  # or 'redis://localhost:6379'
    );

    my $app = $channels->wrap(async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = $scope->{'pagi.channels'};
        await $ch->subscribe("chat.room1");
        # ...
    });

=head1 DESCRIPTION

PAGI-Channels provides cross-process and cross-server messaging for PAGI
applications. It exceeds Django Channels with built-in:

=over 4

=item * Presence tracking (who's online)

=item * Pattern subscriptions (chat.* matches chat.room1, chat.room2)

=item * Delayed messages (send after N seconds)

=item * Message history (new subscribers get last N messages)

=back

B<IMPORTANT:> This library is event-loop agnostic. All I/O uses Future::IO
primitives. It works with IO::Async, Mojo::IOLoop, UV, or any Future::IO
backend.

=cut
```

**Step 7: Create basic load test t/00-load.t**

```perl
use strict;
use warnings;
use Test::Lib;
use Test2::V0;

use_ok('PAGI::Channels');
use_ok('PAGI::Channels::Backend');

done_testing;
```

**Step 8: Run test to verify scaffold**

Run: `prove -l t/00-load.t`
Expected: PASS (2 tests)

**Step 9: Commit**

```bash
git add -A
git commit -m "feat: initial project scaffolding

- cpanfile with core and test dependencies
- dist.ini for Dist::Zilla
- Docker Compose for Redis tests
- Test helper module
- Backend role definition
- PAGI::Channels stub with backend selection"
```

---

## Task 2: Memory Backend Core Operations

**Files:**
- Create: `lib/PAGI/Channels/Backend/Memory.pm`
- Create: `t/10-memory/01-core.t`

**Step 1: Write failing test for send/poll**

```perl
# t/10-memory/01-core.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use_ok('PAGI::Channels::Backend::Memory');

subtest 'send and poll' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    # Initially empty
    my $msg = $backend->poll('test.channel');
    is($msg, undef, 'poll on empty channel returns undef');

    # Send message
    run { $backend->send('test.channel', { type => 'test', data => 1 }) };

    # Poll receives it
    $msg = $backend->poll('test.channel');
    is($msg, { type => 'test', data => 1 }, 'poll returns sent message');

    # Now empty again
    $msg = $backend->poll('test.channel');
    is($msg, undef, 'poll after consume returns undef');
};

subtest 'FIFO ordering' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->send('ch', { type => 'msg', n => 1 }) };
    run { $backend->send('ch', { type => 'msg', n => 2 }) };
    run { $backend->send('ch', { type => 'msg', n => 3 }) };

    is($backend->poll('ch')->{n}, 1, 'first message');
    is($backend->poll('ch')->{n}, 2, 'second message');
    is($backend->poll('ch')->{n}, 3, 'third message');
    is($backend->poll('ch'), undef, 'queue empty');
};

subtest 'capacity limit' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new(capacity => 3);

    run { $backend->send('ch', { type => 'msg', n => 1 }) };
    run { $backend->send('ch', { type => 'msg', n => 2 }) };
    run { $backend->send('ch', { type => 'msg', n => 3 }) };

    # Fourth should fail
    my $result = run {
        $backend->send('ch', { type => 'msg', n => 4 })->catch(sub {
            my ($cat) = @_;
            return { error => $cat };
        });
    };
    is($result->{error}, 'ChannelFull', 'send to full channel fails');
};

done_testing;
```

**Step 2: Run test to verify it fails**

Run: `prove -lv t/10-memory/01-core.t`
Expected: FAIL (module not found)

**Step 3: Implement Memory backend core**

```perl
# lib/PAGI/Channels/Backend/Memory.pm
package PAGI::Channels::Backend::Memory;
use strict;
use warnings;
use Future::AsyncAwait;
use Future;
use Role::Tiny::With;
use namespace::clean;

with 'PAGI::Channels::Backend';

# Defaults
use constant {
    DEFAULT_CAPACITY     => 100,
    DEFAULT_EXPIRY       => 60,
    DEFAULT_GROUP_EXPIRY => 86400,
    DEFAULT_MAX_SIZE     => 1_048_576,
    DEFAULT_HISTORY_SIZE => 0,
};

sub new {
    my ($class, %args) = @_;

    return bless {
        # Config
        capacity     => $args{capacity}     // DEFAULT_CAPACITY,
        expiry       => $args{expiry}       // DEFAULT_EXPIRY,
        group_expiry => $args{group_expiry} // DEFAULT_GROUP_EXPIRY,
        max_size     => $args{max_size}     // DEFAULT_MAX_SIZE,
        history_size => $args{history_size} // DEFAULT_HISTORY_SIZE,

        # State
        queues       => {},  # channel -> [ {msg, expires} ]
        groups       => {},  # topic -> { channel -> expires }
        patterns     => {},  # channel -> [ {pattern, regex} ]
        presence     => {},  # topic -> { channel -> {data, expires} }
        history      => {},  # topic -> [ {msg, timestamp} ]
        delayed      => [],  # [ {time, type, target, msg} ]

        # Internal
        _channel_id  => undef,  # Set by facade for presence
    }, $class;
}

# Core: send
async sub send {
    my ($self, $channel, $message) = @_;

    $self->_validate_channel($channel);
    $self->_validate_message($message);

    $self->{queues}{$channel} //= [];
    my $queue = $self->{queues}{$channel};

    # Capacity check
    if (@$queue >= $self->{capacity}) {
        return Future->fail('ChannelFull', 'channel', $channel);
    }

    push @$queue, {
        msg     => $message,
        expires => time() + $self->{expiry},
    };

    return Future->done(1);
}

# Core: poll (synchronous)
sub poll {
    my ($self, $channel) = @_;

    my $queue = $self->{queues}{$channel} or return undef;

    # Remove expired messages
    my $now = time();
    while (@$queue && $queue->[0]{expires} < $now) {
        shift @$queue;
    }

    return undef unless @$queue;

    my $entry = shift @$queue;
    return $entry->{msg};
}

# Validation helpers
sub _validate_channel {
    my ($self, $channel) = @_;

    die "InvalidChannelName: empty" unless defined $channel && length $channel;
    die "InvalidChannelName: too long" if length $channel > 100;
    die "InvalidChannelName: bad chars" unless $channel =~ /^[\w.\-:]+$/;
}

sub _validate_message {
    my ($self, $message) = @_;

    die "InvalidMessage: not a hashref" unless ref $message eq 'HASH';
    die "InvalidMessage: missing type" unless defined $message->{type};
}

# Stubs for required methods (implemented in later tasks)
async sub subscribe { Future->done(1) }
async sub unsubscribe { Future->done(1) }
async sub publish { Future->done(1) }
async sub flush {
    my ($self) = @_;
    $self->{queues} = {};
    $self->{groups} = {};
    $self->{patterns} = {};
    $self->{presence} = {};
    $self->{history} = {};
    $self->{delayed} = [];
    return Future->done(1);
}
async sub cleanup { Future->done(1) }
async sub psubscribe { Future->done(1) }
async sub punsubscribe { Future->done(1) }
async sub track { Future->done(1) }
async sub untrack { Future->done(1) }
async sub list_presence { Future->done([]) }
async sub send_delayed { Future->done(1) }
async sub publish_delayed { Future->done(1) }
async sub subscribe_with_history { Future->done(1) }

1;
```

**Step 4: Run test to verify it passes**

Run: `prove -lv t/10-memory/01-core.t`
Expected: PASS

**Step 5: Commit**

```bash
git add -A
git commit -m "feat(memory): implement send/poll with capacity limits"
```

---

## Task 3: Memory Backend PubSub

**Files:**
- Modify: `lib/PAGI/Channels/Backend/Memory.pm`
- Create: `t/10-memory/02-pubsub.t`

**Step 1: Write failing test for subscribe/publish**

```perl
# t/10-memory/02-pubsub.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Channels::Backend::Memory;

subtest 'subscribe and publish' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    # Subscribe two channels to a topic
    run { $backend->subscribe('ch1', 'room.general') };
    run { $backend->subscribe('ch2', 'room.general') };

    # Publish to topic
    run { $backend->publish('room.general', { type => 'chat', text => 'hello' }) };

    # Both receive
    my $msg1 = $backend->poll('ch1');
    my $msg2 = $backend->poll('ch2');

    is($msg1->{text}, 'hello', 'ch1 received');
    is($msg2->{text}, 'hello', 'ch2 received');
};

subtest 'publish with exclude' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->subscribe('ch1', 'room') };
    run { $backend->subscribe('ch2', 'room') };
    run { $backend->subscribe('ch3', 'room') };

    # Publish excluding ch2
    run { $backend->publish('room', { type => 'msg' }, exclude => 'ch2') };

    ok($backend->poll('ch1'), 'ch1 received');
    is($backend->poll('ch2'), undef, 'ch2 excluded');
    ok($backend->poll('ch3'), 'ch3 received');
};

subtest 'publish to full channel drops silently' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new(capacity => 1);

    run { $backend->subscribe('ch1', 'room') };

    # Fill ch1
    run { $backend->send('ch1', { type => 'fill' }) };

    # Publish should not die even though ch1 is full
    my $ok = run { $backend->publish('room', { type => 'dropped' }) };
    is($ok, 1, 'publish succeeds even with full subscriber');

    # ch1 still only has original message
    is($backend->poll('ch1')->{type}, 'fill', 'original message');
    is($backend->poll('ch1'), undef, 'broadcast was dropped');
};

subtest 'unsubscribe' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->subscribe('ch1', 'room') };
    run { $backend->unsubscribe('ch1', 'room') };
    run { $backend->publish('room', { type => 'msg' }) };

    is($backend->poll('ch1'), undef, 'unsubscribed channel does not receive');
};

subtest 'subscribe is idempotent' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->subscribe('ch1', 'room') };
    run { $backend->subscribe('ch1', 'room') };  # duplicate
    run { $backend->publish('room', { type => 'msg' }) };

    ok($backend->poll('ch1'), 'received once');
    is($backend->poll('ch1'), undef, 'no duplicate');
};

done_testing;
```

**Step 2: Run test to verify it fails**

Run: `prove -lv t/10-memory/02-pubsub.t`
Expected: FAIL (subscribe doesn't actually work)

**Step 3: Implement subscribe/unsubscribe/publish**

Add to `lib/PAGI/Channels/Backend/Memory.pm`:

```perl
# Replace the stub async sub subscribe
async sub subscribe {
    my ($self, $channel, $topic, %opts) = @_;

    $self->_validate_channel($channel);
    $self->_validate_channel($topic);  # Same rules for topics

    $self->{groups}{$topic} //= {};
    $self->{groups}{$topic}{$channel} = time() + $self->{group_expiry};

    return Future->done(1);
}

# Replace the stub async sub unsubscribe
async sub unsubscribe {
    my ($self, $channel, $topic) = @_;

    if ($self->{groups}{$topic}) {
        delete $self->{groups}{$topic}{$channel};
    }

    return Future->done(1);
}

# Replace the stub async sub publish
async sub publish {
    my ($self, $topic, $message, %opts) = @_;

    $self->_validate_channel($topic);
    $self->_validate_message($message);

    my $exclude = $opts{exclude} // [];
    $exclude = [$exclude] unless ref $exclude eq 'ARRAY';
    my %excluded = map { $_ => 1 } @$exclude;

    my $members = $self->{groups}{$topic} // {};
    my $now = time();

    for my $channel (keys %$members) {
        # Skip expired memberships
        next if $members->{$channel} < $now;

        # Skip excluded
        next if $excluded{$channel};

        # Send (silently drop if full)
        $self->{queues}{$channel} //= [];
        my $queue = $self->{queues}{$channel};

        if (@$queue < $self->{capacity}) {
            push @$queue, {
                msg     => $message,
                expires => $now + $self->{expiry},
            };
        }
        # else: silently drop (at-most-once semantics)
    }

    return Future->done(1);
}
```

**Step 4: Run test to verify it passes**

Run: `prove -lv t/10-memory/02-pubsub.t`
Expected: PASS

**Step 5: Commit**

```bash
git commit -am "feat(memory): implement subscribe/unsubscribe/publish"
```

---

## Task 4: Memory Backend Pattern Subscriptions

**Files:**
- Modify: `lib/PAGI/Channels/Backend/Memory.pm`
- Create: `t/10-memory/03-patterns.t`

**Step 1: Write failing test for pattern subscriptions**

```perl
# t/10-memory/03-patterns.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Channels::Backend::Memory;

subtest 'single-level wildcard (*)' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    # chat.* matches chat.room1, chat.general, NOT chat.room1.messages
    run { $backend->psubscribe('ch1', 'chat.*') };

    run { $backend->publish('chat.room1', { type => 'msg', n => 1 }) };
    run { $backend->publish('chat.general', { type => 'msg', n => 2 }) };
    run { $backend->publish('chat.room1.messages', { type => 'msg', n => 3 }) };
    run { $backend->publish('notifications', { type => 'msg', n => 4 }) };

    is($backend->poll('ch1')->{n}, 1, 'chat.room1 matched');
    is($backend->poll('ch1')->{n}, 2, 'chat.general matched');
    is($backend->poll('ch1'), undef, 'chat.room1.messages NOT matched');
};

subtest 'multi-level wildcard (**)' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    # notifications.** matches notifications, notifications.user, notifications.user.123
    run { $backend->psubscribe('ch1', 'notifications.**') };

    run { $backend->publish('notifications', { type => 'msg', n => 1 }) };
    run { $backend->publish('notifications.user', { type => 'msg', n => 2 }) };
    run { $backend->publish('notifications.user.123.email', { type => 'msg', n => 3 }) };
    run { $backend->publish('alerts', { type => 'msg', n => 4 }) };

    is($backend->poll('ch1')->{n}, 1, 'notifications matched');
    is($backend->poll('ch1')->{n}, 2, 'notifications.user matched');
    is($backend->poll('ch1')->{n}, 3, 'notifications.user.123.email matched');
    is($backend->poll('ch1'), undef, 'alerts NOT matched');
};

subtest 'punsubscribe' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->psubscribe('ch1', 'events.*') };
    run { $backend->punsubscribe('ch1', 'events.*') };
    run { $backend->publish('events.click', { type => 'msg' }) };

    is($backend->poll('ch1'), undef, 'punsubscribed pattern no longer matches');
};

subtest 'mixed exact and pattern subscriptions' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    # Exact subscription
    run { $backend->subscribe('ch1', 'room.vip') };
    # Pattern subscription
    run { $backend->psubscribe('ch1', 'room.*') };

    run { $backend->publish('room.vip', { type => 'msg' }) };

    # Should only receive once (dedup)
    ok($backend->poll('ch1'), 'received message');
    is($backend->poll('ch1'), undef, 'no duplicate from pattern');
};

done_testing;
```

**Step 2: Run test to verify it fails**

Run: `prove -lv t/10-memory/03-patterns.t`
Expected: FAIL

**Step 3: Implement pattern subscriptions**

Add to `lib/PAGI/Channels/Backend/Memory.pm`:

```perl
# Helper to convert pattern to regex
sub _pattern_to_regex {
    my ($self, $pattern) = @_;

    # Escape special regex chars except our wildcards
    my $regex = quotemeta($pattern);

    # ** matches zero or more segments (including dots)
    $regex =~ s/\\\*\\\*/.*?/g;

    # * matches exactly one segment (no dots)
    $regex =~ s/\\\*/[^.]+/g;

    return qr/^$regex$/;
}

# Replace stub psubscribe
async sub psubscribe {
    my ($self, $channel, $pattern) = @_;

    $self->_validate_channel($channel);

    my $regex = $self->_pattern_to_regex($pattern);

    $self->{patterns}{$channel} //= [];

    # Avoid duplicate patterns
    for my $p (@{$self->{patterns}{$channel}}) {
        return Future->done(1) if $p->{pattern} eq $pattern;
    }

    push @{$self->{patterns}{$channel}}, {
        pattern => $pattern,
        regex   => $regex,
    };

    return Future->done(1);
}

# Replace stub punsubscribe
async sub punsubscribe {
    my ($self, $channel, $pattern) = @_;

    if ($self->{patterns}{$channel}) {
        $self->{patterns}{$channel} = [
            grep { $_->{pattern} ne $pattern } @{$self->{patterns}{$channel}}
        ];
    }

    return Future->done(1);
}
```

**Step 4: Update publish to check patterns**

Modify the `publish` method:

```perl
async sub publish {
    my ($self, $topic, $message, %opts) = @_;

    $self->_validate_channel($topic);
    $self->_validate_message($message);

    my $exclude = $opts{exclude} // [];
    $exclude = [$exclude] unless ref $exclude eq 'ARRAY';
    my %excluded = map { $_ => 1 } @$exclude;

    my $now = time();
    my %delivered;  # Track to avoid duplicates

    # Direct group subscribers
    my $members = $self->{groups}{$topic} // {};
    for my $channel (keys %$members) {
        next if $members->{$channel} < $now;
        next if $excluded{$channel};
        $self->_deliver_to_channel($channel, $message, $now);
        $delivered{$channel} = 1;
    }

    # Pattern subscribers
    for my $channel (keys %{$self->{patterns}}) {
        next if $excluded{$channel};
        next if $delivered{$channel};  # Already delivered via exact match

        for my $p (@{$self->{patterns}{$channel}}) {
            if ($topic =~ $p->{regex}) {
                $self->_deliver_to_channel($channel, $message, $now);
                $delivered{$channel} = 1;
                last;  # Only deliver once per channel
            }
        }
    }

    return Future->done(1);
}

# Helper for delivery
sub _deliver_to_channel {
    my ($self, $channel, $message, $now) = @_;

    $self->{queues}{$channel} //= [];
    my $queue = $self->{queues}{$channel};

    if (@$queue < $self->{capacity}) {
        push @$queue, {
            msg     => $message,
            expires => $now + $self->{expiry},
        };
    }
}
```

**Step 5: Run test to verify it passes**

Run: `prove -lv t/10-memory/03-patterns.t`
Expected: PASS

**Step 6: Commit**

```bash
git commit -am "feat(memory): implement pattern subscriptions with * and **"
```

---

## Task 5: Memory Backend Presence Tracking

**Files:**
- Modify: `lib/PAGI/Channels/Backend/Memory.pm`
- Create: `t/10-memory/04-presence.t`

**Step 1: Write failing test for presence**

```perl
# t/10-memory/04-presence.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Channels::Backend::Memory;

subtest 'explicit track/untrack' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();
    $backend->set_channel_id('worker.1');

    run { $backend->track('workers.pool', { worker_id => 1, started => 1000 }) };

    my @presence = run { $backend->list_presence('workers.pool') };
    is(scalar @presence, 1, 'one presence entry');
    is($presence[0]->{worker_id}, 1, 'correct data');

    run { $backend->untrack('workers.pool') };

    @presence = run { $backend->list_presence('workers.pool') };
    is(scalar @presence, 0, 'presence removed');
};

subtest 'subscribe with presence option' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();
    $backend->set_channel_id('user.alice');

    run { $backend->subscribe('user.alice', 'chat.room1',
        presence => { user => 'alice', status => 'online' }
    )};

    my @presence = run { $backend->list_presence('chat.room1') };
    is(scalar @presence, 1, 'presence tracked via subscribe');
    is($presence[0]->{user}, 'alice', 'correct user');
    is($presence[0]->{status}, 'online', 'correct status');
};

subtest 'presence events on join/leave' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    # Subscribe ch1 first (to receive events)
    $backend->set_channel_id('ch1');
    run { $backend->subscribe('ch1', 'room', presence => { user => 'ch1' }) };

    # Now ch2 joins - ch1 should get presence.join event
    $backend->set_channel_id('ch2');
    run { $backend->subscribe('ch2', 'room', presence => { user => 'ch2' }) };

    # Check ch1 received join event
    my $event = $backend->poll('ch1');
    is($event->{type}, 'presence.join', 'presence.join event');
    is($event->{presence}{user}, 'ch2', 'correct joiner');

    # ch2 leaves - ch1 should get presence.leave event
    run { $backend->unsubscribe('ch2', 'room') };

    $event = $backend->poll('ch1');
    is($event->{type}, 'presence.leave', 'presence.leave event');
    is($event->{presence}{user}, 'ch2', 'correct leaver');
};

subtest 'list_presence returns all current' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    $backend->set_channel_id('u1');
    run { $backend->subscribe('u1', 'room', presence => { name => 'Alice' }) };

    $backend->set_channel_id('u2');
    run { $backend->subscribe('u2', 'room', presence => { name => 'Bob' }) };

    $backend->set_channel_id('u3');
    run { $backend->subscribe('u3', 'room', presence => { name => 'Carol' }) };

    my @presence = run { $backend->list_presence('room') };
    is(scalar @presence, 3, 'three users present');

    my @names = sort map { $_->{name} } @presence;
    is(\@names, ['Alice', 'Bob', 'Carol'], 'correct names');
};

done_testing;
```

**Step 2: Run test to verify it fails**

Run: `prove -lv t/10-memory/04-presence.t`
Expected: FAIL

**Step 3: Implement presence tracking**

Add to `lib/PAGI/Channels/Backend/Memory.pm`:

```perl
# Add channel_id setter (called by facade)
sub set_channel_id {
    my ($self, $channel_id) = @_;
    $self->{_channel_id} = $channel_id;
}

sub channel_id { shift->{_channel_id} }

# Update subscribe to handle presence option
async sub subscribe {
    my ($self, $channel, $topic, %opts) = @_;

    $self->_validate_channel($channel);
    $self->_validate_channel($topic);

    my $now = time();
    my $is_new = !exists $self->{groups}{$topic}{$channel};

    $self->{groups}{$topic} //= {};
    $self->{groups}{$topic}{$channel} = $now + $self->{group_expiry};

    # Handle presence option
    if (my $presence_data = $opts{presence}) {
        $self->{presence}{$topic} //= {};
        $self->{presence}{$topic}{$channel} = {
            data    => $presence_data,
            expires => $now + $self->{group_expiry},
        };

        # Broadcast presence.join to other subscribers (if new)
        if ($is_new) {
            await $self->_broadcast_presence_event($topic, 'presence.join', $presence_data, $channel);
        }
    }

    return Future->done(1);
}

# Update unsubscribe to handle presence
async sub unsubscribe {
    my ($self, $channel, $topic) = @_;

    my $presence_data;
    if ($self->{presence}{$topic} && $self->{presence}{$topic}{$channel}) {
        $presence_data = $self->{presence}{$topic}{$channel}{data};
        delete $self->{presence}{$topic}{$channel};
    }

    if ($self->{groups}{$topic}) {
        delete $self->{groups}{$topic}{$channel};
    }

    # Broadcast presence.leave if had presence
    if ($presence_data) {
        await $self->_broadcast_presence_event($topic, 'presence.leave', $presence_data, $channel);
    }

    return Future->done(1);
}

# Helper for presence events
async sub _broadcast_presence_event {
    my ($self, $topic, $event_type, $presence_data, $exclude_channel) = @_;

    my $event = {
        type     => $event_type,
        topic    => $topic,
        presence => $presence_data,
    };

    await $self->publish($topic, $event, exclude => $exclude_channel);
}

# Replace stub track
async sub track {
    my ($self, $topic, $presence_data) = @_;

    my $channel = $self->{_channel_id}
        or die "track() requires set_channel_id() first";

    $self->_validate_channel($topic);

    my $now = time();
    $self->{presence}{$topic} //= {};
    $self->{presence}{$topic}{$channel} = {
        data    => $presence_data,
        expires => $now + $self->{group_expiry},
    };

    return Future->done(1);
}

# Replace stub untrack
async sub untrack {
    my ($self, $topic) = @_;

    my $channel = $self->{_channel_id}
        or die "untrack() requires set_channel_id() first";

    if ($self->{presence}{$topic}) {
        delete $self->{presence}{$topic}{$channel};
    }

    return Future->done(1);
}

# Replace stub list_presence
async sub list_presence {
    my ($self, $topic) = @_;

    my $entries = $self->{presence}{$topic} // {};
    my $now = time();

    my @result;
    for my $channel (keys %$entries) {
        my $entry = $entries->{$channel};
        next if $entry->{expires} < $now;
        push @result, $entry->{data};
    }

    return @result;
}
```

**Step 4: Run test to verify it passes**

Run: `prove -lv t/10-memory/04-presence.t`
Expected: PASS

**Step 5: Commit**

```bash
git commit -am "feat(memory): implement presence tracking with join/leave events"
```

---

## Task 6: Memory Backend Delayed Messages

**Files:**
- Modify: `lib/PAGI/Channels/Backend/Memory.pm`
- Create: `t/10-memory/05-delayed.t`

**Step 1: Write failing test for delayed messages**

```perl
# t/10-memory/05-delayed.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run get_loop);
use Test2::V0;

my $loop = init_loop();

use PAGI::Channels::Backend::Memory;

subtest 'send_delayed delivers after delay' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    # Send with 0.1 second delay
    run { $backend->send_delayed('ch1', { type => 'delayed' }, 0.1) };

    # Not delivered immediately
    is($backend->poll('ch1'), undef, 'not delivered immediately');

    # Process delayed messages
    run { $backend->process_delayed() };
    is($backend->poll('ch1'), undef, 'still not delivered');

    # Wait and process again
    run {
        my $timer = Future::IO->sleep(0.15);
        await $timer;
    };
    run { $backend->process_delayed() };

    my $msg = $backend->poll('ch1');
    is($msg->{type}, 'delayed', 'delivered after delay');
};

subtest 'publish_delayed delivers to all subscribers after delay' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->subscribe('ch1', 'room') };
    run { $backend->subscribe('ch2', 'room') };

    run { $backend->publish_delayed('room', { type => 'broadcast' }, 0.1) };

    # Not delivered immediately
    is($backend->poll('ch1'), undef, 'ch1 not delivered yet');
    is($backend->poll('ch2'), undef, 'ch2 not delivered yet');

    # Wait and process
    run {
        my $timer = Future::IO->sleep(0.15);
        await $timer;
    };
    run { $backend->process_delayed() };

    is($backend->poll('ch1')->{type}, 'broadcast', 'ch1 received');
    is($backend->poll('ch2')->{type}, 'broadcast', 'ch2 received');
};

subtest 'multiple delayed messages in order' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->send_delayed('ch', { n => 1 }, 0.05) };
    run { $backend->send_delayed('ch', { n => 2 }, 0.15) };
    run { $backend->send_delayed('ch', { n => 3 }, 0.10) };

    # Wait for all
    run {
        my $timer = Future::IO->sleep(0.2);
        await $timer;
    };
    run { $backend->process_delayed() };

    # Should arrive in delay order: 1, 3, 2
    is($backend->poll('ch')->{n}, 1, 'first (0.05s)');
    is($backend->poll('ch')->{n}, 3, 'second (0.10s)');
    is($backend->poll('ch')->{n}, 2, 'third (0.15s)');
};

done_testing;
```

**Step 2: Run test to verify it fails**

Run: `prove -lv t/10-memory/05-delayed.t`
Expected: FAIL

**Step 3: Implement delayed messages**

Add to `lib/PAGI/Channels/Backend/Memory.pm`:

```perl
# Replace stub send_delayed
async sub send_delayed {
    my ($self, $channel, $message, $delay_seconds) = @_;

    $self->_validate_channel($channel);
    $self->_validate_message($message);

    my $deliver_at = time() + $delay_seconds;

    push @{$self->{delayed}}, {
        deliver_at => $deliver_at,
        type       => 'send',
        target     => $channel,
        message    => $message,
    };

    # Keep sorted by delivery time
    @{$self->{delayed}} = sort { $a->{deliver_at} <=> $b->{deliver_at} } @{$self->{delayed}};

    return Future->done(1);
}

# Replace stub publish_delayed
async sub publish_delayed {
    my ($self, $topic, $message, $delay_seconds) = @_;

    $self->_validate_channel($topic);
    $self->_validate_message($message);

    my $deliver_at = time() + $delay_seconds;

    push @{$self->{delayed}}, {
        deliver_at => $deliver_at,
        type       => 'publish',
        target     => $topic,
        message    => $message,
    };

    @{$self->{delayed}} = sort { $a->{deliver_at} <=> $b->{deliver_at} } @{$self->{delayed}};

    return Future->done(1);
}

# Process delayed messages (called periodically or on poll)
async sub process_delayed {
    my ($self) = @_;

    my $now = time();

    while (@{$self->{delayed}} && $self->{delayed}[0]{deliver_at} <= $now) {
        my $entry = shift @{$self->{delayed}};

        if ($entry->{type} eq 'send') {
            await $self->send($entry->{target}, $entry->{message});
        }
        elsif ($entry->{type} eq 'publish') {
            await $self->publish($entry->{target}, $entry->{message});
        }
    }

    return Future->done(1);
}
```

**Step 4: Run test to verify it passes**

Run: `prove -lv t/10-memory/05-delayed.t`
Expected: PASS

**Step 5: Commit**

```bash
git commit -am "feat(memory): implement delayed messages for send and publish"
```

---

## Task 7: Memory Backend Message History

**Files:**
- Modify: `lib/PAGI/Channels/Backend/Memory.pm`
- Create: `t/10-memory/06-history.t`

**Step 1: Write failing test for message history**

```perl
# t/10-memory/06-history.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Channels::Backend::Memory;

subtest 'subscribe_with_history receives last N messages' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new(history_size => 10);

    # Publish some messages first (no subscribers yet)
    run { $backend->publish('chat.room', { type => 'msg', n => 1 }) };
    run { $backend->publish('chat.room', { type => 'msg', n => 2 }) };
    run { $backend->publish('chat.room', { type => 'msg', n => 3 }) };

    # Now subscribe with history
    run { $backend->subscribe_with_history('ch1', 'chat.room', 10) };

    # Should have received historical messages
    is($backend->poll('ch1')->{n}, 1, 'history msg 1');
    is($backend->poll('ch1')->{n}, 2, 'history msg 2');
    is($backend->poll('ch1')->{n}, 3, 'history msg 3');
    is($backend->poll('ch1'), undef, 'no more');
};

subtest 'history respects count limit' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new(history_size => 100);

    for my $n (1..10) {
        run { $backend->publish('room', { type => 'msg', n => $n }) };
    }

    # Request only last 3
    run { $backend->subscribe_with_history('ch1', 'room', 3) };

    is($backend->poll('ch1')->{n}, 8, 'only last 3: msg 8');
    is($backend->poll('ch1')->{n}, 9, 'only last 3: msg 9');
    is($backend->poll('ch1')->{n}, 10, 'only last 3: msg 10');
    is($backend->poll('ch1'), undef, 'no more');
};

subtest 'history buffer respects global limit' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new(history_size => 5);

    for my $n (1..10) {
        run { $backend->publish('room', { type => 'msg', n => $n }) };
    }

    # Only last 5 are retained
    run { $backend->subscribe_with_history('ch1', 'room', 100) };

    is($backend->poll('ch1')->{n}, 6, 'buffer only has 6-10');
    is($backend->poll('ch1')->{n}, 7, 'msg 7');
    is($backend->poll('ch1')->{n}, 8, 'msg 8');
    is($backend->poll('ch1')->{n}, 9, 'msg 9');
    is($backend->poll('ch1')->{n}, 10, 'msg 10');
    is($backend->poll('ch1'), undef, 'no more');
};

subtest 'new messages after subscribe arrive normally' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new(history_size => 10);

    run { $backend->publish('room', { type => 'history', n => 1 }) };
    run { $backend->subscribe_with_history('ch1', 'room', 10) };

    # Consume history
    $backend->poll('ch1');

    # New message
    run { $backend->publish('room', { type => 'live', n => 2 }) };

    my $msg = $backend->poll('ch1');
    is($msg->{type}, 'live', 'live message received');
    is($msg->{n}, 2, 'correct content');
};

done_testing;
```

**Step 2: Run test to verify it fails**

Run: `prove -lv t/10-memory/06-history.t`
Expected: FAIL

**Step 3: Implement message history**

Modify `lib/PAGI/Channels/Backend/Memory.pm`:

```perl
# Update publish to store history
async sub publish {
    my ($self, $topic, $message, %opts) = @_;

    $self->_validate_channel($topic);
    $self->_validate_message($message);

    my $exclude = $opts{exclude} // [];
    $exclude = [$exclude] unless ref $exclude eq 'ARRAY';
    my %excluded = map { $_ => 1 } @$exclude;

    my $now = time();
    my %delivered;

    # Store in history buffer (if history enabled and not a presence event)
    if ($self->{history_size} > 0 && $message->{type} !~ /^presence\./) {
        $self->{history}{$topic} //= [];
        push @{$self->{history}{$topic}}, {
            message   => $message,
            timestamp => $now,
        };

        # Trim to history_size
        while (@{$self->{history}{$topic}} > $self->{history_size}) {
            shift @{$self->{history}{$topic}};
        }
    }

    # Direct group subscribers
    my $members = $self->{groups}{$topic} // {};
    for my $channel (keys %$members) {
        next if $members->{$channel} < $now;
        next if $excluded{$channel};
        $self->_deliver_to_channel($channel, $message, $now);
        $delivered{$channel} = 1;
    }

    # Pattern subscribers
    for my $channel (keys %{$self->{patterns}}) {
        next if $excluded{$channel};
        next if $delivered{$channel};

        for my $p (@{$self->{patterns}{$channel}}) {
            if ($topic =~ $p->{regex}) {
                $self->_deliver_to_channel($channel, $message, $now);
                $delivered{$channel} = 1;
                last;
            }
        }
    }

    return Future->done(1);
}

# Replace stub subscribe_with_history
async sub subscribe_with_history {
    my ($self, $channel, $topic, $history_count, %opts) = @_;

    $self->_validate_channel($channel);
    $self->_validate_channel($topic);

    my $now = time();

    # Deliver history first
    if ($history_count > 0 && $self->{history}{$topic}) {
        my @history = @{$self->{history}{$topic}};

        # Take last N
        if (@history > $history_count) {
            @history = @history[-$history_count..-1];
        }

        for my $entry (@history) {
            $self->_deliver_to_channel($channel, $entry->{message}, $now);
        }
    }

    # Now do regular subscribe (with presence if provided)
    await $self->subscribe($channel, $topic, %opts);

    return Future->done(1);
}
```

**Step 4: Run test to verify it passes**

Run: `prove -lv t/10-memory/06-history.t`
Expected: PASS

**Step 5: Commit**

```bash
git commit -am "feat(memory): implement message history with configurable buffer"
```

---

## Task 8: Memory Backend Cleanup

**Files:**
- Modify: `lib/PAGI/Channels/Backend/Memory.pm`
- Create: `t/10-memory/07-cleanup.t`

**Step 1: Write failing test for cleanup**

```perl
# t/10-memory/07-cleanup.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Channels::Backend::Memory;

subtest 'cleanup removes channel from all groups' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->subscribe('ch1', 'room1') };
    run { $backend->subscribe('ch1', 'room2') };
    run { $backend->subscribe('ch1', 'room3') };

    run { $backend->cleanup('ch1') };

    # Publish to all rooms - ch1 should not receive
    run { $backend->publish('room1', { type => 'msg' }) };
    run { $backend->publish('room2', { type => 'msg' }) };
    run { $backend->publish('room3', { type => 'msg' }) };

    is($backend->poll('ch1'), undef, 'ch1 removed from all groups');
};

subtest 'cleanup removes pending messages' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->send('ch1', { type => 'msg1' }) };
    run { $backend->send('ch1', { type => 'msg2' }) };

    run { $backend->cleanup('ch1') };

    is($backend->poll('ch1'), undef, 'queue cleared');
};

subtest 'cleanup removes pattern subscriptions' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->psubscribe('ch1', 'events.*') };
    run { $backend->cleanup('ch1') };
    run { $backend->publish('events.click', { type => 'msg' }) };

    is($backend->poll('ch1'), undef, 'pattern subscription removed');
};

subtest 'cleanup removes presence and broadcasts leave' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    # ch2 subscribes first to receive events
    $backend->set_channel_id('ch2');
    run { $backend->subscribe('ch2', 'room', presence => { user => 'bob' }) };

    # ch1 subscribes with presence
    $backend->set_channel_id('ch1');
    run { $backend->subscribe('ch1', 'room', presence => { user => 'alice' }) };

    # Consume join event
    $backend->poll('ch2');

    # Cleanup ch1
    run { $backend->cleanup('ch1') };

    # ch2 should get leave event
    my $event = $backend->poll('ch2');
    is($event->{type}, 'presence.leave', 'leave event sent');
    is($event->{presence}{user}, 'alice', 'correct user');

    # Presence list should not include ch1
    my @presence = run { $backend->list_presence('room') };
    is(scalar @presence, 1, 'only one remaining');
    is($presence[0]->{user}, 'bob', 'bob remains');
};

subtest 'flush clears everything' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new(history_size => 10);

    run { $backend->subscribe('ch1', 'room') };
    run { $backend->psubscribe('ch2', 'events.*') };
    run { $backend->send('ch1', { type => 'msg' }) };
    run { $backend->publish('room', { type => 'msg' }) };

    run { $backend->flush() };

    is($backend->poll('ch1'), undef, 'queues cleared');
    is(scalar keys %{$backend->{groups}}, 0, 'groups cleared');
    is(scalar keys %{$backend->{patterns}}, 0, 'patterns cleared');
    is(scalar keys %{$backend->{presence}}, 0, 'presence cleared');
    is(scalar keys %{$backend->{history}}, 0, 'history cleared');
};

done_testing;
```

**Step 2: Run test to verify it fails**

Run: `prove -lv t/10-memory/07-cleanup.t`
Expected: FAIL

**Step 3: Implement cleanup**

Replace stub `cleanup` in `lib/PAGI/Channels/Backend/Memory.pm`:

```perl
async sub cleanup {
    my ($self, $channel) = @_;

    # Remove from all groups and handle presence
    for my $topic (keys %{$self->{groups}}) {
        if (delete $self->{groups}{$topic}{$channel}) {
            # Check if had presence
            if ($self->{presence}{$topic} && $self->{presence}{$topic}{$channel}) {
                my $presence_data = $self->{presence}{$topic}{$channel}{data};
                delete $self->{presence}{$topic}{$channel};

                # Broadcast leave event
                await $self->_broadcast_presence_event($topic, 'presence.leave', $presence_data, $channel);
            }
        }
    }

    # Remove pattern subscriptions
    delete $self->{patterns}{$channel};

    # Clear message queue
    delete $self->{queues}{$channel};

    return Future->done(1);
}
```

**Step 4: Run test to verify it passes**

Run: `prove -lv t/10-memory/07-cleanup.t`
Expected: PASS

**Step 5: Commit**

```bash
git commit -am "feat(memory): implement cleanup with presence leave events"
```

---

## Task 9: PAGI::Channels Facade

**Files:**
- Modify: `lib/PAGI/Channels.pm`
- Create: `t/20-facade/01-basic.t`

**Step 1: Write failing test for facade**

```perl
# t/20-facade/01-basic.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Channels;

subtest 'basic send/subscribe/publish flow' => sub {
    my $channels = PAGI::Channels->new();

    # Simulate two connections
    my $scope1 = {};
    my $scope2 = {};

    # Wrap creates channel names and injects scope keys
    # For now test backend directly via facade methods

    run { $channels->backend->subscribe('ch1', 'room') };
    run { $channels->backend->subscribe('ch2', 'room') };

    run { $channels->backend->publish('room', { type => 'msg', text => 'hello' }) };

    my $msg1 = $channels->backend->poll('ch1');
    my $msg2 = $channels->backend->poll('ch2');

    is($msg1->{text}, 'hello', 'ch1 received');
    is($msg2->{text}, 'hello', 'ch2 received');
};

subtest 'default memory backend' => sub {
    my $channels = PAGI::Channels->new();
    isa_ok($channels->backend, 'PAGI::Channels::Backend::Memory');
};

subtest 'env var backend selection' => sub {
    local $ENV{PAGI_CHANNELS_BACKEND} = 'memory://';
    my $channels = PAGI::Channels->new();
    isa_ok($channels->backend, 'PAGI::Channels::Backend::Memory');
};

done_testing;
```

**Step 2: Run test to verify it passes**

Run: `prove -lv t/20-facade/01-basic.t`
Expected: PASS (basic facade already works)

**Step 3: Commit**

```bash
git commit -am "test: add facade basic tests"
```

---

## Task 10: PAGI::Channels wrap() Method

**Files:**
- Modify: `lib/PAGI/Channels.pm`
- Create: `t/20-facade/02-wrap.t`

**Step 1: Write failing test for wrap**

```perl
# t/20-facade/02-wrap.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Channels;
use Future::AsyncAwait;

subtest 'wrap injects scope keys' => sub {
    my $channels = PAGI::Channels->new();

    my $captured_scope;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;
        $captured_scope = $scope;
    };

    my $wrapped = $channels->wrap($inner_app);

    # Simulate calling the wrapped app
    my $scope = { type => 'websocket' };
    my $receive = async sub { { type => 'websocket.disconnect' } };
    my $send = async sub { };

    run { $wrapped->($scope, $receive, $send) };

    ok(exists $captured_scope->{'pagi.channels'}, 'pagi.channels injected');
    ok(exists $captured_scope->{'pagi.channel'}, 'pagi.channel injected');
    like($captured_scope->{'pagi.channel'}, qr/^conn\./, 'channel name has conn. prefix');
};

subtest 'wrapped receive interleaves channel messages' => sub {
    my $channels = PAGI::Channels->new();

    my @received;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = $scope->{'pagi.channels'};
        my $my_channel = $scope->{'pagi.channel'};

        # Subscribe to a room
        await $ch->subscribe('room');

        # Have someone send us a message
        await $ch->backend->send($my_channel, { type => 'chat.msg', text => 'hello' });

        # Receive should get channel message first
        my $event = await $receive->();
        push @received, $event;

        # Then protocol event
        $event = await $receive->();
        push @received, $event;
    };

    my $wrapped = $channels->wrap($inner_app);

    my $protocol_event = { type => 'websocket.receive', text => 'from client' };
    my @protocol_events = ($protocol_event);

    my $scope = { type => 'websocket' };
    my $receive = async sub { shift @protocol_events };
    my $send = async sub { };

    run { $wrapped->($scope, $receive, $send) };

    is($received[0]->{type}, 'chat.msg', 'channel message first');
    is($received[1]->{type}, 'websocket.receive', 'protocol event second');
};

subtest 'cleanup on app exit' => sub {
    my $channels = PAGI::Channels->new();

    my $my_channel;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = $scope->{'pagi.channels'};
        $my_channel = $scope->{'pagi.channel'};

        await $ch->subscribe('room');
        # App exits
    };

    my $wrapped = $channels->wrap($inner_app);

    my $scope = { type => 'websocket' };
    my $receive = async sub { { type => 'websocket.disconnect' } };
    my $send = async sub { };

    run { $wrapped->($scope, $receive, $send) };

    # Publish to room - cleaned up channel should not receive
    run { $channels->backend->publish('room', { type => 'msg' }) };

    is($channels->backend->poll($my_channel), undef, 'channel cleaned up');
};

done_testing;
```

**Step 2: Run test to verify it fails**

Run: `prove -lv t/20-facade/02-wrap.t`
Expected: FAIL (wrap not implemented)

**Step 3: Implement wrap method**

Add to `lib/PAGI/Channels.pm`:

```perl
use Future::AsyncAwait;
use Future;

sub wrap {
    my ($self, $inner_app) = @_;

    return async sub {
        my ($scope, $receive, $send) = @_;

        # 1. Generate unique channel name
        my $channel_name = $self->_generate_channel_name();

        # 2. Inject scope keys
        $scope->{'pagi.channels'} = $self->_create_channel_interface($channel_name);
        $scope->{'pagi.channel'} = $channel_name;

        # 3. Wrap receive to interleave channel messages
        my $wrapped_receive = async sub {
            # Check channel queue first (non-blocking)
            if (my $msg = $self->{_backend}->poll($channel_name)) {
                return $msg;
            }
            # Fall through to protocol receive
            return await $receive->();
        };

        # 4. Call inner app with error handling
        my $err;
        eval { await $inner_app->($scope, $wrapped_receive, $send) };
        $err = $@;

        # 5. Always cleanup
        await $self->{_backend}->cleanup($channel_name);

        # Re-throw if error
        die $err if $err;
    };
}

sub _generate_channel_name {
    my ($self) = @_;
    $self->{_counter}++;
    return sprintf("conn.%d.%d.%d", $$, time(), $self->{_counter});
}

# Create a scoped interface for this channel
sub _create_channel_interface {
    my ($self, $channel_name) = @_;

    # Set channel_id on backend for presence operations
    $self->{_backend}->set_channel_id($channel_name);

    return PAGI::Channels::Interface->new(
        backend      => $self->{_backend},
        channel_name => $channel_name,
    );
}

# Nested class for per-connection interface
package PAGI::Channels::Interface;
use Future::AsyncAwait;

sub new {
    my ($class, %args) = @_;
    return bless \%args, $class;
}

sub backend { shift->{backend} }
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
*group_add = \&subscribe;
*group_discard = \&unsubscribe;
*group_send = \&publish;

1;
```

**Step 4: Run test to verify it passes**

Run: `prove -lv t/20-facade/02-wrap.t`
Expected: PASS

**Step 5: Commit**

```bash
git commit -am "feat: implement wrap() with channel interface and receive interleaving"
```

---

## Task 11: Redis Backend Foundation

**Files:**
- Create: `lib/PAGI/Channels/Backend/Redis.pm`
- Create: `t/30-redis/01-core.t`

**Step 1: Write failing test for Redis backend core**

```perl
# t/30-redis/01-core.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run skip_without_redis redis_host redis_port);
use Test2::V0;

my $loop = init_loop();

SKIP: {
    skip_without_redis();

    use_ok('PAGI::Channels::Backend::Redis');

    subtest 'connect to Redis' => sub {
        my $backend = PAGI::Channels::Backend::Redis->new(
            uri => "redis://" . redis_host() . ":" . redis_port(),
        );

        run { $backend->connect() };
        ok($backend->connected, 'connected to Redis');

        run { $backend->disconnect() };
    };

    subtest 'send and poll' => sub {
        my $backend = PAGI::Channels::Backend::Redis->new(
            uri => "redis://" . redis_host() . ":" . redis_port(),
        );
        run { $backend->connect() };

        # Clear any existing data
        run { $backend->flush() };

        # Send
        run { $backend->send('test:ch1', { type => 'msg', data => 'hello' }) };

        # Poll
        my $msg = run { $backend->poll('test:ch1') };
        is($msg->{type}, 'msg', 'received message type');
        is($msg->{data}, 'hello', 'received message data');

        # Poll again - empty
        $msg = run { $backend->poll('test:ch1') };
        is($msg, undef, 'queue now empty');

        run { $backend->disconnect() };
    };

    subtest 'FIFO ordering' => sub {
        my $backend = PAGI::Channels::Backend::Redis->new(
            uri => "redis://" . redis_host() . ":" . redis_port(),
        );
        run { $backend->connect() };
        run { $backend->flush() };

        run { $backend->send('test:ch', { type => 'msg', n => 1 }) };
        run { $backend->send('test:ch', { type => 'msg', n => 2 }) };
        run { $backend->send('test:ch', { type => 'msg', n => 3 }) };

        is(run { $backend->poll('test:ch') }->{n}, 1, 'first');
        is(run { $backend->poll('test:ch') }->{n}, 2, 'second');
        is(run { $backend->poll('test:ch') }->{n}, 3, 'third');

        run { $backend->disconnect() };
    };
}

done_testing;
```

**Step 2: Run test to verify it fails**

Run: `REDIS_HOST=localhost prove -lv t/30-redis/01-core.t`
Expected: FAIL (module not found)

**Step 3: Implement Redis backend foundation**

```perl
# lib/PAGI/Channels/Backend/Redis.pm
package PAGI::Channels::Backend::Redis;
use strict;
use warnings;
use Future::AsyncAwait;
use Future;
use Role::Tiny::With;
use JSON::MaybeXS qw(encode_json decode_json);
use namespace::clean;

with 'PAGI::Channels::Backend';

# Defaults
use constant {
    DEFAULT_CAPACITY     => 100,
    DEFAULT_EXPIRY       => 60,
    DEFAULT_GROUP_EXPIRY => 86400,
    DEFAULT_MAX_SIZE     => 1_048_576,
    DEFAULT_HISTORY_SIZE => 0,
    DEFAULT_PREFIX       => 'pagi:',
};

sub new {
    my ($class, %args) = @_;

    my $uri = $args{uri} // 'redis://localhost:6379';

    return bless {
        uri          => $uri,
        prefix       => $args{prefix}       // DEFAULT_PREFIX,
        capacity     => $args{capacity}     // DEFAULT_CAPACITY,
        expiry       => $args{expiry}       // DEFAULT_EXPIRY,
        group_expiry => $args{group_expiry} // DEFAULT_GROUP_EXPIRY,
        max_size     => $args{max_size}     // DEFAULT_MAX_SIZE,
        history_size => $args{history_size} // DEFAULT_HISTORY_SIZE,

        _redis       => undef,
        _channel_id  => undef,
        _connected   => 0,
    }, $class;
}

async sub connect {
    my ($self) = @_;

    require Async::Redis;

    # Parse URI for host/port
    my ($host, $port) = $self->_parse_uri($self->{uri});

    $self->{_redis} = Async::Redis->new(
        host => $host,
        port => $port,
    );

    await $self->{_redis}->connect();
    $self->{_connected} = 1;

    return Future->done(1);
}

sub _parse_uri {
    my ($self, $uri) = @_;

    if ($uri =~ m{^redis://([^:]+):(\d+)}) {
        return ($1, $2);
    }
    elsif ($uri =~ m{^redis://([^/]+)}) {
        return ($1, 6379);
    }
    return ('localhost', 6379);
}

async sub disconnect {
    my ($self) = @_;

    if ($self->{_redis}) {
        $self->{_redis}->disconnect();
        $self->{_redis} = undef;
    }
    $self->{_connected} = 0;

    return Future->done(1);
}

sub connected { shift->{_connected} }

sub set_channel_id {
    my ($self, $channel_id) = @_;
    $self->{_channel_id} = $channel_id;
}

# Key helpers
sub _queue_key  { shift->{prefix} . 'q:' . shift }
sub _group_key  { shift->{prefix} . 'g:' . shift }
sub _presence_key { shift->{prefix} . 'p:' . shift }
sub _history_key  { shift->{prefix} . 'h:' . shift }

# Core: send
async sub send {
    my ($self, $channel, $message) = @_;

    my $key = $self->_queue_key($channel);
    my $len = await $self->{_redis}->llen($key);

    if ($len >= $self->{capacity}) {
        return Future->fail('ChannelFull', 'channel', $channel);
    }

    my $json = encode_json($message);
    await $self->{_redis}->rpush($key, $json);
    await $self->{_redis}->expire($key, $self->{expiry});

    return Future->done(1);
}

# Core: poll
async sub poll {
    my ($self, $channel) = @_;

    my $key = $self->_queue_key($channel);
    my $json = await $self->{_redis}->lpop($key);

    return undef unless defined $json;
    return decode_json($json);
}

# Stubs for other required methods
async sub subscribe {
    my ($self, $channel, $topic, %opts) = @_;

    my $key = $self->_group_key($topic);
    await $self->{_redis}->sadd($key, $channel);
    await $self->{_redis}->expire($key, $self->{group_expiry});

    return Future->done(1);
}

async sub unsubscribe {
    my ($self, $channel, $topic) = @_;

    my $key = $self->_group_key($topic);
    await $self->{_redis}->srem($key, $channel);

    return Future->done(1);
}

async sub publish {
    my ($self, $topic, $message, %opts) = @_;

    my $exclude = $opts{exclude} // [];
    $exclude = [$exclude] unless ref $exclude eq 'ARRAY';
    my %excluded = map { $_ => 1 } @$exclude;

    my $key = $self->_group_key($topic);
    my @members = await $self->{_redis}->smembers($key);

    for my $channel (@members) {
        next if $excluded{$channel};

        # Silently drop if full
        my $qkey = $self->_queue_key($channel);
        my $len = await $self->{_redis}->llen($qkey);

        if ($len < $self->{capacity}) {
            my $json = encode_json($message);
            await $self->{_redis}->rpush($qkey, $json);
            await $self->{_redis}->expire($qkey, $self->{expiry});
        }
    }

    return Future->done(1);
}

async sub flush {
    my ($self) = @_;

    # Delete all keys with our prefix
    my @keys = await $self->{_redis}->keys($self->{prefix} . '*');

    if (@keys) {
        await $self->{_redis}->del(@keys);
    }

    return Future->done(1);
}

async sub cleanup {
    my ($self, $channel) = @_;

    # Remove queue
    await $self->{_redis}->del($self->_queue_key($channel));

    # Remove from all groups (scan for membership)
    my @group_keys = await $self->{_redis}->keys($self->{prefix} . 'g:*');
    for my $key (@group_keys) {
        await $self->{_redis}->srem($key, $channel);
    }

    return Future->done(1);
}

# Advanced features - basic stubs (implement in next tasks)
async sub psubscribe { Future->done(1) }
async sub punsubscribe { Future->done(1) }
async sub track { Future->done(1) }
async sub untrack { Future->done(1) }
async sub list_presence { Future->done([]) }
async sub send_delayed { Future->done(1) }
async sub publish_delayed { Future->done(1) }
async sub subscribe_with_history { Future->done(1) }

1;
```

**Step 4: Run test to verify it passes**

Run: `cd t && docker compose up -d && cd .. && REDIS_HOST=localhost prove -lv t/30-redis/01-core.t`
Expected: PASS

**Step 5: Commit**

```bash
git commit -am "feat(redis): implement Redis backend foundation with send/poll/pubsub"
```

---

## Task 12-15: Redis Backend Advanced Features

(Similar structure to Memory backend tasks 4-7, implementing patterns, presence, delays, history on Redis)

**Files:**
- Modify: `lib/PAGI/Channels/Backend/Redis.pm`
- Create: `t/30-redis/02-pubsub.t`
- Create: `t/30-redis/03-patterns.t`
- Create: `t/30-redis/04-presence.t`
- Create: `t/30-redis/05-delayed.t`
- Create: `t/30-redis/06-history.t`

Each task follows the same TDD pattern:
1. Write failing test
2. Run test to verify failure
3. Implement feature
4. Run test to verify pass
5. Commit

Key differences for Redis:
- Patterns use Redis KEYS scanning with Lua scripts
- Presence uses Redis HASHes with HSET/HGETALL
- Delays use Redis ZSET (sorted set) with score = delivery time
- History uses Redis LIST with LTRIM for capping

---

## Task 16: Example App 1 - Chat with Workers

**Files:**
- Create: `examples/chat-with-workers/app.pl`
- Create: `examples/chat-with-workers/worker.pl`
- Create: `examples/chat-with-workers/README.md`

**Step 1: Create chat application**

```perl
#!/usr/bin/env perl
# examples/chat-with-workers/app.pl
# Chat application with worker pool for background tasks

use strict;
use warnings;
use Future::AsyncAwait;
use IO::Async::Loop;
use Future::IO::Impl::IOAsync;

use PAGI::Channels;

# NOTE: This example uses IO::Async explicitly.
# The main library (PAGI::Channels) uses Future::IO only.

my $loop = IO::Async::Loop->new;
Future::IO->override_impl(Future::IO::Impl::IOAsync->new(loop => $loop));

my $channels = PAGI::Channels->new(
    backend => $ENV{PAGI_CHANNELS_BACKEND} // 'redis://localhost:6379',
);

# Chat room handler
my $chat_app = async sub {
    my ($scope, $receive, $send) = @_;
    return unless $scope->{type} eq 'websocket';

    my $ch = $scope->{'pagi.channels'};
    my $my_channel = $scope->{'pagi.channel'};
    my $room = $scope->{path_params}{room} // 'general';
    my $username = $scope->{query_params}{user} // 'anonymous';

    # Accept WebSocket
    await $send->({ type => 'websocket.accept' });

    # Subscribe with presence
    await $ch->subscribe("chat.$room",
        presence => { user => $username, channel => $my_channel }
    );

    # Main event loop
    eval {
        while (1) {
            my $event = await $receive->();

            if ($event->{type} eq 'websocket.receive') {
                my $data = decode_json($event->{text});

                if ($data->{action} eq 'message') {
                    # Broadcast to room
                    await $ch->publish("chat.$room", {
                        type => 'chat.message',
                        user => $username,
                        text => $data->{text},
                        timestamp => time(),
                    }, exclude => $my_channel);
                }
                elsif ($data->{action} eq 'process_image') {
                    # Send to worker pool
                    await $ch->send('worker.pool', {
                        type     => 'task.process_image',
                        image_id => $data->{image_id},
                        reply_to => $my_channel,
                    });
                }
            }
            elsif ($event->{type} eq 'chat.message') {
                # Forward to client
                await $send->({
                    type => 'websocket.send',
                    text => encode_json($event),
                });
            }
            elsif ($event->{type} eq 'presence.join') {
                await $send->({
                    type => 'websocket.send',
                    text => encode_json({
                        type => 'user_joined',
                        user => $event->{presence}{user},
                    }),
                });
            }
            elsif ($event->{type} eq 'presence.leave') {
                await $send->({
                    type => 'websocket.send',
                    text => encode_json({
                        type => 'user_left',
                        user => $event->{presence}{user},
                    }),
                });
            }
            elsif ($event->{type} eq 'task.result') {
                await $send->({
                    type => 'websocket.send',
                    text => encode_json($event),
                });
            }
            elsif ($event->{type} eq 'websocket.disconnect') {
                last;
            }
        }
    };

    # Cleanup happens automatically
};

my $app = $channels->wrap($chat_app);

print "Chat server ready. Connect with WebSocket to /chat/{room}?user={name}\n";
# In real usage: PAGI::Server->new(app => $app)->run;
```

**Step 2: Create worker process**

```perl
#!/usr/bin/env perl
# examples/chat-with-workers/worker.pl
# Background worker for processing tasks

use strict;
use warnings;
use Future::AsyncAwait;
use IO::Async::Loop;
use Future::IO::Impl::IOAsync;

use PAGI::Channels;

my $loop = IO::Async::Loop->new;
Future::IO->override_impl(Future::IO::Impl::IOAsync->new(loop => $loop));

my $channels = PAGI::Channels->new(
    backend => $ENV{PAGI_CHANNELS_BACKEND} // 'redis://localhost:6379',
);

my $worker_id = $$;

async sub run_worker {
    my $backend = $channels->backend;

    # Track worker presence
    $backend->set_channel_id("worker.$worker_id");
    await $backend->track('workers.pool', {
        worker_id => $worker_id,
        started   => time(),
        status    => 'idle',
    });

    print "Worker $worker_id started\n";

    while (1) {
        # Poll for tasks
        my $task = await $backend->poll('worker.pool');

        if ($task && $task->{type} eq 'task.process_image') {
            print "Processing image: $task->{image_id}\n";

            # Update presence to show busy
            await $backend->track('workers.pool', {
                worker_id => $worker_id,
                status    => 'processing',
                task      => $task->{image_id},
            });

            # Simulate work
            await Future::IO->sleep(2);

            # Send result back
            await $backend->send($task->{reply_to}, {
                type     => 'task.result',
                image_id => $task->{image_id},
                status   => 'processed',
                url      => "https://cdn.example.com/$task->{image_id}.jpg",
            });

            # Update presence back to idle
            await $backend->track('workers.pool', {
                worker_id => $worker_id,
                status    => 'idle',
            });
        }
        else {
            # No task, wait a bit
            await Future::IO->sleep(0.1);
        }
    }
}

$loop->add(IO::Async::Function->new(code => sub { }));  # Keep loop alive
run_worker()->get;
```

**Step 3: Create README**

```markdown
# Chat with Workers Example

Demonstrates PAGI-Channels with:
- WebSocket chat with multiple rooms
- Presence tracking (who's online)
- Worker pool for background tasks
- Request/reply pattern

## Running

1. Start Redis:
```bash
docker compose up -d
```

2. Start workers (run multiple):
```bash
PAGI_CHANNELS_BACKEND=redis://localhost:6379 perl worker.pl
```

3. Start chat server:
```bash
PAGI_CHANNELS_BACKEND=redis://localhost:6379 perl app.pl
```

4. Connect WebSocket clients to `/chat/general?user=alice`
```

**Step 4: Commit**

```bash
git add examples/
git commit -m "example: chat with workers demonstrating presence and request/reply"
```

---

## Task 17: Example App 2 - Task Queue with Progress

**Files:**
- Create: `examples/task-queue/server.pl`
- Create: `examples/task-queue/README.md`

**Step 1: Create task queue server**

```perl
#!/usr/bin/env perl
# examples/task-queue/server.pl
# Task queue with real-time progress updates

use strict;
use warnings;
use Future::AsyncAwait;
use IO::Async::Loop;
use Future::IO::Impl::IOAsync;
use JSON::MaybeXS qw(encode_json decode_json);

use PAGI::Channels;

my $loop = IO::Async::Loop->new;
Future::IO->override_impl(Future::IO::Impl::IOAsync->new(loop => $loop));

my $channels = PAGI::Channels->new(
    backend => $ENV{PAGI_CHANNELS_BACKEND} // 'redis://localhost:6379',
);

my $task_counter = 0;

# HTTP endpoint to submit tasks
my $http_handler = async sub {
    my ($scope, $receive, $send) = @_;
    return unless $scope->{type} eq 'http' && $scope->{method} eq 'POST';

    my $ch = $scope->{'pagi.channels'};

    # Read request body
    my $body_event = await $receive->();
    my $body = decode_json($body_event->{body});

    # Create task
    my $task_id = ++$task_counter;

    # Queue task for worker
    await $ch->send('task.queue', {
        type    => 'task.execute',
        task_id => $task_id,
        payload => $body,
    });

    # Respond with task ID
    await $send->({ type => 'http.response.start', status => 202 });
    await $send->({
        type => 'http.response.body',
        body => encode_json({ task_id => $task_id, status => 'queued' }),
    });
};

# WebSocket endpoint to watch task progress
my $ws_handler = async sub {
    my ($scope, $receive, $send) = @_;
    return unless $scope->{type} eq 'websocket';

    my $ch = $scope->{'pagi.channels'};
    my $task_id = $scope->{path_params}{task_id};

    await $send->({ type => 'websocket.accept' });

    # Subscribe to task updates with history (get any we missed)
    await $ch->subscribe("task.$task_id.**", history => 10);

    while (1) {
        my $event = await $receive->();

        if ($event->{type} =~ /^task\./) {
            await $send->({
                type => 'websocket.send',
                text => encode_json($event),
            });

            # Close on completion
            last if $event->{type} eq 'task.complete';
        }
        elsif ($event->{type} eq 'websocket.disconnect') {
            last;
        }
    }
};

# Worker (runs in same process for demo)
async sub worker_loop {
    my $backend = $channels->backend;

    while (1) {
        my $task = await $backend->poll('task.queue');

        if ($task && $task->{type} eq 'task.execute') {
            my $task_id = $task->{task_id};

            # Broadcast progress updates
            for my $percent (0, 25, 50, 75, 100) {
                await $backend->publish("task.$task_id.progress", {
                    type    => 'task.progress',
                    task_id => $task_id,
                    percent => $percent,
                });

                await Future::IO->sleep(0.5);  # Simulate work
            }

            # Broadcast completion
            await $backend->publish("task.$task_id.complete", {
                type    => 'task.complete',
                task_id => $task_id,
                result  => { success => 1 },
            });
        }
        else {
            await Future::IO->sleep(0.1);
        }
    }
}

# Start worker in background
worker_loop()->retain;

print "Task queue ready.\n";
print "POST /tasks to submit, WS /tasks/{id} to watch progress\n";
```

**Step 2: Create README**

```markdown
# Task Queue with Progress Example

Demonstrates PAGI-Channels with:
- HTTP endpoint to submit tasks
- WebSocket endpoint to watch progress
- Pattern subscriptions (`task.{id}.**`)
- Message history (catch up on missed updates)
- Real-time progress broadcasting

## API

### Submit Task
```
POST /tasks
{"data": "some payload"}

Response: {"task_id": 1, "status": "queued"}
```

### Watch Progress
```
WS /tasks/1

Receives:
{"type": "task.progress", "task_id": 1, "percent": 0}
{"type": "task.progress", "task_id": 1, "percent": 25}
...
{"type": "task.complete", "task_id": 1, "result": {...}}
```

## Running

```bash
PAGI_CHANNELS_BACKEND=redis://localhost:6379 perl server.pl
```
```

**Step 3: Commit**

```bash
git add examples/
git commit -m "example: task queue with pattern subscriptions and history"
```

---

## Task 18: Documentation

**Files:**
- Modify: `lib/PAGI/Channels.pm` (POD)
- Modify: `CLAUDE.md`
- Create: `README.md`

**Step 1: Add comprehensive POD to PAGI::Channels**

Add to `lib/PAGI/Channels.pm` after `__END__`:

```pod
=head1 NAME

PAGI::Channels - Cross-process messaging for PAGI applications

=head1 SYNOPSIS

    use PAGI::Channels;

    # Create channel layer (memory for dev, redis for production)
    my $channels = PAGI::Channels->new(
        backend => 'redis://localhost:6379',
    );

    # Wrap your PAGI app
    my $app = $channels->wrap(async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = $scope->{'pagi.channels'};
        my $my_channel = $scope->{'pagi.channel'};

        # Subscribe to a room with presence
        await $ch->subscribe("chat.room1",
            presence => { user => 'alice', status => 'online' }
        );

        # Pattern subscription
        await $ch->psubscribe("notifications.**");

        # Send with delay
        await $ch->send($target, { type => 'reminder' }, delay => 300);

        # Publish with history for new subscribers
        await $ch->publish("chat.room1", { type => 'msg', text => 'hello' });

        # List who's present
        my @users = await $ch->list_presence("chat.room1");
    });

=head1 DESCRIPTION

PAGI-Channels provides cross-process and cross-server messaging for PAGI
applications. It exceeds Django Channels with built-in:

=over 4

=item * B<Presence tracking> - Who's subscribed to a topic

=item * B<Pattern subscriptions> - C<chat.*> matches C<chat.room1>, C<chat.room2>

=item * B<Delayed messages> - Send after N seconds

=item * B<Message history> - New subscribers get last N messages

=back

=head2 LOOP AGNOSTICISM

B<CRITICAL:> This library uses Future::IO only. It works with any event loop:

    # IO::Async
    use IO::Async::Loop;
    use Future::IO::Impl::IOAsync;

    # Mojo::IOLoop
    use Future::IO::Impl::MojoIOLoop;

    # UV
    use Future::IO::Impl::UV;

The main library code (C<lib/>) never imports loop-specific modules directly.

=head1 METHODS

=head2 new

    my $channels = PAGI::Channels->new(
        backend => 'redis://localhost:6379',  # or 'memory://'
    );

=head2 wrap

    my $app = $channels->wrap($inner_app);

Wraps a PAGI application, injecting:

=over 4

=item * C<< $scope->{'pagi.channels'} >> - Channel interface

=item * C<< $scope->{'pagi.channel'} >> - This connection's unique channel name

=back

=head1 CHANNEL INTERFACE

Methods available on C<< $scope->{'pagi.channels'} >>:

=head2 subscribe

    await $ch->subscribe($topic);
    await $ch->subscribe($topic, presence => { user => 'alice' });
    await $ch->subscribe($topic, history => 10);

=head2 psubscribe

    await $ch->psubscribe("chat.*");       # Matches chat.room1
    await $ch->psubscribe("events.**");    # Matches events.user.123

=head2 publish

    await $ch->publish($topic, { type => 'msg', ... });
    await $ch->publish($topic, $msg, exclude => $my_channel);
    await $ch->publish($topic, $msg, delay => 60);

=head2 send

    await $ch->send($channel, { type => 'msg', ... });
    await $ch->send($channel, $msg, delay => 300);

=head2 list_presence

    my @users = await $ch->list_presence($topic);

=head2 track / untrack

    await $ch->track($topic, { worker_id => $$, status => 'idle' });
    await $ch->untrack($topic);

=head1 BACKENDS

=head2 Memory (memory://)

Single-process only. Good for development and testing.

=head2 Redis (redis://host:port)

Multi-process/multi-server. Uses L<Async::Redis>.

=head1 SEE ALSO

L<Async::Redis>, L<Future::IO>, L<Future::AsyncAwait>

=cut
```

**Step 2: Update CLAUDE.md**

```markdown
# CLAUDE.md

## Build & Test Commands

```bash
# Start Redis for tests
cd t && docker compose up -d && cd ..

# Run all tests
REDIS_HOST=localhost prove -l t/

# Run memory backend tests (no Redis needed)
prove -l t/10-memory/

# Run Redis backend tests
REDIS_HOST=localhost prove -l t/30-redis/
```

## Architecture

**Loop Agnosticism (CRITICAL):**
- `lib/` - Future::IO ONLY, no IO::Async/Mojo/etc.
- `t/` - Uses IO::Async via Future::IO backend
- `examples/` - Can use any loop

**Core Modules:**
- `PAGI::Channels` - Facade and wrap() middleware
- `PAGI::Channels::Backend` - Role defining backend interface
- `PAGI::Channels::Backend::Memory` - In-memory (single process)
- `PAGI::Channels::Backend::Redis` - Redis (multi-process)

**Advanced Features:**
- Presence tracking (subscribe with presence option)
- Pattern subscriptions (psubscribe with * and **)
- Delayed messages (delay option on send/publish)
- Message history (history option on subscribe)
```

**Step 3: Create README.md**

```markdown
# PAGI-Channels

Cross-process messaging for PAGI applications. Exceeds Django Channels.

## Features

- **Presence Tracking** - Who's online (Phoenix-style)
- **Pattern Subscriptions** - `chat.*`, `events.**`
- **Delayed Messages** - Send after N seconds
- **Message History** - New subscribers get last N messages
- **Loop Agnostic** - Works with IO::Async, Mojo, UV

## Installation

```bash
cpanm PAGI::Channels
```

## Quick Start

```perl
use PAGI::Channels;

my $channels = PAGI::Channels->new(
    backend => 'redis://localhost:6379',
);

my $app = $channels->wrap(async sub {
    my ($scope, $receive, $send) = @_;

    my $ch = $scope->{'pagi.channels'};

    await $ch->subscribe("chat.room1",
        presence => { user => 'alice' }
    );

    while (1) {
        my $event = await $receive->();
        # Handle chat messages, presence events, etc.
    }
});
```

## Documentation

See `perldoc PAGI::Channels` for full documentation.

## License

Same as Perl itself.
```

**Step 4: Commit**

```bash
git add -A
git commit -m "docs: comprehensive POD, README, and CLAUDE.md"
```

---

## Task 19: Full Test Suite Run

**Step 1: Run complete test suite**

```bash
cd t && docker compose up -d && cd ..
REDIS_HOST=localhost prove -lr t/
```

**Step 2: Verify all tests pass**

Expected: All tests pass

**Step 3: Commit any fixes needed**

---

## Task 20: Final Review and Tag

**Step 1: Code review checklist**

- [ ] All Future::IO in lib/, no direct loop imports
- [ ] All tests use IO::Async via Future::IO backend
- [ ] POD complete with examples
- [ ] No debug print statements
- [ ] No hardcoded hosts/ports (use env vars)

**Step 2: Final commit**

```bash
git commit -am "chore: final review and cleanup for v0.001"
```

**Step 3: Tag release**

```bash
git tag v0.001
```

---

## Verification Commands

```bash
# Start Redis
cd t && docker compose up -d && cd ..

# Run all tests
REDIS_HOST=localhost prove -lr t/

# Run specific suites
prove -l t/00-load.t
prove -l t/10-memory/
REDIS_HOST=localhost prove -l t/30-redis/
prove -l t/20-facade/

# Stop Redis
cd t && docker compose down
```

---

## Critical Files Summary

| File | Purpose |
|------|---------|
| `lib/PAGI/Channels.pm` | Main facade, wrap() middleware |
| `lib/PAGI/Channels/Backend.pm` | Role defining backend interface |
| `lib/PAGI/Channels/Backend/Memory.pm` | In-memory backend |
| `lib/PAGI/Channels/Backend/Redis.pm` | Redis backend |
| `t/lib/Test/PAGI/Channels.pm` | Test helper module |
| `cpanfile` | Dependencies |
| `t/docker-compose.yml` | Redis for tests |
