package PAGI::Middleware::Channels::Backend::Memory;
use strict;
use warnings;
use parent 'PAGI::Middleware::Channels::Backend';
use Role::Tiny::With;
use Future::AsyncAwait;
use Future;
use Time::HiRes ();
use Carp ();
use namespace::clean;

with 'PAGI::Middleware::Channels::Backend::Role::Presence',
     'PAGI::Middleware::Channels::Backend::Role::History',
     'PAGI::Middleware::Channels::Backend::Role::Delayed',
     'PAGI::Middleware::Channels::Backend::Role::PatternSubs';

sub new {
    my ($class, %args) = @_;
    my $self = $class->SUPER::new(%args);

    # Memory-specific state initialization
    $self->{queues}   = {};
    $self->{groups}   = {};
    $self->{patterns} = {};
    $self->{presence} = {};
    $self->{history}  = {};
    $self->{history_seq} = {};
    $self->{delayed}  = [];
    $self->{_waiters} = {};

    return $self;
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
        await Future->fail('ChannelFull', 'channel', $channel);
    }

    push @$queue, {
        msg     => $message,
        expires => time() + $self->{expiry},
    };

    $self->_notify_waiters($channel);

    return 1;
}

# Core: poll (async for consistency with Redis backend)
async sub poll {
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

# Resolve any futures waiting for messages on this channel.
# Called by send() and _deliver_to_channel() after enqueueing.
# Feed waiters one at a time until the queue is drained; any unsatisfied
# waiters are re-registered so they keep waiting for the next send.
# (Without this, a single enqueue would resolve every waiter — the extras
# with undef — because poll() on an empty queue returns undef and we
# would have called ->done(undef) on them.)
sub _notify_waiters {
    my ($self, $channel) = @_;
    my $waiters = delete $self->{_waiters}{$channel} or return;

    # Memory's poll is synchronous (no await in its body), so the Future
    # returned is already ready and ->get is non-blocking.
    for my $i (0 .. $#$waiters) {
        my $result_f = $waiters->[$i];
        next if $result_f->is_ready;

        my $msg = $self->poll($channel)->get;
        if (defined $msg) {
            $result_f->done($msg);
        } else {
            # Queue drained. Re-register this waiter and the rest; their
            # on_cancel handlers (installed in next_message) stay intact.
            push @{$self->{_waiters}{$channel}},
                grep { !$_->is_ready && !$_->is_cancelled }
                    @{$waiters}[$i .. $#$waiters];
            return;
        }
    }
}

# PubSub: subscribe
async sub subscribe {
    my ($self, $channel, $topic, %opts) = @_;
    $self->_validate_channel($channel);
    $self->_validate_topic($topic);

    my $now = time();
    $self->{groups}{$topic} //= {};
    $self->{groups}{$topic}{$channel} = $now + $self->{group_expiry};
    return 1;
}

# PubSub: unsubscribe
async sub unsubscribe {
    my ($self, $channel, $topic) = @_;
    if ($self->{groups}{$topic}) {
        delete $self->{groups}{$topic}{$channel};
    }
    return 1;
}

# Helper for delivery with current timestamp
sub _deliver_to_channel {
    my ($self, $channel, $message, $now) = @_;

    $self->{queues}{$channel} //= [];
    my $queue = $self->{queues}{$channel};

    if (@$queue < $self->{capacity}) {
        push @$queue, {
            msg     => $message,
            expires => $now + $self->{expiry},
        };
        $self->_notify_waiters($channel);
    }
}

# Required by Role::History's subscribe_with_history and Role::PatternSubs.
# Delivers a message to a channel; returns a Future (roles await this).
sub _deliver {
    my ($self, $channel, $message) = @_;
    $self->_deliver_to_channel($channel, $message, Time::HiRes::time());
    return Future->done(1);
}

# PubSub: publish
async sub publish {
    my ($self, $topic, $message, %opts) = @_;

    $self->_validate_topic($topic);
    $self->_validate_message($message);

    my %excluded = %{ $self->_normalize_exclude($opts{exclude}) };

    my $now = Time::HiRes::time();

    # Direct group subscribers
    my $members = $self->{groups}{$topic} // {};
    for my $channel (keys %$members) {
        next if $members->{$channel} < $now;
        next if $excluded{$channel};
        $self->_deliver_to_channel($channel, $message, $now);
    }

    return 1;
}

async sub flush {
    my ($self) = @_;
    $self->{queues}   = {};
    $self->{groups}   = {};
    $self->{patterns} = {};
    $self->{presence} = {};
    $self->{history}  = {};
    $self->{history_seq} = {};
    $self->{delayed}  = [];

    # Cancel all pending next_message waiters
    for my $waiters (values %{$self->{_waiters}}) {
        $_->cancel for grep { !$_->is_ready } @$waiters;
    }
    $self->{_waiters} = {};

    return 1;
}

async sub cleanup {
    my ($self, $channel) = @_;

    # Remove from all groups (no presence broadcast — Role::Presence's
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

# PubSub: psubscribe (pattern subscribe)
async sub psubscribe {
    my ($self, $channel, $pattern) = @_;

    $self->_validate_channel($channel);
    $self->_validate_pattern($pattern);

    my $regex = $self->_pattern_to_regex($pattern);

    $self->{patterns}{$channel} //= [];

    # Avoid duplicate patterns
    for my $p (@{$self->{patterns}{$channel}}) {
        return 1 if $p->{pattern} eq $pattern;
    }

    push @{$self->{patterns}{$channel}}, {
        pattern => $pattern,
        regex   => $regex,
    };

    return 1;
}

# PubSub: punsubscribe (pattern unsubscribe)
async sub punsubscribe {
    my ($self, $channel, $pattern) = @_;

    if (!defined $pattern) {
        delete $self->{patterns}{$channel};
        return 1;
    }

    if ($self->{patterns}{$channel}) {
        $self->{patterns}{$channel} = [
            grep { $_->{pattern} ne $pattern } @{$self->{patterns}{$channel}}
        ];
    }

    return 1;
}

# Presence: track
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

# Presence: untrack
async sub untrack {
    my ($self, $topic, $channel) = @_;
    if ($self->{presence}{$topic}) {
        delete $self->{presence}{$topic}{$channel};
    }
    return 1;
}

# Presence: _presence_topics_for_channel (required by Role::Presence)
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

# Presence: list_presence
async sub list_presence {
    my ($self, $topic, %opts) = @_;
    my $limit = $opts{limit};

    my $entries = $self->{presence}{$topic} // {};
    my $now = time();

    my @result;
    for my $channel (keys %$entries) {
        my $entry = $entries->{$channel};
        next if $entry->{expires} < $now;
        push @result, $entry->{data};
    }

    if (defined $limit && @result > $limit) {
        Carp::croak(
            "list_presence: topic '$topic' has " . scalar(@result) . " members,"
            . " which exceeds limit $limit."
            . " Use count_presence() for counts or scan_presence() for paginated access."
        );
    }

    return @result;
}

# Presence: count_presence
async sub count_presence {
    my ($self, $topic) = @_;

    my $entries = $self->{presence}{$topic} // {};
    my $now = time();

    return scalar grep { $entries->{$_}{expires} >= $now } keys %$entries;
}

# Presence: scan_presence (cursor-based pagination)
# cursor => 0 to start; returns 0 when iteration complete.
# NOTE: cursor is a numeric offset into a sorted list. If entries are
# added/removed between calls, pages may overlap or skip — same guarantee
# as Redis SCAN.
async sub scan_presence {
    my ($self, $topic, %opts) = @_;
    my $cursor = $opts{cursor} // 0;
    my $count  = $opts{count}  // 100;

    my $entries = $self->{presence}{$topic} // {};
    my $now = time();

    my @all = sort grep { $entries->{$_}{expires} >= $now } keys %$entries;
    my $total = scalar @all;

    return (0) if $total == 0;

    my $end = $cursor + $count;
    $end = $total if $end > $total;

    my @page = @all[$cursor .. $end - 1];
    my @result = map { $entries->{$_}{data} } @page;

    my $next_cursor = ($cursor + $count < $total) ? $cursor + $count : 0;

    return ($next_cursor, @result);
}

# Core: next_message — wait for a message (condition variable pattern).
# Returns a Future that resolves with the next message on $channel.
# The Future may be cancelled to abort the wait.
sub next_message {
    my ($self, $channel) = @_;

    my $result_f = Future->new;

    # Fast path: check queue via poll; resolve immediately if message present.
    # Slow path: register result_f as a waiter; _notify_waiters will poll and
    # resolve it when a message arrives.
    $self->poll($channel)->on_done(sub {
        my ($msg) = @_;
        if (defined $msg) {
            $result_f->done($msg) unless $result_f->is_ready;
            return;
        }

        # Queue was empty — register as waiter
        push @{$self->{_waiters}{$channel}}, $result_f;

        # On cancel: remove from waiters list
        $result_f->on_cancel(sub {
            my $list = $self->{_waiters}{$channel} or return;
            @$list = grep { !$_->is_cancelled } @$list;
            delete $self->{_waiters}{$channel} unless @$list;
        });
    });

    return $result_f;
}

# History: _record_history (required by Role::History)
# Returns the new opaque cursor for the recorded message, or undef if not
# recorded (history disabled, or an ephemeral presence event).
async sub _record_history {
    my ($self, $topic, $message) = @_;
    return undef unless $self->{history_size} > 0;
    return undef if ($message->{type} // '') =~ /^presence\./;

    my $seq = ++$self->{history_seq}{$topic};   # monotonic integer per topic
    $self->{history}{$topic} //= [];
    push @{$self->{history}{$topic}}, {
        seq       => $seq,
        message   => $message,
        timestamp => Time::HiRes::time(),
    };
    while (@{$self->{history}{$topic}} > $self->{history_size}) {
        shift @{$self->{history}{$topic}};
    }
    return $seq;
}

# History: read_history (required by Role::History)
async sub read_history {
    my ($self, $topic, $count) = @_;
    my @history = @{$self->{history}{$topic} // []};
    @history = @history[-$count..-1] if @history > $count;
    return map { $_->{message} } @history;
}

# Delayed: schedule_delayed (required by Role::Delayed)
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

# Delayed: _has_due_delayed (required by Role::Delayed)
async sub _has_due_delayed {
    my ($self) = @_;
    return @{$self->{delayed}} && $self->{delayed}[0]{deliver_at} <= Time::HiRes::time();
}

# Delayed: _remove_delayed_for_channel (required by Role::Delayed)
async sub _remove_delayed_for_channel {
    my ($self, $channel) = @_;
    $self->{delayed} = [
        grep { !($_->{type} eq 'send' && $_->{target} eq $channel) } @{$self->{delayed}}
    ];
    return 1;
}

# Delayed: process_delayed (required by Role::Delayed)
async sub process_delayed {
    my ($self) = @_;

    my $now = Time::HiRes::time();
    my $processed = 0;

    while (@{$self->{delayed}} && $self->{delayed}[0]{deliver_at} <= $now) {
        my $entry = shift @{$self->{delayed}};

        if ($entry->{type} eq 'send') {
            await $self->send($entry->{target}, $entry->{message});
        }
        elsif ($entry->{type} eq 'publish') {
            await $self->publish($entry->{target}, $entry->{message});
        }

        $processed++;
    }

    return $processed;
}

# PatternSubs: _list_pattern_subscribers (required by Role::PatternSubs)
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

# PatternSubs: _group_members (required by Role::PatternSubs's around publish)
async sub _group_members {
    my ($self, $topic) = @_;
    my $members = $self->{groups}{$topic} // {};
    my $now = time();
    return grep { $members->{$_} >= $now } keys %$members;
}

1;

__END__

=head1 NAME

PAGI::Middleware::Channels::Backend::Memory - In-memory channel backend for single-process use

=head1 SYNOPSIS

    use PAGI::Middleware::Channels::Backend::Memory;

    my $backend = PAGI::Middleware::Channels::Backend::Memory->new(
        capacity => 100,    # Max messages per channel
        expiry   => 60,     # Message TTL in seconds
    );

    # Send a message
    await $backend->send('my.channel', { type => 'greeting', msg => 'hello' });

    # Poll for messages (non-blocking)
    my $msg = $backend->poll('my.channel');

=head1 DESCRIPTION

This backend stores all messages in memory and is suitable for single-process
applications or testing. Messages are stored in FIFO queues with configurable
capacity limits and expiration.

This backend implements the core L<PAGI::Middleware::Channels::Backend>
contract and declares the Presence, History, Delayed, and PatternSubs
capability roles.

B<Note:> This backend does not provide cross-process communication. For
multi-process or multi-server scenarios, use the Redis backend.

=head1 CONSTRUCTOR OPTIONS

=over 4

=item capacity => $int

Maximum number of messages per channel queue. Default: 100.

=item expiry => $seconds

Time-to-live for messages in seconds. Default: 60.

=item group_expiry => $seconds

Time-to-live for subscription group membership. Default: 86400 (1 day).

=item max_size => $bytes

Maximum size of serialized message. Default: 1048576 (1MB).

=item history_size => $int

Number of messages to retain for history feature. Default: 0 (disabled).

=back

=head1 METHODS

=head2 send($channel, $message) -> Future

Send a message to a channel. Returns a Future that resolves to 1 on success,
or fails with 'ChannelFull' if the channel queue is at capacity.

=head2 poll($channel) -> Future($message | undef)

Async poll for a message from a channel. Returns a Future that resolves to
the oldest message or undef if the channel is empty.

=cut
