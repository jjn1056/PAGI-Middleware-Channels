package PAGI::Channels::Backend::Memory;
use strict;
use warnings;
use Future::AsyncAwait;
use Future;
use Role::Tiny::With;
use Time::HiRes ();
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
        await Future->fail('ChannelFull', 'channel', $channel);
    }

    push @$queue, {
        msg     => $message,
        expires => time() + $self->{expiry},
    };

    return 1;
}

# Core: poll (async for consistency with Redis backend)
async sub poll {
    my ($self, $channel) = @_;

    # Drain any due delayed messages into their target queues first
    await $self->process_delayed if @{$self->{delayed}};

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

# Helper to convert pattern to regex
sub _pattern_to_regex {
    my ($self, $pattern) = @_;

    # Escape special regex chars except our wildcards
    my $regex = quotemeta($pattern);

    # ** matches zero or more segments (including dots)
    # When ** follows a dot (e.g., "foo.**"), make the dot optional
    # so "foo.**" matches "foo", "foo.bar", "foo.bar.baz"
    $regex =~ s/\\\.\\\*\\\*/(\\..*)?\$/g;

    # Handle ** at the beginning or not preceded by a dot
    $regex =~ s/\\\*\\\*/.*/g;

    # * matches exactly one segment (no dots)
    $regex =~ s/\\\*/[^.]+/g;

    return qr/^$regex$/;
}

# PubSub: subscribe
async sub subscribe {
    my ($self, $channel, $topic, %opts) = @_;

    $self->_validate_channel($channel);
    $self->_validate_channel($topic);  # Same rules for topics

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

    return 1;
}

# PubSub: unsubscribe
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

    return 1;
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

# PubSub: publish
async sub publish {
    my ($self, $topic, $message, %opts) = @_;

    $self->_validate_channel($topic);
    $self->_validate_message($message);

    my $exclude = $opts{exclude} // [];
    $exclude = [$exclude] unless ref $exclude eq 'ARRAY';
    my %excluded = map { $_ => 1 } @$exclude;

    my $now = Time::HiRes::time();
    my %delivered;  # Track to avoid duplicates

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
        next if $delivered{$channel};  # Already delivered via exact match

        for my $p (@{$self->{patterns}{$channel}}) {
            if ($topic =~ $p->{regex}) {
                $self->_deliver_to_channel($channel, $message, $now);
                $delivered{$channel} = 1;
                last;  # Only deliver once per channel
            }
        }
    }

    return 1;
}
async sub flush {
    my ($self) = @_;
    $self->{queues} = {};
    $self->{groups} = {};
    $self->{patterns} = {};
    $self->{presence} = {};
    $self->{history} = {};
    $self->{delayed} = [];
    return 1;
}

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

    # Remove delayed messages for this channel
    $self->{delayed} = [
        grep { $_->{target} ne $channel } @{$self->{delayed}}
    ];

    return 1;
}

# Channel ID management (for presence tracking)
sub set_channel_id {
    my ($self, $channel_id) = @_;
    $self->{_channel_id} = $channel_id;
}

sub channel_id { shift->{_channel_id} }

# PubSub: psubscribe (pattern subscribe)
async sub psubscribe {
    my ($self, $channel, $pattern) = @_;

    $self->_validate_channel($channel);

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

    if ($self->{patterns}{$channel}) {
        $self->{patterns}{$channel} = [
            grep { $_->{pattern} ne $pattern } @{$self->{patterns}{$channel}}
        ];
    }

    return 1;
}
# Presence: track
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

    return 1;
}

# Presence: untrack
async sub untrack {
    my ($self, $topic) = @_;

    my $channel = $self->{_channel_id}
        or die "untrack() requires set_channel_id() first";

    if ($self->{presence}{$topic}) {
        delete $self->{presence}{$topic}{$channel};
    }

    return 1;
}

# Presence: list_presence
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
# Delayed: send_delayed
async sub send_delayed {
    my ($self, $channel, $message, $delay_seconds) = @_;

    $self->_validate_channel($channel);
    $self->_validate_message($message);

    my $deliver_at = Time::HiRes::time() + $delay_seconds;

    push @{$self->{delayed}}, {
        deliver_at => $deliver_at,
        type       => 'send',
        target     => $channel,
        message    => $message,
    };

    # Keep sorted by delivery time
    @{$self->{delayed}} = sort { $a->{deliver_at} <=> $b->{deliver_at} } @{$self->{delayed}};

    return 1;
}

# Delayed: publish_delayed
async sub publish_delayed {
    my ($self, $topic, $message, $delay_seconds) = @_;

    $self->_validate_channel($topic);
    $self->_validate_message($message);

    my $deliver_at = Time::HiRes::time() + $delay_seconds;

    push @{$self->{delayed}}, {
        deliver_at => $deliver_at,
        type       => 'publish',
        target     => $topic,
        message    => $message,
    };

    @{$self->{delayed}} = sort { $a->{deliver_at} <=> $b->{deliver_at} } @{$self->{delayed}};

    return 1;
}

# Delayed: process_delayed (called periodically or on poll)
async sub process_delayed {
    my ($self) = @_;

    my $now = Time::HiRes::time();

    while (@{$self->{delayed}} && $self->{delayed}[0]{deliver_at} <= $now) {
        my $entry = shift @{$self->{delayed}};

        if ($entry->{type} eq 'send') {
            await $self->send($entry->{target}, $entry->{message});
        }
        elsif ($entry->{type} eq 'publish') {
            await $self->publish($entry->{target}, $entry->{message});
        }
    }

    return 1;
}

# History: subscribe_with_history
async sub subscribe_with_history {
    my ($self, $channel, $topic, $history_count, %opts) = @_;

    $self->_validate_channel($channel);
    $self->_validate_channel($topic);

    my $now = Time::HiRes::time();

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

    return 1;
}

1;

__END__

=head1 NAME

PAGI::Channels::Backend::Memory - In-memory channel backend for single-process use

=head1 SYNOPSIS

    use PAGI::Channels::Backend::Memory;

    my $backend = PAGI::Channels::Backend::Memory->new(
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
