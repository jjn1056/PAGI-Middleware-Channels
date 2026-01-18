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
        await Future->fail('ChannelFull', 'channel', $channel);
    }

    push @$queue, {
        msg     => $message,
        expires => time() + $self->{expiry},
    };

    return 1;
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
async sub subscribe { return 1 }
async sub unsubscribe { return 1 }
async sub publish { return 1 }
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
async sub cleanup { return 1 }
async sub psubscribe { return 1 }
async sub punsubscribe { return 1 }
async sub track { return 1 }
async sub untrack { return 1 }
async sub list_presence { return [] }
async sub send_delayed { return 1 }
async sub publish_delayed { return 1 }
async sub subscribe_with_history { return 1 }

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

=head2 poll($channel) -> $message | undef

Non-blocking poll for a message from a channel. Returns the oldest message
or undef if the channel is empty.

=cut
