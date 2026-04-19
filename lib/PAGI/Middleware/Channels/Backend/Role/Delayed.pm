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
    $self->_validate_delay($delay_seconds);
    return await $self->schedule_delayed(
        'send', $channel, $message, Time::HiRes::time() + $delay_seconds
    );
}

async sub publish_delayed {
    my ($self, $topic, $message, $delay_seconds) = @_;
    $self->_validate_topic($topic);
    $self->_validate_message($message);
    $self->_validate_delay($delay_seconds);
    return await $self->schedule_delayed(
        'publish', $topic, $message, Time::HiRes::time() + $delay_seconds
    );
}

sub _validate_delay {
    my ($self, $delay_seconds) = @_;
    die "InvalidDelay: must be defined"
        unless defined $delay_seconds;
    die "InvalidDelay: must be numeric"
        unless $delay_seconds =~ /^-?\d+(?:\.\d+)?$/;
    die "InvalidDelay: negative ($delay_seconds)"
        if $delay_seconds < 0;
    die "InvalidDelay: $delay_seconds exceeds max_delay $self->{max_delay}"
        if $delay_seconds > $self->{max_delay};
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
