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
#   Called by the around-publish hook below. MUST skip messages whose type
#   matches /^presence\./ - those are ephemeral and not history.
#   MUST trim to $self->{history_size} entries.
#
# read_history($topic, $count) -> Future(@messages)
#   Most recent $count messages, oldest first. Empty list if no history.

# Wrap publish so it records history before delivering.
around publish => async sub {
    my ($orig, $self, $topic, $message, %opts) = @_;
    await $self->_record_history($topic, $message);
    return await $self->$orig($topic, $message, %opts);
};

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
