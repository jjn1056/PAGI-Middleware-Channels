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

# _record_history($topic, $message) -> Future($cursor | undef)
#   Called by the around-publish hook below. MUST skip messages whose type
#   matches /^presence\./ (ephemeral, not history) and MUST trim to
#   $self->{history_size} entries. Returns the new opaque cursor when it
#   records, or undef when it does not (history disabled / presence event).
#
# read_history($topic, $count, %opts) -> Future(@messages)
#   Without 'since': the most recent $count messages, oldest first.
#   With 'since' => $cursor: every retained message strictly after $cursor,
#   oldest first. Each returned message carries its cursor as a _seq field.

# Wrap publish: record history (assigning a cursor) and, when recorded,
# deliver a _seq-tagged copy so subscribers can track their resume point.
around publish => async sub {
    my ($orig, $self, $topic, $message, %opts) = @_;
    my $seq = await $self->_record_history($topic, $message);
    my $delivered = defined $seq ? { %$message, _seq => $seq } : $message;
    return await $self->$orig($topic, $delivered, %opts);
};

# Provided default: subscribe + replay history (optionally from a cursor).
# Backends may override for performance.
async sub subscribe_with_history {
    my ($self, $channel, $topic, $count, %opts) = @_;
    my $since = delete $opts{since};
    my @history = await $self->read_history(
        $topic, $count, (defined $since ? (since => $since) : ()),
    );
    for my $msg (@history) {
        await $self->_deliver($channel, $msg);   # read_history returns _seq-tagged copies (see contract above)
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
C<subscribe(..., history =E<gt> N)> option.

=head2 Cursor-resumable history

Recorded messages are delivered (live and on replay) carrying a reserved
C<_seq> field: an B<opaque cursor> that the issuing backend can compare to mean
"strictly after". The representation is backend-private -- the Memory backend
uses a monotonic integer, the Redis backend uses a stream id -- so a consumer
must treat C<_seq> as opaque and never parse it.

C<read_history($topic, $count, %opts)> and
C<< PAGI::Channel->subscribe($topic, since =E<gt> $cursor) >> replay every
retained message B<strictly after> C<$cursor>, oldest first. Resume is exact
within C<history_size>; a cursor older than the oldest retained entry cannot
replay the trimmed messages. Pass the last C<_seq> you saw back as C<since> to
resume after a reconnect (to any fork or node sharing the backend).

=cut
