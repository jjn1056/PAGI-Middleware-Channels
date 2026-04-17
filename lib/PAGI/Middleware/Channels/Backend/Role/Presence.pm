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
