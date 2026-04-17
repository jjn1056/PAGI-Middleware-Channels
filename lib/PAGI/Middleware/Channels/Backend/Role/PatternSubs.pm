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
