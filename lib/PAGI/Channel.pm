package PAGI::Channel;
use strict;
use warnings;
use Future::AsyncAwait;
use Carp ();

our $VERSION = '0.001';

# Class method: extract this connection's channel handle from PAGI scope.
# Accepts a scope hashref or any object that provides a ->scope method.
# Croaks with a useful message if the middleware was not wired up.
sub from {
    my ($class, $arg) = @_;
    my $scope = ref($arg) eq 'HASH' ? $arg : $arg->scope;
    my $ch = $scope->{'pagi.channels'}
        or Carp::croak(
            "No channel layer in scope — did you wrap your app with PAGI::Middleware::Channels?"
        );
    return $ch;
}

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
    my ($self, $topic, %opts) = @_;
    return await $self->{backend}->list_presence($topic, %opts);
}

async sub count_presence {
    my ($self, $topic) = @_;
    return await $self->{backend}->count_presence($topic);
}

async sub scan_presence {
    my ($self, $topic, %opts) = @_;
    return await $self->{backend}->scan_presence($topic, %opts);
}

async sub next_message {
    my ($self, $channel) = @_;
    return await $self->{backend}->next_message($channel);
}

# Django Channels compatibility aliases
*group_add     = \&subscribe;
*group_discard = \&unsubscribe;
*group_send    = \&publish;

1;

__END__

=encoding utf-8

=head1 NAME

PAGI::Channel - Per-connection handle for the PAGI channel layer middleware

=head1 SYNOPSIS

    # Inside a PAGI app wrapped with PAGI::Middleware::Channels:
    my $app = async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = PAGI::Channel->from($scope);
        my $my_channel = $ch->channel_name;

        await $ch->subscribe("chat.room1",
            presence => { user => 'alice', status => 'online' }
        );

        while (1) {
            my $event = await $receive->();
            # Handle chat messages, presence events, etc.
        }
    };

    # For unit tests / scripts you can construct one directly:
    use PAGI::Channel;
    use PAGI::Middleware::Channels::Backend::Memory;

    my $ch = PAGI::Channel->new(
        backend      => PAGI::Middleware::Channels::Backend::Memory->new,
        channel_name => 'test.conn',
    );

=head1 DESCRIPTION

A handler-facing handle bound to a single connection's channel name and the
configured backend. Created per-request by L<PAGI::Middleware::Channels>'s
C<wrap()> and exposed via C<< PAGI::Channel->from($scope) >>.

=head1 CONSTRUCTORS

=head2 from

    my $ch = PAGI::Channel->from($scope);
    my $ch = PAGI::Channel->from($websocket);   # any object with ->scope

Extract this connection's channel handle from a PAGI scope hashref or any
object that provides a C<< ->scope >> method (e.g. L<PAGI::WebSocket>,
L<PAGI::Request>). Croaks with a descriptive message if the app was not
wrapped with L<PAGI::Middleware::Channels>.

    # Also gives you the connection's unique channel name:
    my $my_channel = $ch->channel_name;

=head2 new

    PAGI::Channel->new(
        backend      => $backend,
        channel_name => $channel_name,
    );

Direct constructor, primarily for tests and scripts. In production code,
use L</from> instead.

=head1 ACCESSORS

=head2 channel_name

    my $name = $ch->channel_name;   # e.g. "conn.12345.1735689600.1"

The unique name assigned to this connection by the middleware.

=head2 backend

    my $backend = $ch->backend;

The underlying L<PAGI::Middleware::Channels::Backend> instance.

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

=head2 track

    await $ch->track($topic, { worker_id => $$, status => 'idle' });

Explicitly track presence without subscribing — useful for workers.

=head2 untrack

    await $ch->untrack($topic);

=head2 list_presence

    my @users = await $ch->list_presence($topic);
    my @users = await $ch->list_presence($topic, limit => 100);

Returns presence data for all current subscribers to C<$topic>. Each element
is the hashref that was passed to C<subscribe> or C<track>.

B<Intended for small groups> (chat rooms, lobbies, game sessions). For large
topics, use C<count_presence> to get a count or C<scan_presence> to paginate.

If C<limit> is provided and the number of members exceeds it, the method
croaks with a message directing you to C<count_presence> or C<scan_presence>.

=head2 count_presence

    my $n = await $ch->count_presence($topic);

Returns the number of members currently tracked in C<$topic>. O(1) in both
backends — safe to call on large topics.

=head2 scan_presence

    my ($cursor, @batch) = await $ch->scan_presence($topic, cursor => 0, count => 100);
    while ($cursor) {
        ($cursor, @batch) = await $ch->scan_presence($topic, cursor => $cursor, count => 100);
        # process @batch ...
    }

Cursor-based iteration over presence data. Start with C<cursor =E<gt> 0>;
keep calling until the returned cursor is C<0> (meaning iteration is complete).

C<count> is a hint — actual batch size may be smaller or larger (Redis SCAN
behaviour). For the Memory backend, C<count> is exact.

B<Note:> If members are added or removed between calls, pages may overlap
or skip entries. This matches Redis SCAN's documented semantics.

=head2 next_message

    my $msg = await $ch->next_message($channel_name);

Waits until a message is available on the given channel, then returns it.
Unlike C<poll>, which returns C<undef> immediately when empty,
C<next_message> blocks asynchronously until a message arrives. Used
internally by the middleware for event-driven receive interleaving.

The returned Future may be cancelled to abort the wait.

=head1 DJANGO CHANNELS COMPATIBILITY

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
