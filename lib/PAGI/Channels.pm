package PAGI::Channels;
use strict;
use warnings;
use Future::AsyncAwait;
use Future;
use Future::IO;

our $VERSION = '0.001';

sub new {
    my ($class, %args) = @_;

    my $backend = $args{backend}
        or die "PAGI::Channels: 'backend' argument required "
             . "(a PAGI::Channels::Backend instance)";

    return bless {
        _backend => $backend,
        _counter => 0,
    }, $class;
}

sub backend { shift->{_backend} }

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
        #    Poll channel queue periodically while waiting for protocol events
        my $wrapped_receive = async sub {
            # Check channel queue first (non-blocking)
            if (my $msg = await $self->{_backend}->poll($channel_name)) {
                return $msg;
            }

            # Start waiting for protocol event
            my $protocol_f = $receive->();

            # Poll channel queue while waiting, using select-style loop
            while (!$protocol_f->is_ready) {
                # Short sleep to avoid busy-waiting
                await Future::IO->sleep(0.1);

                # Check channel queue
                if (my $msg = await $self->{_backend}->poll($channel_name)) {
                    return $msg;
                }
            }

            # Protocol event arrived
            return $protocol_f->get;
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

package PAGI::Channels;

1;

__END__

=head1 NAME

PAGI::Channels - Cross-process messaging for PAGI applications

=head1 SYNOPSIS

    use PAGI::Channels;

    # Create channel layer (memory for dev, redis for production)
    my $channels = PAGI::Channels->new(
        backend => 'redis://localhost:6379',  # or 'memory://'
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

Create a new PAGI::Channels instance. The C<backend> option specifies the
backend URI. Defaults to C<memory://> if not specified.

You can also use the C<PAGI_CHANNELS_BACKEND> environment variable.

=head2 wrap

    my $app = $channels->wrap($inner_app);

Wraps a PAGI application, injecting:

=over 4

=item * C<< $scope->{'pagi.channels'} >> - Channel interface

=item * C<< $scope->{'pagi.channel'} >> - This connection's unique channel name

=back

The wrapped receive callable interleaves channel messages with protocol events.
Channel messages are delivered first when available.

Cleanup happens automatically when the inner app exits.

=head2 backend

    my $backend = $channels->backend;

Returns the backend instance.

=head1 CHANNEL INTERFACE

Methods available on C<< $scope->{'pagi.channels'} >>:

=head2 subscribe

    await $ch->subscribe($topic);
    await $ch->subscribe($topic, presence => { user => 'alice' });
    await $ch->subscribe($topic, history => 10);

Subscribe to a topic. Options:

=over 4

=item * C<presence> - Hash of presence data to track

=item * C<history> - Number of recent messages to receive

=back

=head2 unsubscribe

    await $ch->unsubscribe($topic);

Unsubscribe from a topic. Broadcasts C<presence.leave> event if presence was tracked.

=head2 psubscribe

    await $ch->psubscribe("chat.*");       # Matches chat.room1
    await $ch->psubscribe("events.**");    # Matches events.user.123

Subscribe to topics matching a pattern.

=over 4

=item * C<*> matches exactly one segment (no dots)

=item * C<**> matches zero or more segments

=back

=head2 punsubscribe

    await $ch->punsubscribe("chat.*");
    await $ch->punsubscribe();  # Remove all patterns

Unsubscribe from pattern subscriptions.

=head2 publish

    await $ch->publish($topic, { type => 'msg', ... });
    await $ch->publish($topic, $msg, exclude => $my_channel);
    await $ch->publish($topic, $msg, delay => 60);

Publish a message to all subscribers of a topic. Options:

=over 4

=item * C<exclude> - Channel or arrayref of channels to exclude

=item * C<delay> - Delay delivery by N seconds

=back

=head2 send

    await $ch->send($channel, { type => 'msg', ... });
    await $ch->send($channel, $msg, delay => 300);

Send a message directly to a specific channel. Options:

=over 4

=item * C<delay> - Delay delivery by N seconds

=back

=head2 list_presence

    my @users = await $ch->list_presence($topic);

Returns array of presence objects for a topic.

=head2 track

    await $ch->track($topic, { worker_id => $$, status => 'idle' });

Explicitly track presence without subscribing. Useful for workers.

=head2 untrack

    await $ch->untrack($topic);

Remove presence tracking.

=head1 DJANGO CHANNELS COMPATIBILITY

For familiarity, these aliases are provided:

    group_add     => subscribe
    group_discard => unsubscribe
    group_send    => publish

=head1 BACKENDS

=head2 Memory (memory://)

Single-process only. Good for development and testing.

=head2 Redis (redis://host:port)

Multi-process/multi-server. Uses L<Async::Redis>.

=head1 ENVIRONMENT VARIABLES

=over 4

=item * C<PAGI_CHANNELS_BACKEND> - Default backend URI

=back

=head1 SEE ALSO

L<Async::Redis>, L<Future::IO>, L<Future::AsyncAwait>

=head1 AUTHOR

John Googin Napiorkowski

=head1 LICENSE

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
