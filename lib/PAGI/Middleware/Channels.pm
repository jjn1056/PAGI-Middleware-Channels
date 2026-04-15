package PAGI::Middleware::Channels;
use strict;
use warnings;
use Future::AsyncAwait;
use Future;
use Future::IO;

our $VERSION = '0.001';

sub new {
    my ($class, %args) = @_;

    my $backend = $args{backend}
        or die "PAGI::Middleware::Channels: 'backend' argument required "
             . "(a PAGI::Middleware::Channels::Backend instance)";

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

        my $channel_name = $self->_generate_channel_name();

        $scope->{'pagi.channels'} = $self->_create_channel_interface($channel_name);
        $scope->{'pagi.channel'}  = $channel_name;

        my $wrapped_receive = async sub {
            if (my $msg = await $self->{_backend}->poll($channel_name)) {
                return $msg;
            }

            my $protocol_f = $receive->();

            while (!$protocol_f->is_ready) {
                await Future::IO->sleep(0.1);

                if (my $msg = await $self->{_backend}->poll($channel_name)) {
                    return $msg;
                }
            }

            return $protocol_f->get;
        };

        my $err;
        eval { await $inner_app->($scope, $wrapped_receive, $send) };
        $err = $@;

        await $self->{_backend}->cleanup($channel_name);

        die $err if $err;
    };
}

sub _generate_channel_name {
    my ($self) = @_;
    $self->{_counter}++;
    return sprintf("conn.%d.%d.%d", $$, time(), $self->{_counter});
}

sub _create_channel_interface {
    my ($self, $channel_name) = @_;

    require PAGI::Channels;
    $self->{_backend}->set_channel_id($channel_name);

    return PAGI::Channels->new(
        backend      => $self->{_backend},
        channel_name => $channel_name,
    );
}

1;

__END__

=encoding utf-8

=head1 NAME

PAGI::Middleware::Channels - Cross-process messaging middleware for PAGI applications

=head1 SYNOPSIS

    use PAGI::Middleware::Channels;
    use PAGI::Middleware::Channels::Backend::Memory;

    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    # Prod: Redis-backed, with a caller-owned Async::Redis client
    use Async::Redis;
    use PAGI::Middleware::Channels::Backend::Redis;

    my $redis = Async::Redis->new(
        uri       => 'redis://localhost:6379',
        prefix    => 'myapp:channels:',
        reconnect => 1,
    );
    $redis->connect->get;

    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis),
    );

    # Wrap your PAGI app
    my $app = $channels->wrap(async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = $scope->{'pagi.channels'};   # PAGI::Channels instance
        my $my_channel = $scope->{'pagi.channel'};

        await $ch->subscribe("chat.room1",
            presence => { user => 'alice', status => 'online' }
        );

        while (1) {
            my $event = await $receive->();
            # Handle chat messages, presence events, etc.
        }
    });

=head1 DESCRIPTION

Provides cross-process and cross-server messaging for PAGI applications.

The middleware wraps your PAGI app and injects two scope keys:

=over 4

=item * C<< $scope->{'pagi.channels'} >> — a L<PAGI::Channels> helper for this connection.

=item * C<< $scope->{'pagi.channel'} >> — this connection's unique channel name.

=back

It also wraps the C<$receive> callable so channel-delivered messages
interleave with protocol events, and runs cleanup on the backend when
the inner app exits.

=head2 LOOP AGNOSTICISM

This module uses L<Future::IO> only. It works with any event loop —
L<IO::Async>, L<Mojo::IOLoop>, or any other implementation of the
Future::IO interface.

=head1 METHODS

=head2 new

    my $channels = PAGI::Middleware::Channels->new(
        backend => $backend_instance,
    );

The C<backend> argument is B<required> and must be a
L<PAGI::Middleware::Channels::Backend> instance (e.g.,
L<PAGI::Middleware::Channels::Backend::Memory> or
L<PAGI::Middleware::Channels::Backend::Redis>).

This module does no backend construction of its own — callers wire
up the backend (and, for Redis, the underlying L<Async::Redis>
client) explicitly. The distribution has no runtime dependency on
any Redis client.

=head2 wrap

    my $app = $channels->wrap($inner_app);

Wraps a PAGI application, returning a new app that:

=over 4

=item * Generates a unique channel name per request and injects
C<< $scope->{'pagi.channel'} >>.

=item * Constructs a L<PAGI::Channels> helper bound to that channel and
the configured backend, and injects it as C<< $scope->{'pagi.channels'} >>.

=item * Wraps C<$receive> so channel messages are interleaved with
protocol events.

=item * Calls C<< $backend->cleanup($channel) >> when the inner app exits.

=back

=head2 backend

    my $backend = $channels->backend;

Returns the backend instance passed to the constructor.

=head1 BACKENDS

=head2 L<PAGI::Middleware::Channels::Backend::Memory>

Single-process, in-memory. Good for development and testing.

=head2 L<PAGI::Middleware::Channels::Backend::Redis>

Multi-process, multi-server. Takes a caller-owned L<Async::Redis>
instance; this distribution itself has no runtime dependency on any
Redis client — any object that ducks the Async::Redis interface works.

=head1 SEE ALSO

L<PAGI::Channels>, L<Async::Redis>, L<Future::IO>, L<Future::AsyncAwait>

=head1 AUTHOR

John Napiorkowski

=head1 LICENSE

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
