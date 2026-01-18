package PAGI::Channels;
use strict;
use warnings;
use Future::AsyncAwait;

our $VERSION = '0.001';

sub new {
    my ($class, %args) = @_;

    my $backend_uri = $args{backend}
        // $ENV{PAGI_CHANNELS_BACKEND}
        // 'memory://';

    my $self = bless {
        backend_uri => $backend_uri,
        _backend    => undef,
        _counter    => 0,
    }, $class;

    $self->_init_backend($backend_uri);

    return $self;
}

sub _init_backend {
    my ($self, $uri) = @_;

    if ($uri =~ /^memory:/) {
        require PAGI::Channels::Backend::Memory;
        $self->{_backend} = PAGI::Channels::Backend::Memory->new();
    }
    elsif ($uri =~ /^redis:/) {
        require PAGI::Channels::Backend::Redis;
        $self->{_backend} = PAGI::Channels::Backend::Redis->new(uri => $uri);
    }
    else {
        die "Unknown backend: $uri";
    }
}

sub backend { shift->{_backend} }

1;

__END__

=head1 NAME

PAGI::Channels - Cross-process messaging for PAGI applications

=head1 SYNOPSIS

    use PAGI::Channels;

    my $channels = PAGI::Channels->new(
        backend => 'memory://',  # or 'redis://localhost:6379'
    );

    my $app = $channels->wrap(async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = $scope->{'pagi.channels'};
        await $ch->subscribe("chat.room1");
        # ...
    });

=head1 DESCRIPTION

PAGI-Channels provides cross-process and cross-server messaging for PAGI
applications. It exceeds Django Channels with built-in:

=over 4

=item * Presence tracking (who's online)

=item * Pattern subscriptions (chat.* matches chat.room1, chat.room2)

=item * Delayed messages (send after N seconds)

=item * Message history (new subscribers get last N messages)

=back

B<IMPORTANT:> This library is event-loop agnostic. All I/O uses Future::IO
primitives. It works with IO::Async, Mojo::IOLoop, UV, or any Future::IO
backend.

=cut
