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
        my $wrapped_receive = async sub {
            # Check channel queue first (non-blocking)
            if (my $msg = $self->{_backend}->poll($channel_name)) {
                return $msg;
            }
            # Fall through to protocol receive
            return await $receive->();
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
