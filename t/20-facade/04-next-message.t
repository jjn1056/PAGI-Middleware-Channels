# t/20-facade/04-next-message.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;
use Future::AsyncAwait;
use Future;

my $loop = init_loop();

use PAGI::Middleware::Channels;
use PAGI::Middleware::Channels::Backend::Memory;
use PAGI::Channel;

subtest 'next_message delegates via facade' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();
    run { $backend->send('ch1', { type => 'test' }) };

    my $ch = PAGI::Channel->new(backend => $backend, channel_name => 'ch1');
    my $msg = run { $ch->next_message('ch1') };
    is($msg->{type}, 'test', 'facade delegates next_message');
};

subtest 'wrapped receive returns channel message with zero-latency' => sub {
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    my @received;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = PAGI::Channel->from($scope);
        my $my_channel = $ch->channel_name;

        # Send a channel message to self
        await $ch->backend->send($my_channel, { type => 'chat.msg', text => 'hello' });

        # Receive should get channel message first (from poll fast-path)
        my $event = await $receive->();
        push @received, $event;

        # Then protocol event (via wait_any, protocol wins)
        $event = await $receive->();
        push @received, $event;
    };

    my $wrapped = $channels->wrap($inner_app);

    my $protocol_event = { type => 'websocket.receive', text => 'from client' };
    my @protocol_events = ($protocol_event);
    my $scope   = { type => 'websocket' };
    my $receive = async sub { shift @protocol_events };
    my $send    = async sub { };

    run { $wrapped->($scope, $receive, $send) };

    is($received[0]->{type}, 'chat.msg', 'channel message first');
    is($received[1]->{type}, 'websocket.receive', 'protocol event second');
};

subtest 'wrapped receive returns protocol event when no channel message' => sub {
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    my $got_event;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;
        $got_event = await $receive->();
    };

    my $wrapped = $channels->wrap($inner_app);

    my $scope   = { type => 'websocket' };
    my $receive = async sub { { type => 'websocket.connect' } };
    my $send    = async sub { };

    run { $wrapped->($scope, $receive, $send) };

    is($got_event->{type}, 'websocket.connect', 'protocol event returned');
};

subtest 'channel message delivered mid-wait wins the race' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();
    my $channels = PAGI::Middleware::Channels->new(backend => $backend);

    my $got_event;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = PAGI::Channel->from($scope);
        my $my_channel = $ch->channel_name;

        # Subscribe so publish can reach us
        await $ch->subscribe('test.room');

        # Start a receive — no channel message yet, protocol is slow
        my $receive_f = $receive->();

        # Publish a message — should wake next_message and win the race
        await $backend->publish('test.room', { type => 'injected' });

        $got_event = await $receive_f;
    };

    my $wrapped = $channels->wrap($inner_app);

    # Protocol receive that never resolves (simulates slow WebSocket)
    my $scope   = { type => 'websocket' };
    my $receive = sub { Future->new };  # Pending forever
    my $send    = async sub { };

    run { $wrapped->($scope, $receive, $send) };

    is($got_event->{type}, 'injected', 'channel message won the race');
};

done_testing;
