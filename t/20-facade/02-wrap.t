# t/20-facade/02-wrap.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Channels;
use Future::AsyncAwait;

subtest 'wrap injects scope keys' => sub {
    my $channels = PAGI::Channels->new();

    my $captured_scope;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;
        $captured_scope = $scope;
    };

    my $wrapped = $channels->wrap($inner_app);

    # Simulate calling the wrapped app
    my $scope = { type => 'websocket' };
    my $receive = async sub { { type => 'websocket.disconnect' } };
    my $send = async sub { };

    run { $wrapped->($scope, $receive, $send) };

    ok(exists $captured_scope->{'pagi.channels'}, 'pagi.channels injected');
    ok(exists $captured_scope->{'pagi.channel'}, 'pagi.channel injected');
    like($captured_scope->{'pagi.channel'}, qr/^conn\./, 'channel name has conn. prefix');
};

subtest 'wrapped receive interleaves channel messages' => sub {
    my $channels = PAGI::Channels->new();

    my @received;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = $scope->{'pagi.channels'};
        my $my_channel = $scope->{'pagi.channel'};

        # Subscribe to a room
        await $ch->subscribe('room');

        # Have someone send us a message
        await $ch->backend->send($my_channel, { type => 'chat.msg', text => 'hello' });

        # Receive should get channel message first
        my $event = await $receive->();
        push @received, $event;

        # Then protocol event
        $event = await $receive->();
        push @received, $event;
    };

    my $wrapped = $channels->wrap($inner_app);

    my $protocol_event = { type => 'websocket.receive', text => 'from client' };
    my @protocol_events = ($protocol_event);

    my $scope = { type => 'websocket' };
    my $receive = async sub { shift @protocol_events };
    my $send = async sub { };

    run { $wrapped->($scope, $receive, $send) };

    is($received[0]->{type}, 'chat.msg', 'channel message first');
    is($received[1]->{type}, 'websocket.receive', 'protocol event second');
};

subtest 'cleanup on app exit' => sub {
    my $channels = PAGI::Channels->new();

    my $my_channel;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = $scope->{'pagi.channels'};
        $my_channel = $scope->{'pagi.channel'};

        await $ch->subscribe('room');
        # App exits
    };

    my $wrapped = $channels->wrap($inner_app);

    my $scope = { type => 'websocket' };
    my $receive = async sub { { type => 'websocket.disconnect' } };
    my $send = async sub { };

    run { $wrapped->($scope, $receive, $send) };

    # Publish to room - cleaned up channel should not receive
    run { $channels->backend->publish('room', { type => 'msg' }) };

    is(run { $channels->backend->poll($my_channel) }, undef, 'channel cleaned up');
};

done_testing;
