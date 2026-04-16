# t/20-facade/02-wrap.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Middleware::Channels;
use PAGI::Middleware::Channels::Backend::Memory;
use Future::AsyncAwait;
use PAGI::Channel;

subtest 'wrap injects scope keys' => sub {
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

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
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    my @received;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = PAGI::Channel->from($scope);
        my $my_channel = $ch->channel_name;

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
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    my $my_channel;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = PAGI::Channel->from($scope);
        $my_channel = $ch->channel_name;

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

subtest 'wrap does not mutate the outer scope' => sub {
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    my $original_scope = { type => 'websocket', path => '/test' };
    my @original_keys  = sort keys %$original_scope;

    my $inner_app = async sub { };

    my $wrapped = $channels->wrap($inner_app);

    my $receive = async sub { { type => 'websocket.disconnect' } };
    my $send    = async sub { };

    run { $wrapped->($original_scope, $receive, $send) };

    my @keys_after = sort keys %$original_scope;
    is(\@keys_after, \@original_keys, 'outer scope keys unchanged');
    ok(!exists $original_scope->{'pagi.channels'}, 'pagi.channels not leaked into outer scope');
    ok(!exists $original_scope->{'pagi.channel'},  'pagi.channel not leaked into outer scope');
};

done_testing;
