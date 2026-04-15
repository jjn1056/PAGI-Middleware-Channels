use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;
use Future::AsyncAwait;

my $loop = init_loop();

use PAGI::Middleware::Channels;
use PAGI::Middleware::Channels::Backend::Memory;
use PAGI::Channel;

subtest 'from_scope returns PAGI::Channel inside a wrapped app' => sub {
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    my ($got_ch, $got_channel_name);
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;
        my $ch = PAGI::Channel->from_scope($scope);
        $got_ch           = $ch;
        $got_channel_name = $ch->channel_name;
    };

    my $wrapped = $channels->wrap($inner_app);
    run { $wrapped->({ type => 'websocket' }, async sub { { type => 'websocket.disconnect' } }, async sub { }) };

    isa_ok($got_ch, 'PAGI::Channel');
    like($got_channel_name, qr/^conn\./, 'channel_name has conn. prefix');
};

subtest 'from_scope croaks with helpful message when middleware not wired' => sub {
    like(
        dies { PAGI::Channel->from_scope({ type => 'websocket' }) },
        qr/channel layer/i,
        'croaks with message mentioning "channel layer"',
    );
    like(
        dies { PAGI::Channel->from_scope({ type => 'websocket' }) },
        qr/PAGI::Middleware::Channels/i,
        'croaks with message mentioning the middleware class name',
    );
};

subtest 'from_scope channel can subscribe and receive messages' => sub {
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    my @received;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = PAGI::Channel->from_scope($scope);

        await $ch->subscribe('room.test');
        await $ch->backend->send($ch->channel_name, { type => 'ping' });

        my $event = await $receive->();
        push @received, $event;
    };

    my $wrapped = $channels->wrap($inner_app);
    run { $wrapped->({ type => 'websocket' }, async sub { { type => 'websocket.disconnect' } }, async sub { }) };

    is($received[0]{type}, 'ping', 'channel received message via from_scope handle');
};

done_testing;
