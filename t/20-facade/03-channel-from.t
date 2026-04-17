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

subtest 'from returns PAGI::Channel inside a wrapped app' => sub {
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    my ($got_ch, $got_channel_name);
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;
        my $ch = PAGI::Channel->from($scope);
        $got_ch           = $ch;
        $got_channel_name = $ch->channel_name;
    };

    my $wrapped = $channels->wrap($inner_app);
    run { $wrapped->({ type => 'websocket' }, async sub { { type => 'websocket.disconnect' } }, async sub { }) };

    isa_ok($got_ch, 'PAGI::Channel');
    like($got_channel_name, qr/^conn\./, 'channel_name has conn. prefix');
};

subtest 'from croaks when middleware not wired' => sub {
    like(
        dies { PAGI::Channel->from({ type => 'websocket' }) },
        qr/channel layer/i,
        'croaks with message mentioning "channel layer"',
    );
    like(
        dies { PAGI::Channel->from({ type => 'websocket' }) },
        qr/PAGI::Middleware::Channels/i,
        'croaks with message mentioning the middleware class name',
    );
};

subtest 'from accepts object with ->scope method' => sub {
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    my ($got_ch, $got_channel_name);
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;
        my $obj = bless { scope => $scope }, 'MockScopeHolder';
        my $ch = PAGI::Channel->from($obj);
        $got_ch           = $ch;
        $got_channel_name = $ch->channel_name;
    };

    my $wrapped = $channels->wrap($inner_app);
    run { $wrapped->({ type => 'websocket' }, async sub { { type => 'websocket.disconnect' } }, async sub { }) };

    isa_ok($got_ch, 'PAGI::Channel');
    like($got_channel_name, qr/^conn\./, 'channel_name has conn. prefix via object');
};

subtest 'from channel can subscribe and receive messages' => sub {
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    my @received;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = PAGI::Channel->from($scope);

        await $ch->subscribe('room.test');
        await $ch->backend->send($ch->channel_name, { type => 'ping' });

        my $event = await $receive->();
        push @received, $event;
    };

    my $wrapped = $channels->wrap($inner_app);
    run { $wrapped->({ type => 'websocket' }, async sub { { type => 'websocket.disconnect' } }, async sub { }) };

    is($received[0]{type}, 'ping', 'channel received message via from handle');
};

subtest 'count_presence delegates to backend' => sub {
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    my $got_count;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;

        my $ch = PAGI::Channel->from($scope);

        await $ch->subscribe('count.room', presence => { user => 'tester' });

        $got_count = await $ch->count_presence('count.room');
    };

    my $wrapped = $channels->wrap($inner_app);
    run { $wrapped->({ type => 'websocket' }, async sub { { type => 'websocket.disconnect' } }, async sub { }) };

    is($got_count, 1, 'count_presence returns 1');
};


subtest 'scan_presence delegates to backend' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    my $channels = PAGI::Middleware::Channels->new(
        backend => $backend,
    );

    my @all;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;
        my $ch = PAGI::Channel->from($scope);

        for my $i (1..3) {
            await $backend->subscribe("scan.u$i", 'scan.facade.room');
            await $backend->track('scan.facade.room', "scan.u$i", { n => $i });
        }

        my $cursor = 0;
        do {
            my @batch;
            ($cursor, @batch) = await $ch->scan_presence(
                'scan.facade.room', cursor => $cursor, count => 10);
            push @all, @batch;
        } while ($cursor);
    };

    my $wrapped = $channels->wrap($inner_app);
    run { $wrapped->({ type => 'websocket' }, async sub { { type => 'websocket.disconnect' } }, async sub { }) };

    is(scalar @all, 3, 'scan_presence via facade returns all 3 entries');
};

subtest 'list_presence limit croaks via facade when exceeded' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    my $channels = PAGI::Middleware::Channels->new(
        backend => $backend,
    );

    my $err;
    my $inner_app = async sub {
        my ($scope, $receive, $send) = @_;
        my $ch = PAGI::Channel->from($scope);

        for my $i (1..3) {
            await $backend->subscribe("lim.u$i", 'lim.facade.room');
            await $backend->track('lim.facade.room', "lim.u$i", { n => $i });
        }

        eval {
            await $ch->list_presence('lim.facade.room', limit => 2);
        };
        $err = $@;
    };

    my $wrapped = $channels->wrap($inner_app);
    run { $wrapped->({ type => 'websocket' }, async sub { { type => 'websocket.disconnect' } }, async sub { }) };

    like($err, qr/exceeds limit/, 'limit croak propagates via facade');
};

# Minimal mock for testing object-with-scope support
package MockScopeHolder;
sub scope { shift->{scope} }

package main;

done_testing;
