# t/30-redis/01-core.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run skip_without_redis redis_host redis_port);
use Test2::V0;

my $loop = init_loop();

SKIP: {
    skip_without_redis();

    require PAGI::Channels::Backend::Redis;
    ok(1, 'loaded PAGI::Channels::Backend::Redis');

    subtest 'connect to Redis' => sub {
        my $backend = PAGI::Channels::Backend::Redis->new(
            uri => "redis://" . redis_host() . ":" . redis_port(),
        );

        run { $backend->connect() };
        ok($backend->connected, 'connected to Redis');

        run { $backend->disconnect() };
    };

    subtest 'send and poll' => sub {
        my $backend = PAGI::Channels::Backend::Redis->new(
            uri => "redis://" . redis_host() . ":" . redis_port(),
        );
        run { $backend->connect() };

        # Clear any existing data
        run { $backend->flush() };

        # Send
        run { $backend->send('test:ch1', { type => 'msg', data => 'hello' }) };

        # Poll
        my $msg = run { $backend->poll('test:ch1') };
        is($msg->{type}, 'msg', 'received message type');
        is($msg->{data}, 'hello', 'received message data');

        # Poll again - empty
        $msg = run { $backend->poll('test:ch1') };
        is($msg, undef, 'queue now empty');

        run { $backend->disconnect() };
    };

    subtest 'FIFO ordering' => sub {
        my $backend = PAGI::Channels::Backend::Redis->new(
            uri => "redis://" . redis_host() . ":" . redis_port(),
        );
        run { $backend->connect() };
        run { $backend->flush() };

        run { $backend->send('test:ch', { type => 'msg', n => 1 }) };
        run { $backend->send('test:ch', { type => 'msg', n => 2 }) };
        run { $backend->send('test:ch', { type => 'msg', n => 3 }) };

        is(run { $backend->poll('test:ch') }->{n}, 1, 'first');
        is(run { $backend->poll('test:ch') }->{n}, 2, 'second');
        is(run { $backend->poll('test:ch') }->{n}, 3, 'third');

        run { $backend->disconnect() };
    };
}

done_testing;
