# t/30-redis/01-core.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run skip_without_redis make_redis);
use Test2::V0;

my $loop = init_loop();

SKIP: {
    skip_without_redis();

    require PAGI::Channels::Backend::Redis;
    ok(1, 'loaded PAGI::Channels::Backend::Redis');

    subtest 'backend uses the passed Async::Redis instance' => sub {
        # Two backends with distinct prefixes — they must be isolated.
        # Fails if the backend ignores `redis =>` and creates its own
        # connection from a default URI (old behavior).
        my $redis_a = make_redis(prefix => "test:A:$$:");
        my $redis_b = make_redis(prefix => "test:B:$$:");

        my $backend_a = PAGI::Channels::Backend::Redis->new(redis => $redis_a);
        my $backend_b = PAGI::Channels::Backend::Redis->new(redis => $redis_b);

        run { $backend_a->flush() };
        run { $backend_b->flush() };

        run { $backend_a->send('ch1', { type => 'from_a' }) };

        my $msg_b = run { $backend_b->poll('ch1') };
        is($msg_b, undef, 'backend B prefix is isolated from A');

        my $msg_a = run { $backend_a->poll('ch1') };
        is($msg_a->{type}, 'from_a', 'backend A receives its own message');

        run { $backend_a->flush() };
        run { $backend_b->flush() };
    };

    subtest 'send and poll' => sub {
        my $backend = PAGI::Channels::Backend::Redis->new(
            redis => make_redis(),
        );
        run { $backend->flush() };

        run { $backend->send('test.ch1', { type => 'msg', data => 'hello' }) };

        my $msg = run { $backend->poll('test.ch1') };
        is($msg->{type}, 'msg',   'received message type');
        is($msg->{data}, 'hello', 'received message data');

        $msg = run { $backend->poll('test.ch1') };
        is($msg, undef, 'queue now empty');

        run { $backend->flush() };
    };

    subtest 'FIFO ordering' => sub {
        my $backend = PAGI::Channels::Backend::Redis->new(
            redis => make_redis(),
        );
        run { $backend->flush() };

        run { $backend->send('test.ch', { type => 'msg', n => 1 }) };
        run { $backend->send('test.ch', { type => 'msg', n => 2 }) };
        run { $backend->send('test.ch', { type => 'msg', n => 3 }) };

        is(run { $backend->poll('test.ch') }->{n}, 1, 'first');
        is(run { $backend->poll('test.ch') }->{n}, 2, 'second');
        is(run { $backend->poll('test.ch') }->{n}, 3, 'third');

        run { $backend->flush() };
    };
}

done_testing;
