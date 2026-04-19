# t/30-redis/01-redis-specific.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run skip_without_redis make_redis);
use Test2::V0;
use Future::IO;
use Future::AsyncAwait;

init_loop();

SKIP: {
    skip_without_redis();
    require PAGI::Middleware::Channels::Backend::Redis;

    subtest 'backend uses the passed Async::Redis instance' => sub {
        # Two backends with distinct prefixes — they must be isolated.
        # Fails if the backend ignores `redis =>` and creates its own
        # connection from a default URI (old behavior).
        my $redis_a = make_redis(prefix => "test:A:$$:");
        my $redis_b = make_redis(prefix => "test:B:$$:");

        my $backend_a = PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis_a);
        my $backend_b = PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis_b);

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

    subtest 'schedule_delayed sets TTL on delayed ZSET' => sub {
        my $redis = make_redis(prefix => "test:delayed-ttl:$$:");
        my $backend = PAGI::Middleware::Channels::Backend::Redis->new(
            redis  => $redis,
            expiry => 3600,  # 1 hour
        );
        run { $backend->flush() };
        run { $backend->send_delayed('ch', { type => 'x' }, 0.1) };

        my $ttl = run { $redis->ttl('delayed') };
        ok($ttl > 0, "delayed key has TTL set (got $ttl)");
        ok($ttl <= 3600, "TTL does not exceed expiry");

        run { $backend->flush() };
    };

    subtest 'schedule_delayed uses unique monotonic IDs' => sub {
        my $redis = make_redis(prefix => "test:delayed-id:$$:");
        my $backend = PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis);
        run { $backend->flush() };

        # Schedule 100 identical messages at the same delay — each must
        # serialize to a distinct ZSET member.
        for (1..100) {
            run { $backend->send_delayed('ch', { type => 'same' }, 0.5) };
        }

        my $count = run { $redis->zcard('delayed') };
        is($count, 100, 'all 100 entries stored as distinct ZSET members');

        run { $backend->flush() };
    };

}

done_testing;
