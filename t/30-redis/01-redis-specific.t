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

    subtest 'schedule_delayed sets TTL that outlasts the delivery time' => sub {
        my $redis = make_redis(prefix => "test:delayed-ttl:$$:");
        my $backend = PAGI::Middleware::Channels::Backend::Redis->new(
            redis     => $redis,
            max_delay => 3600,  # 1 hour
        );
        run { $backend->flush() };
        run { $backend->send_delayed('ch', { type => 'x' }, 60) };

        my $ttl = run { $redis->ttl('delayed') };
        ok($ttl >= 60,   "TTL outlasts the scheduled delivery (got $ttl)");
        ok($ttl <= 3600, "TTL does not exceed max_delay");

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

    subtest 'subscriber_factory is used for the subscriber connection' => sub {
        my $redis = make_redis(prefix => "test:sub-factory:$$:");

        my $factory_called = 0;
        my $factory = sub {
            $factory_called++;
            require Async::Redis;
            my $sub = Async::Redis->new(
                uri    => "redis://" . Test::PAGI::Channels::redis_host() . ":" . Test::PAGI::Channels::redis_port(),
                prefix => $redis->{prefix},
            );
            $sub->connect->get;
            return $sub;
        };

        my $backend = PAGI::Middleware::Channels::Backend::Redis->new(
            redis              => $redis,
            subscriber_factory => $factory,
        );
        run { $backend->flush() };

        # Force subscriber creation by awaiting on next_message.
        my $f = $backend->next_message('sub-factory-ch');
        run { Future::IO->sleep(0.05) };
        is($factory_called, 1, 'subscriber_factory was invoked exactly once');

        $f->cancel;
        run { $backend->cleanup('sub-factory-ch') };
        run { $backend->flush() };
    };

    subtest 'pattern regex cache is populated and cleared on flush' => sub {
        my $redis = make_redis(prefix => "test:pat-cache:$$:");
        my $backend = PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis);
        run { $backend->flush() };

        run { $backend->psubscribe('ch1', 'events.*') };
        run { $backend->publish('events.click', { type => 'x' }) };

        ok($backend->{_pattern_regex_cache}{'events.*'},
            'regex cached after first publish match');

        run { $backend->flush() };

        is(
            $backend->{_pattern_regex_cache}, {},
            'cache cleared by flush'
        );
    };

    subtest 'patterns:index maintains membership invariants' => sub {
        my $redis = make_redis(prefix => "test:pat-idx:$$:");
        my $backend = PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis);
        run { $backend->flush() };

        # Initially empty.
        is(run { $redis->scard('patterns:index') }, 0, 'index empty at start');

        run { $backend->psubscribe('ch-a', 'evt.*') };
        run { $backend->psubscribe('ch-b', 'log.**') };

        my $members_ref = run { $redis->smembers('patterns:index') };
        my @members = sort @{ $members_ref || [] };
        is(\@members, ['ch-a', 'ch-b'], 'both channels in index after psubscribe');

        run { $backend->punsubscribe('ch-a', 'evt.*') };
        is(run { $redis->scard('patterns:index') }, 1,
            'ch-a removed from index after its only pattern is dropped');

        run { $backend->cleanup('ch-b') };
        is(run { $redis->scard('patterns:index') }, 0,
            'index empty after cleanup of remaining channel');

        run { $backend->flush() };
    };

    subtest 'memberships:<channel> reverse index' => sub {
        my $redis = make_redis(prefix => "test:memb:$$:");
        my $backend = PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis);
        run { $backend->flush() };

        run { $backend->subscribe('ch1', 'room.a') };
        run { $backend->subscribe('ch1', 'room.b') };
        run { $backend->subscribe('ch2', 'room.a') };

        my $ch1_ref = run { $redis->smembers('memberships:ch1') };
        my @ch1 = sort @{ $ch1_ref || [] };
        is(\@ch1, ['room.a', 'room.b'], 'ch1 memberships indexed');

        run { $backend->unsubscribe('ch1', 'room.a') };
        $ch1_ref = run { $redis->smembers('memberships:ch1') };
        @ch1 = sort @{ $ch1_ref || [] };
        is(\@ch1, ['room.b'], 'unsubscribe updates index');

        # cleanup removes the index entry AND removes ch1 from room.b's group
        run { $backend->cleanup('ch1') };
        is(run { $redis->exists('memberships:ch1') }, 0,
            'cleanup deletes ch1 memberships key');
        my $room_b_ref = run { $redis->smembers('g:room.b') };
        my @room_b = @{ $room_b_ref || [] };
        is(\@room_b, [], 'cleanup removed ch1 from room.b group');
        # ch2 still in room.a
        my $room_a_ref = run { $redis->smembers('g:room.a') };
        my @room_a = @{ $room_a_ref || [] };
        is(\@room_a, ['ch2'], 'cleanup did not touch other channels');

        run { $backend->flush() };
    };

    subtest 'presence:channel:<channel> reverse index' => sub {
        my $redis = make_redis(prefix => "test:pres-idx:$$:");
        my $backend = PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis);
        run { $backend->flush() };

        run { $backend->track('room1', 'alice', { user => 'alice' }) };
        run { $backend->track('room2', 'alice', { user => 'alice' }) };
        run { $backend->track('room1', 'bob',   { user => 'bob'   }) };

        my $alice_ref = run { $redis->smembers('presence:channel:alice') };
        my @alice = sort @{ $alice_ref || [] };
        is(\@alice, ['room1', 'room2'], 'alice indexed into two rooms');

        # _presence_topics_for_channel returns topic + data
        my @entries = run { $backend->_presence_topics_for_channel('alice') };
        my @topics = sort map { $_->[0] } @entries;
        is(\@topics, ['room1', 'room2'], 'lookup returns both topics for alice');

        run { $backend->untrack('room1', 'alice') };
        $alice_ref = run { $redis->smembers('presence:channel:alice') };
        @alice = sort @{ $alice_ref || [] };
        is(\@alice, ['room2'], 'untrack updates index');

        run { $backend->flush() };
    };

}

done_testing;
