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

    subtest 'process_delayed returns count of processed messages' => sub {
        my $backend = PAGI::Middleware::Channels::Backend::Redis->new(
            redis => make_redis(),
        );
        run { $backend->flush() };

        run { $backend->send_delayed('ch', { type => 'msg', n => 1 }, 0.05) };
        run { $backend->send_delayed('ch', { type => 'msg', n => 2 }, 0.05) };
        run { $backend->send_delayed('ch', { type => 'msg', n => 3 }, 0.05) };

        # Wait for all
        run { Future::IO->sleep(0.1) };

        my $count = run { $backend->process_delayed() };
        is($count, 3, 'processed 3 messages');

        run { $backend->flush() };
    };

    subtest 'history does not include presence events' => sub {
        my $backend = PAGI::Middleware::Channels::Backend::Redis->new(
            redis        => make_redis(),
            history_size => 10,
        );
        run { $backend->flush() };

        # Regular message
        run { $backend->publish('room', { type => 'msg', n => 1 }) };

        # Presence event (should not be stored in history)
        $backend->set_channel_id('user1');
        run { $backend->subscribe('user1', 'room', presence => { name => 'User1' }) };

        # Another regular message
        run { $backend->publish('room', { type => 'msg', n => 2 }) };

        # Drain user1's queue
        while (run { $backend->poll('user1') }) {}

        # New subscriber with history
        run { $backend->subscribe_with_history('ch2', 'room', 10) };

        # Should only get the regular messages, not presence events
        my @msgs;
        while (my $m = run { $backend->poll('ch2') }) {
            push @msgs, $m;
        }

        # Filter out presence events we might have received during subscribe
        @msgs = grep { $_->{type} !~ /^presence\./ } @msgs;

        is(scalar @msgs, 2, 'got 2 history messages');
        is($msgs[0]->{n}, 1, 'first history message');
        is($msgs[1]->{n}, 2, 'second history message');

        run { $backend->flush() };
    };
}

done_testing;
