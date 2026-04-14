# t/30-redis/03-patterns.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run skip_without_redis make_redis);
use Test2::V0;

my $loop = init_loop();

SKIP: {
    skip_without_redis();

    require PAGI::Channels::Backend::Redis;

    my $make_backend = sub {
        my $backend = PAGI::Channels::Backend::Redis->new(
            redis => make_redis(),
        );
        run { $backend->flush() };
        return $backend;
    };

    subtest 'single-level wildcard (*)' => sub {
        my $backend = $make_backend->();

        # chat.* matches chat.room1, chat.general, NOT chat.room1.messages
        run { $backend->psubscribe('ch1', 'chat.*') };

        run { $backend->publish('chat.room1', { type => 'msg', n => 1 }) };
        run { $backend->publish('chat.general', { type => 'msg', n => 2 }) };
        run { $backend->publish('chat.room1.messages', { type => 'msg', n => 3 }) };
        run { $backend->publish('notifications', { type => 'msg', n => 4 }) };

        is(run { $backend->poll('ch1') }->{n}, 1, 'chat.room1 matched');
        is(run { $backend->poll('ch1') }->{n}, 2, 'chat.general matched');
        is(run { $backend->poll('ch1') }, undef, 'chat.room1.messages NOT matched');

    };

    subtest 'multi-level wildcard (**)' => sub {
        my $backend = $make_backend->();

        # notifications.** matches notifications, notifications.user, notifications.user.123
        run { $backend->psubscribe('ch1', 'notifications.**') };

        run { $backend->publish('notifications', { type => 'msg', n => 1 }) };
        run { $backend->publish('notifications.user', { type => 'msg', n => 2 }) };
        run { $backend->publish('notifications.user.123.email', { type => 'msg', n => 3 }) };
        run { $backend->publish('alerts', { type => 'msg', n => 4 }) };

        is(run { $backend->poll('ch1') }->{n}, 1, 'notifications matched');
        is(run { $backend->poll('ch1') }->{n}, 2, 'notifications.user matched');
        is(run { $backend->poll('ch1') }->{n}, 3, 'notifications.user.123.email matched');
        is(run { $backend->poll('ch1') }, undef, 'alerts NOT matched');

    };

    subtest 'punsubscribe' => sub {
        my $backend = $make_backend->();

        run { $backend->psubscribe('ch1', 'events.*') };
        run { $backend->punsubscribe('ch1', 'events.*') };
        run { $backend->publish('events.click', { type => 'msg' }) };

        is(run { $backend->poll('ch1') }, undef, 'punsubscribed pattern no longer matches');

    };

    subtest 'mixed exact and pattern subscriptions' => sub {
        my $backend = $make_backend->();

        # Exact subscription
        run { $backend->subscribe('ch1', 'room.vip') };
        # Pattern subscription
        run { $backend->psubscribe('ch1', 'room.*') };

        run { $backend->publish('room.vip', { type => 'msg' }) };

        # Should only receive once (dedup)
        ok(run { $backend->poll('ch1') }, 'received message');
        is(run { $backend->poll('ch1') }, undef, 'no duplicate from pattern');

    };

    subtest 'multiple patterns per channel' => sub {
        my $backend = $make_backend->();

        run { $backend->psubscribe('ch1', 'users.*') };
        run { $backend->psubscribe('ch1', 'orders.*') };

        run { $backend->publish('users.login', { type => 'msg', n => 1 }) };
        run { $backend->publish('orders.new', { type => 'msg', n => 2 }) };
        run { $backend->publish('products.update', { type => 'msg', n => 3 }) };

        is(run { $backend->poll('ch1') }->{n}, 1, 'users.login matched');
        is(run { $backend->poll('ch1') }->{n}, 2, 'orders.new matched');
        is(run { $backend->poll('ch1') }, undef, 'products.update NOT matched');

    };
}

done_testing;
