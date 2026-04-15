# t/30-redis/02-pubsub.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run skip_without_redis make_redis);
use Test2::V0;

my $loop = init_loop();

SKIP: {
    skip_without_redis();

    require PAGI::Middleware::Channels::Backend::Redis;

    my $make_backend = sub {
        my $backend = PAGI::Middleware::Channels::Backend::Redis->new(
            redis => make_redis(),
        );
        run { $backend->flush() };
        return $backend;
    };

    subtest 'subscribe and publish' => sub {
        my $backend = $make_backend->();

        # Subscribe two channels to a topic
        run { $backend->subscribe('ch1', 'room.general') };
        run { $backend->subscribe('ch2', 'room.general') };

        # Publish to topic
        run { $backend->publish('room.general', { type => 'chat', text => 'hello' }) };

        # Both receive
        my $msg1 = run { $backend->poll('ch1') };
        my $msg2 = run { $backend->poll('ch2') };

        is($msg1->{text}, 'hello', 'ch1 received');
        is($msg2->{text}, 'hello', 'ch2 received');

    };

    subtest 'publish with exclude' => sub {
        my $backend = $make_backend->();

        run { $backend->subscribe('ch1', 'room') };
        run { $backend->subscribe('ch2', 'room') };
        run { $backend->subscribe('ch3', 'room') };

        # Publish excluding ch2
        run { $backend->publish('room', { type => 'msg' }, exclude => 'ch2') };

        ok(run { $backend->poll('ch1') }, 'ch1 received');
        is(run { $backend->poll('ch2') }, undef, 'ch2 excluded');
        ok(run { $backend->poll('ch3') }, 'ch3 received');

    };

    subtest 'publish to full channel drops silently' => sub {
        my $backend = PAGI::Middleware::Channels::Backend::Redis->new(
            redis    => make_redis(prefix => "test:pubsub:full:$$:"),
            capacity => 1,
        );
        run { $backend->flush() };

        run { $backend->subscribe('ch1', 'room') };

        # Fill ch1
        run { $backend->send('ch1', { type => 'fill' }) };

        # Publish should not die even though ch1 is full
        my $ok = run { $backend->publish('room', { type => 'dropped' }) };
        is($ok, 1, 'publish succeeds even with full subscriber');

        # ch1 still only has original message
        is(run { $backend->poll('ch1') }->{type}, 'fill', 'original message');
        is(run { $backend->poll('ch1') }, undef, 'broadcast was dropped');

    };

    subtest 'unsubscribe' => sub {
        my $backend = $make_backend->();

        run { $backend->subscribe('ch1', 'room') };
        run { $backend->unsubscribe('ch1', 'room') };
        run { $backend->publish('room', { type => 'msg' }) };

        is(run { $backend->poll('ch1') }, undef, 'unsubscribed channel does not receive');

    };

    subtest 'subscribe is idempotent' => sub {
        my $backend = $make_backend->();

        run { $backend->subscribe('ch1', 'room') };
        run { $backend->subscribe('ch1', 'room') };  # duplicate
        run { $backend->publish('room', { type => 'msg' }) };

        ok(run { $backend->poll('ch1') }, 'received once');
        is(run { $backend->poll('ch1') }, undef, 'no duplicate');

    };

    subtest 'multiple subscribers' => sub {
        my $backend = $make_backend->();

        # Subscribe multiple channels to multiple topics
        run { $backend->subscribe('ch1', 'topic.a') };
        run { $backend->subscribe('ch2', 'topic.a') };
        run { $backend->subscribe('ch2', 'topic.b') };
        run { $backend->subscribe('ch3', 'topic.b') };

        # Publish to topic.a
        run { $backend->publish('topic.a', { type => 'msg', topic => 'a' }) };

        is(run { $backend->poll('ch1') }->{topic}, 'a', 'ch1 receives topic.a');
        is(run { $backend->poll('ch2') }->{topic}, 'a', 'ch2 receives topic.a');
        is(run { $backend->poll('ch3') }, undef, 'ch3 not subscribed to topic.a');

        # Publish to topic.b
        run { $backend->publish('topic.b', { type => 'msg', topic => 'b' }) };

        is(run { $backend->poll('ch1') }, undef, 'ch1 not subscribed to topic.b');
        is(run { $backend->poll('ch2') }->{topic}, 'b', 'ch2 receives topic.b');
        is(run { $backend->poll('ch3') }->{topic}, 'b', 'ch3 receives topic.b');

    };
}

done_testing;
