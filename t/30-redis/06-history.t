# t/30-redis/06-history.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run skip_without_redis redis_host redis_port);
use Test2::V0;

my $loop = init_loop();

SKIP: {
    skip_without_redis();

    require PAGI::Channels::Backend::Redis;

    my $make_backend = sub {
        my %opts = @_;
        my $backend = PAGI::Channels::Backend::Redis->new(
            uri          => "redis://" . redis_host() . ":" . redis_port(),
            prefix       => 'test:history:',
            history_size => $opts{history_size} // 10,
        );
        run { $backend->connect() };
        run { $backend->flush() };
        return $backend;
    };

    subtest 'subscribe_with_history receives last N messages' => sub {
        my $backend = $make_backend->(history_size => 10);

        # Publish some messages first (no subscribers yet)
        run { $backend->publish('chat.room', { type => 'msg', n => 1 }) };
        run { $backend->publish('chat.room', { type => 'msg', n => 2 }) };
        run { $backend->publish('chat.room', { type => 'msg', n => 3 }) };

        # Now subscribe with history
        run { $backend->subscribe_with_history('ch1', 'chat.room', 10) };

        # Should have received historical messages
        is(run { $backend->poll('ch1') }->{n}, 1, 'history msg 1');
        is(run { $backend->poll('ch1') }->{n}, 2, 'history msg 2');
        is(run { $backend->poll('ch1') }->{n}, 3, 'history msg 3');
        is(run { $backend->poll('ch1') }, undef, 'no more');

        run { $backend->disconnect() };
    };

    subtest 'history respects count limit' => sub {
        my $backend = $make_backend->(history_size => 100);

        for my $n (1..10) {
            run { $backend->publish('room', { type => 'msg', n => $n }) };
        }

        # Request only last 3
        run { $backend->subscribe_with_history('ch1', 'room', 3) };

        is(run { $backend->poll('ch1') }->{n}, 8, 'only last 3: msg 8');
        is(run { $backend->poll('ch1') }->{n}, 9, 'only last 3: msg 9');
        is(run { $backend->poll('ch1') }->{n}, 10, 'only last 3: msg 10');
        is(run { $backend->poll('ch1') }, undef, 'no more');

        run { $backend->disconnect() };
    };

    subtest 'history buffer respects global limit' => sub {
        my $backend = $make_backend->(history_size => 5);

        for my $n (1..10) {
            run { $backend->publish('room', { type => 'msg', n => $n }) };
        }

        # Only last 5 are retained
        run { $backend->subscribe_with_history('ch1', 'room', 100) };

        is(run { $backend->poll('ch1') }->{n}, 6, 'buffer only has 6-10');
        is(run { $backend->poll('ch1') }->{n}, 7, 'msg 7');
        is(run { $backend->poll('ch1') }->{n}, 8, 'msg 8');
        is(run { $backend->poll('ch1') }->{n}, 9, 'msg 9');
        is(run { $backend->poll('ch1') }->{n}, 10, 'msg 10');
        is(run { $backend->poll('ch1') }, undef, 'no more');

        run { $backend->disconnect() };
    };

    subtest 'new messages after subscribe arrive normally' => sub {
        my $backend = $make_backend->(history_size => 10);

        run { $backend->publish('room', { type => 'history', n => 1 }) };
        run { $backend->subscribe_with_history('ch1', 'room', 10) };

        # Consume history
        run { $backend->poll('ch1') };

        # New message
        run { $backend->publish('room', { type => 'live', n => 2 }) };

        my $msg = run { $backend->poll('ch1') };
        is($msg->{type}, 'live', 'live message received');
        is($msg->{n}, 2, 'correct content');

        run { $backend->disconnect() };
    };

    subtest 'history does not include presence events' => sub {
        my $backend = $make_backend->(history_size => 10);

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

        run { $backend->disconnect() };
    };
}

done_testing;
