use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Channels::Backend::Memory;

subtest 'subscribe_with_history receives last N messages' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new(history_size => 10);

    # Publish some messages first (no subscribers yet)
    run { $backend->publish('chat.room', { type => 'msg', n => 1 }) };
    run { $backend->publish('chat.room', { type => 'msg', n => 2 }) };
    run { $backend->publish('chat.room', { type => 'msg', n => 3 }) };

    # Now subscribe with history
    run { $backend->subscribe_with_history('ch1', 'chat.room', 10) };

    # Should have received historical messages
    is($backend->poll('ch1')->{n}, 1, 'history msg 1');
    is($backend->poll('ch1')->{n}, 2, 'history msg 2');
    is($backend->poll('ch1')->{n}, 3, 'history msg 3');
    is($backend->poll('ch1'), undef, 'no more');
};

subtest 'history respects count limit' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new(history_size => 100);

    for my $n (1..10) {
        run { $backend->publish('room', { type => 'msg', n => $n }) };
    }

    # Request only last 3
    run { $backend->subscribe_with_history('ch1', 'room', 3) };

    is($backend->poll('ch1')->{n}, 8, 'only last 3: msg 8');
    is($backend->poll('ch1')->{n}, 9, 'only last 3: msg 9');
    is($backend->poll('ch1')->{n}, 10, 'only last 3: msg 10');
    is($backend->poll('ch1'), undef, 'no more');
};

subtest 'history buffer respects global limit' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new(history_size => 5);

    for my $n (1..10) {
        run { $backend->publish('room', { type => 'msg', n => $n }) };
    }

    # Only last 5 are retained
    run { $backend->subscribe_with_history('ch1', 'room', 100) };

    is($backend->poll('ch1')->{n}, 6, 'buffer only has 6-10');
    is($backend->poll('ch1')->{n}, 7, 'msg 7');
    is($backend->poll('ch1')->{n}, 8, 'msg 8');
    is($backend->poll('ch1')->{n}, 9, 'msg 9');
    is($backend->poll('ch1')->{n}, 10, 'msg 10');
    is($backend->poll('ch1'), undef, 'no more');
};

subtest 'new messages after subscribe arrive normally' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new(history_size => 10);

    run { $backend->publish('room', { type => 'history', n => 1 }) };
    run { $backend->subscribe_with_history('ch1', 'room', 10) };

    # Consume history
    $backend->poll('ch1');

    # New message
    run { $backend->publish('room', { type => 'live', n => 2 }) };

    my $msg = $backend->poll('ch1');
    is($msg->{type}, 'live', 'live message received');
    is($msg->{n}, 2, 'correct content');
};

done_testing;
