# t/10-memory/09-next-message.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;
use Future::AsyncAwait;
use Future;

my $loop = init_loop();

use PAGI::Middleware::Channels::Backend::Memory;

subtest 'next_message returns queued message immediately' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();
    run { $backend->send('ch1', { type => 'test', text => 'hello' }) };

    my $msg = run { $backend->next_message('ch1') };
    is($msg->{type}, 'test', 'got queued message');
    is($msg->{text}, 'hello', 'correct payload');
};

subtest 'next_message waits and resolves when send delivers' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    run {
        (async sub {
            # Start waiting (will suspend — queue is empty)
            my $wait_f = $backend->next_message('ch1');

            # Send a message (resolves the waiter synchronously)
            await $backend->send('ch1', { type => 'delayed', n => 42 });

            # The waiter should now be done
            my $msg = await $wait_f;
            is($msg->{type}, 'delayed', 'next_message resolved after send');
            is($msg->{n}, 42, 'correct payload');
        })->();
    };
};

subtest 'next_message wakes on publish (via _deliver_to_channel)' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    # Subscribe ch1 to a topic so publish reaches it
    run { $backend->subscribe('ch1', 'room') };

    run {
        (async sub {
            my $wait_f = $backend->next_message('ch1');

            # Publish to room — delivers to ch1 via _deliver_to_channel
            await $backend->publish('room', { type => 'chat', text => 'hi' });

            my $msg = await $wait_f;
            is($msg->{type}, 'chat', 'next_message woke on publish');
        })->();
    };
};

subtest 'next_message cancel cleans up waiter' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    my $f = $backend->next_message('ch1');
    ok(!$f->is_ready, 'future is pending');

    $f->cancel;
    ok($f->is_cancelled, 'future cancelled');

    # Internal waiters list should be clean
    my $waiters = $backend->{_waiters}{'ch1'} // [];
    my @active = grep { !$_->is_cancelled } @$waiters;
    is(scalar @active, 0, 'no active waiters after cancel');
};

subtest 'cleanup cancels waiters for channel' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    my $f = $backend->next_message('ch1');
    ok(!$f->is_ready, 'waiter pending before cleanup');

    run { $backend->cleanup('ch1') };
    ok($f->is_cancelled, 'waiter cancelled by cleanup');
};

subtest 'flush cancels all waiters' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    my $f1 = $backend->next_message('ch1');
    my $f2 = $backend->next_message('ch2');

    run { $backend->flush() };
    ok($f1->is_cancelled, 'ch1 waiter cancelled');
    ok($f2->is_cancelled, 'ch2 waiter cancelled');
};

subtest 'next_message works after prior cancel' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    # Cancel a pending next_message
    my $f = $backend->next_message('ch1');
    $f->cancel;

    # Now send and retrieve normally
    run { $backend->send('ch1', { type => 'after-cancel' }) };
    my $msg = run { $backend->next_message('ch1') };
    is($msg->{type}, 'after-cancel', 'next_message works after prior cancel');
};

done_testing;
