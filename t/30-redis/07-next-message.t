# t/30-redis/07-next-message.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run skip_without_redis make_redis);
use Test2::V0;
use Future::AsyncAwait;
use Future;
use Future::IO;

my $loop = init_loop();

SKIP: {
    skip_without_redis();

    require PAGI::Middleware::Channels::Backend::Redis;
    ok(1, 'loaded Redis backend');

    my $make_backend = sub {
        my $redis = make_redis(prefix => "test:nm:$$:" . ++$main::_nm_counter . ":");
        my $b = PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis);
        run { $b->flush() };
        return $b;
    };

    subtest 'next_message returns queued message immediately' => sub {
        my $backend = $make_backend->();
        run { $backend->send('ch1', { type => 'test', text => 'hello' }) };

        my $msg = run { $backend->next_message('ch1') };
        is($msg->{type}, 'test', 'got queued message');
        is($msg->{text}, 'hello', 'correct payload');
    };

    subtest 'next_message waits and resolves when send delivers' => sub {
        my $backend = $make_backend->();

        run {
            (async sub {
                my $wait_f = $backend->next_message('ch1');

                # Small yield to let subscriber set up
                await Future::IO->sleep(0.05);

                await $backend->send('ch1', { type => 'delayed', n => 42 });

                my $msg = await $wait_f;
                is($msg->{type}, 'delayed', 'next_message resolved after send');
                is($msg->{n}, 42, 'correct payload');
            })->();
        };
    };

    subtest 'next_message wakes on publish (via _deliver_to_channel)' => sub {
        my $backend = $make_backend->();
        run { $backend->subscribe('ch1', 'room') };

        run {
            (async sub {
                my $wait_f = $backend->next_message('ch1');

                await Future::IO->sleep(0.05);

                await $backend->publish('room', { type => 'chat', text => 'hi' });

                my $msg = await $wait_f;
                is($msg->{type}, 'chat', 'next_message woke on publish');
            })->();
        };
    };

    subtest 'next_message works after cancel' => sub {
        my $backend = $make_backend->();

        my $f = $backend->next_message('ch1');
        # Need to let subscriber set up before cancelling
        run { (async sub { await Future::IO->sleep(0.05) })->() };
        $f->cancel;

        run { $backend->send('ch1', { type => 'after-cancel' }) };
        my $msg = run { $backend->next_message('ch1') };
        is($msg->{type}, 'after-cancel', 'works after cancel');
    };

    subtest 'flush cancels waiters' => sub {
        my $backend = $make_backend->();

        my $f = $backend->next_message('ch1');
        ok(!$f->is_ready, 'waiter pending');

        run { $backend->flush() };
        ok($f->is_cancelled, 'waiter cancelled by flush');
    };
}

done_testing;
