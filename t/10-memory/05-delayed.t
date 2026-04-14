use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Channels::Backend::Memory;

subtest 'send_delayed delivers after delay' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    # Send with 0.1 second delay
    run { $backend->send_delayed('ch1', { type => 'delayed' }, 0.1) };

    # Not delivered immediately
    is(run { $backend->poll('ch1') }, undef, 'not delivered immediately');

    # Process delayed messages
    run { $backend->process_delayed() };
    is(run { $backend->poll('ch1') }, undef, 'still not delivered');

    # Wait and process again
    run {
        my $timer = Future::IO->sleep(0.15);
        await $timer;
    };
    run { $backend->process_delayed() };

    my $msg = run { $backend->poll('ch1') };
    is($msg->{type}, 'delayed', 'delivered after delay');
};

subtest 'publish_delayed delivers to all subscribers after delay' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->subscribe('ch1', 'room') };
    run { $backend->subscribe('ch2', 'room') };

    run { $backend->publish_delayed('room', { type => 'broadcast' }, 0.1) };

    # Not delivered immediately
    is(run { $backend->poll('ch1') }, undef, 'ch1 not delivered yet');
    is(run { $backend->poll('ch2') }, undef, 'ch2 not delivered yet');

    # Wait and process
    run {
        my $timer = Future::IO->sleep(0.15);
        await $timer;
    };
    run { $backend->process_delayed() };

    is(run { $backend->poll('ch1') }->{type}, 'broadcast', 'ch1 received');
    is(run { $backend->poll('ch2') }->{type}, 'broadcast', 'ch2 received');
};

subtest 'multiple delayed messages in order' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->send_delayed('ch', { type => 'msg', n => 1 }, 0.05) };
    run { $backend->send_delayed('ch', { type => 'msg', n => 2 }, 0.15) };
    run { $backend->send_delayed('ch', { type => 'msg', n => 3 }, 0.10) };

    # Wait for all
    run {
        my $timer = Future::IO->sleep(0.2);
        await $timer;
    };
    run { $backend->process_delayed() };

    # Should arrive in delay order: 1, 3, 2
    is(run { $backend->poll('ch') }->{n}, 1, 'first (0.05s)');
    is(run { $backend->poll('ch') }->{n}, 3, 'second (0.10s)');
    is(run { $backend->poll('ch') }->{n}, 2, 'third (0.15s)');
};

subtest 'poll() delivers due delayed messages without manual pump' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->send_delayed('ch1', { type => 'reminder' }, 0.05) };

    # Nothing before the delay elapses
    is(run { $backend->poll('ch1') }, undef, 'not delivered before delay');

    # Wait past the delay
    run {
        my $timer = Future::IO->sleep(0.1);
        await $timer;
    };

    # poll() itself should pump the delayed queue — no manual process_delayed
    my $msg = run { $backend->poll('ch1') };
    is($msg->{type}, 'reminder', 'delayed message pumped by poll()');
};

done_testing;
