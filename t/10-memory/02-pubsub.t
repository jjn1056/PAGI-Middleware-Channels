use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Channels::Backend::Memory;

subtest 'subscribe and publish' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    # Subscribe two channels to a topic
    run { $backend->subscribe('ch1', 'room.general') };
    run { $backend->subscribe('ch2', 'room.general') };

    # Publish to topic
    run { $backend->publish('room.general', { type => 'chat', text => 'hello' }) };

    # Both receive
    my $msg1 = $backend->poll('ch1');
    my $msg2 = $backend->poll('ch2');

    is($msg1->{text}, 'hello', 'ch1 received');
    is($msg2->{text}, 'hello', 'ch2 received');
};

subtest 'publish with exclude' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->subscribe('ch1', 'room') };
    run { $backend->subscribe('ch2', 'room') };
    run { $backend->subscribe('ch3', 'room') };

    # Publish excluding ch2
    run { $backend->publish('room', { type => 'msg' }, exclude => 'ch2') };

    ok($backend->poll('ch1'), 'ch1 received');
    is($backend->poll('ch2'), undef, 'ch2 excluded');
    ok($backend->poll('ch3'), 'ch3 received');
};

subtest 'publish to full channel drops silently' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new(capacity => 1);

    run { $backend->subscribe('ch1', 'room') };

    # Fill ch1
    run { $backend->send('ch1', { type => 'fill' }) };

    # Publish should not die even though ch1 is full
    my $ok = run { $backend->publish('room', { type => 'dropped' }) };
    is($ok, 1, 'publish succeeds even with full subscriber');

    # ch1 still only has original message
    is($backend->poll('ch1')->{type}, 'fill', 'original message');
    is($backend->poll('ch1'), undef, 'broadcast was dropped');
};

subtest 'unsubscribe' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->subscribe('ch1', 'room') };
    run { $backend->unsubscribe('ch1', 'room') };
    run { $backend->publish('room', { type => 'msg' }) };

    is($backend->poll('ch1'), undef, 'unsubscribed channel does not receive');
};

subtest 'subscribe is idempotent' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->subscribe('ch1', 'room') };
    run { $backend->subscribe('ch1', 'room') };  # duplicate
    run { $backend->publish('room', { type => 'msg' }) };

    ok($backend->poll('ch1'), 'received once');
    is($backend->poll('ch1'), undef, 'no duplicate');
};

done_testing;
