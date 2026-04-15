use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Middleware::Channels::Backend::Memory;

subtest 'cleanup removes channel from all groups' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    run { $backend->subscribe('ch1', 'room1') };
    run { $backend->subscribe('ch1', 'room2') };
    run { $backend->subscribe('ch1', 'room3') };

    run { $backend->cleanup('ch1') };

    # Publish to all rooms - ch1 should not receive
    run { $backend->publish('room1', { type => 'msg' }) };
    run { $backend->publish('room2', { type => 'msg' }) };
    run { $backend->publish('room3', { type => 'msg' }) };

    is(run { $backend->poll('ch1') }, undef, 'ch1 removed from all groups');
};

subtest 'cleanup removes pending messages' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    run { $backend->send('ch1', { type => 'msg1' }) };
    run { $backend->send('ch1', { type => 'msg2' }) };

    run { $backend->cleanup('ch1') };

    is(run { $backend->poll('ch1') }, undef, 'queue cleared');
};

subtest 'cleanup removes pattern subscriptions' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    run { $backend->psubscribe('ch1', 'events.*') };
    run { $backend->cleanup('ch1') };
    run { $backend->publish('events.click', { type => 'msg' }) };

    is(run { $backend->poll('ch1') }, undef, 'pattern subscription removed');
};

subtest 'cleanup removes presence and broadcasts leave' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    # ch2 subscribes first to receive events
    $backend->set_channel_id('ch2');
    run { $backend->subscribe('ch2', 'room', presence => { user => 'bob' }) };

    # ch1 subscribes with presence
    $backend->set_channel_id('ch1');
    run { $backend->subscribe('ch1', 'room', presence => { user => 'alice' }) };

    # Consume join event
    run { $backend->poll('ch2') };

    # Cleanup ch1
    run { $backend->cleanup('ch1') };

    # ch2 should get leave event
    my $event = run { $backend->poll('ch2') };
    is($event->{type}, 'presence.leave', 'leave event sent');
    is($event->{presence}{user}, 'alice', 'correct user');

    # Presence list should not include ch1
    my @presence = run { $backend->list_presence('room') };
    is(scalar @presence, 1, 'only one remaining');
    is($presence[0]->{user}, 'bob', 'bob remains');
};

subtest 'flush clears everything' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new(history_size => 10);

    run { $backend->subscribe('ch1', 'room') };
    run { $backend->psubscribe('ch2', 'events.*') };
    run { $backend->send('ch1', { type => 'msg' }) };
    run { $backend->publish('room', { type => 'msg' }) };

    run { $backend->flush() };

    is(run { $backend->poll('ch1') }, undef, 'queues cleared');
    is(scalar keys %{$backend->{groups}}, 0, 'groups cleared');
    is(scalar keys %{$backend->{patterns}}, 0, 'patterns cleared');
    is(scalar keys %{$backend->{presence}}, 0, 'presence cleared');
    is(scalar keys %{$backend->{history}}, 0, 'history cleared');
};

subtest 'cleanup removes delayed messages' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new;
    $backend->set_channel_id('ch1');

    # Schedule delayed messages
    run { $backend->send_delayed('ch1', { type => 'delayed1' }, 10) };
    run { $backend->send_delayed('ch2', { type => 'delayed2' }, 10) };
    run { $backend->publish_delayed('topic1', { type => 'pub' }, 10) };

    # Verify delayed messages exist
    my $delayed = $backend->{delayed};
    is(scalar @$delayed, 3, 'three delayed messages');

    # Cleanup ch1
    run { $backend->cleanup('ch1') };

    # Verify ch1's delayed message removed, others remain
    $delayed = $backend->{delayed};
    is(scalar @$delayed, 2, 'ch1 delayed message removed');

    my @targets = map { $_->{target} } @$delayed;
    ok(!grep { $_ eq 'ch1' } @targets, 'no delayed messages for ch1');
    ok(grep { $_ eq 'ch2' } @targets, 'ch2 delayed message remains');
    ok(grep { $_ eq 'topic1' } @targets, 'publish delayed remains');
};

done_testing;
