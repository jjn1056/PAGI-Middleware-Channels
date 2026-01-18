use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Channels::Backend::Memory;

subtest 'explicit track/untrack' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();
    $backend->set_channel_id('worker.1');

    run { $backend->track('workers.pool', { worker_id => 1, started => 1000 }) };

    my @presence = run { $backend->list_presence('workers.pool') };
    is(scalar @presence, 1, 'one presence entry');
    is($presence[0]->{worker_id}, 1, 'correct data');

    run { $backend->untrack('workers.pool') };

    @presence = run { $backend->list_presence('workers.pool') };
    is(scalar @presence, 0, 'presence removed');
};

subtest 'subscribe with presence option' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();
    $backend->set_channel_id('user.alice');

    run { $backend->subscribe('user.alice', 'chat.room1',
        presence => { user => 'alice', status => 'online' }
    )};

    my @presence = run { $backend->list_presence('chat.room1') };
    is(scalar @presence, 1, 'presence tracked via subscribe');
    is($presence[0]->{user}, 'alice', 'correct user');
    is($presence[0]->{status}, 'online', 'correct status');
};

subtest 'presence events on join/leave' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    # Subscribe ch1 first (to receive events)
    $backend->set_channel_id('ch1');
    run { $backend->subscribe('ch1', 'room', presence => { user => 'ch1' }) };

    # Now ch2 joins - ch1 should get presence.join event
    $backend->set_channel_id('ch2');
    run { $backend->subscribe('ch2', 'room', presence => { user => 'ch2' }) };

    # Check ch1 received join event
    my $event = $backend->poll('ch1');
    is($event->{type}, 'presence.join', 'presence.join event');
    is($event->{presence}{user}, 'ch2', 'correct joiner');

    # ch2 leaves - ch1 should get presence.leave event
    run { $backend->unsubscribe('ch2', 'room') };

    $event = $backend->poll('ch1');
    is($event->{type}, 'presence.leave', 'presence.leave event');
    is($event->{presence}{user}, 'ch2', 'correct leaver');
};

subtest 'list_presence returns all current' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    $backend->set_channel_id('u1');
    run { $backend->subscribe('u1', 'room', presence => { name => 'Alice' }) };

    $backend->set_channel_id('u2');
    run { $backend->subscribe('u2', 'room', presence => { name => 'Bob' }) };

    $backend->set_channel_id('u3');
    run { $backend->subscribe('u3', 'room', presence => { name => 'Carol' }) };

    my @presence = run { $backend->list_presence('room') };
    is(scalar @presence, 3, 'three users present');

    my @names = sort map { $_->{name} } @presence;
    is(\@names, ['Alice', 'Bob', 'Carol'], 'correct names');
};

done_testing;
