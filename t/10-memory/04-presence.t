use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Middleware::Channels::Backend::Memory;

subtest 'explicit track/untrack' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();
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
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();
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
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    # Subscribe ch1 first (to receive events)
    $backend->set_channel_id('ch1');
    run { $backend->subscribe('ch1', 'room', presence => { user => 'ch1' }) };

    # Now ch2 joins - ch1 should get presence.join event
    $backend->set_channel_id('ch2');
    run { $backend->subscribe('ch2', 'room', presence => { user => 'ch2' }) };

    # Check ch1 received join event
    my $event = run { $backend->poll('ch1') };
    is($event->{type}, 'presence.join', 'presence.join event');
    is($event->{presence}{user}, 'ch2', 'correct joiner');

    # ch2 leaves - ch1 should get presence.leave event
    run { $backend->unsubscribe('ch2', 'room') };

    $event = run { $backend->poll('ch1') };
    is($event->{type}, 'presence.leave', 'presence.leave event');
    is($event->{presence}{user}, 'ch2', 'correct leaver');
};

subtest 'list_presence returns all current' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

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

subtest 'count_presence returns zero for unknown topic' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();
    my $count = run { $backend->count_presence('no.such.topic') };
    is($count, 0, 'zero for unknown topic');
};

subtest 'count_presence returns number of non-expired entries' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    $backend->set_channel_id('u1');
    run { $backend->subscribe('u1', 'room', presence => { name => 'Alice' }) };

    $backend->set_channel_id('u2');
    run { $backend->subscribe('u2', 'room', presence => { name => 'Bob' }) };

    my $count = run { $backend->count_presence('room') };
    is($count, 2, 'two users present');

    run { $backend->unsubscribe('u2', 'room') };
    $count = run { $backend->count_presence('room') };
    is($count, 1, 'one user after unsubscribe');
};

subtest 'list_presence with limit — under limit succeeds' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    $backend->set_channel_id('u1');
    run { $backend->subscribe('u1', 'lim.room', presence => { n => 1 }) };
    $backend->set_channel_id('u2');
    run { $backend->subscribe('u2', 'lim.room', presence => { n => 2 }) };

    my @presence = run { $backend->list_presence('lim.room', limit => 5) };
    is(scalar @presence, 2, 'returns 2 entries when under limit of 5');
};

subtest 'list_presence with limit — over limit croaks' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    $backend->set_channel_id('u1');
    run { $backend->subscribe('u1', 'big.room', presence => { n => 1 }) };
    $backend->set_channel_id('u2');
    run { $backend->subscribe('u2', 'big.room', presence => { n => 2 }) };
    $backend->set_channel_id('u3');
    run { $backend->subscribe('u3', 'big.room', presence => { n => 3 }) };

    my $err = dies {
        run { $backend->list_presence('big.room', limit => 2) };
    };
    like($err, qr/exceeds limit/, 'croaks with helpful message');
    like($err, qr/scan_presence/, 'message mentions scan_presence');
};

subtest 'scan_presence with cursor 0 returns all entries for small set' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    for my $i (1..3) {
        $backend->set_channel_id("u$i");
        run { $backend->subscribe("u$i", 'scan.room', presence => { n => $i }) };
    }

    my ($cursor, @entries) = run { $backend->scan_presence('scan.room', cursor => 0, count => 10) };
    is($cursor, 0, 'cursor 0 means done');
    is(scalar @entries, 3, 'all 3 entries returned');
};

subtest 'scan_presence paginates correctly' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();

    for my $i (1..5) {
        $backend->set_channel_id("u$i");
        run { $backend->subscribe("u$i", 'page.room', presence => { n => $i }) };
    }

    my @all;
    my $cursor = 0;
    do {
        my @batch;
        ($cursor, @batch) = run { $backend->scan_presence('page.room', cursor => $cursor, count => 2) };
        push @all, @batch;
    } while ($cursor);

    is(scalar @all, 5, 'collected all 5 entries across pages');
};

subtest 'scan_presence on empty topic returns empty' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new();
    my ($cursor, @entries) = run { $backend->scan_presence('empty.room', cursor => 0, count => 10) };
    is($cursor, 0, 'cursor 0');
    is(scalar @entries, 0, 'no entries');
};

done_testing;
