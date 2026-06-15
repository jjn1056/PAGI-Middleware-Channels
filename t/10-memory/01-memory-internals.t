# t/10-memory/01-memory-internals.t
#
# Tests that inspect Memory backend internal data structures.
# These complement the contract suite by verifying that flush() and cleanup()
# actually zero out the in-memory state, not just produce correct observable
# behavior through the public API.
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Middleware::Channels::Backend::Memory;

subtest 'flush clears all internal state' => sub {
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
    is(scalar keys %{$backend->{history_seq}}, 0, 'history_seq cleared');
};

subtest 'cleanup removes delayed messages from internal queue' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new;

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

subtest 'next_message cancel cleans up internal waiters list' => sub {
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

subtest 'history records a monotonic cursor and returns it' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new(history_size => 10);
    my $c1 = run { $backend->_record_history('t', { type => 'msg', n => 1 }) };
    my $c2 = run { $backend->_record_history('t', { type => 'msg', n => 2 }) };
    ok($c1, 'first record returns a cursor');
    ok($c2 > $c1, 'cursor is monotonic increasing');
    is(run { $backend->_record_history('t', { type => 'presence.join' }) }, undef,
       'presence events are not recorded and return undef');
    is(run { $backend->_record_history('t', { type => 'msg' }) }, $c2 + 1,
       'cursor continues after the skipped presence event');
};

subtest 'read_history supports since-cursor replay and tags _seq' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new(history_size => 10);
    my $c1 = run { $backend->_record_history('t', { type => 'msg', n => 1 }) };
    my $c2 = run { $backend->_record_history('t', { type => 'msg', n => 2 }) };
    my $c3 = run { $backend->_record_history('t', { type => 'msg', n => 3 }) };

    my @last2 = run { $backend->read_history('t', 2) };                 # legacy: last N
    is(scalar @last2, 2, 'last-N still works');
    is($last2[0]{n}, 2, 'oldest-first');
    is($last2[1]{_seq}, $c3, 'returned messages carry their _seq');

    my @after1 = run { $backend->read_history('t', 100, since => $c1) }; # cursor replay
    is([map { $_->{n} } @after1], [2, 3], 'since returns strictly-after, oldest-first');
    is($after1[0]{_seq}, $c2, 'first replayed _seq is c2');

    my @caught_up = run { $backend->read_history('t', 100, since => $c3) };
    is(scalar @caught_up, 0, 'caught-up cursor returns nothing');

    ok(!exists $backend->{history}{'t'}[0]{message}{_seq},
       'read_history does not mutate the stored message');
};

subtest 'live publish carries _seq; subscribe_with_history honours since' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new(history_size => 10);

    # Live delivery is _seq-tagged
    run { $backend->subscribe('chan', 't') };
    run { $backend->publish('t', { type => 'live', n => 1 }) };
    my $live = run { $backend->poll('chan') };
    is($live->{n}, 1, 'live message delivered');
    ok($live->{_seq}, 'live delivery carries _seq');

    # Record two more, then resume strictly after the first of them
    my $c1 = run { $backend->_record_history('t', { type => 'msg', n => 2 }) };
    my $c2 = run { $backend->_record_history('t', { type => 'msg', n => 3 }) };
    run { $backend->subscribe_with_history('chan2', 't', 100, since => $c1) };
    my $r1 = run { $backend->poll('chan2') };
    is($r1->{n}, 3, 'replays strictly after the since-cursor');
    is($r1->{_seq}, $c2, 'replayed message carries its cursor');
    is(run { $backend->poll('chan2') }, undef, 'nothing else to replay');

    # Round-trip: the _seq a live subscriber saw is itself a valid `since`
    # cursor (this is exactly how a reconnecting client resumes).
    run { $backend->subscribe_with_history('chan3', 't', 100, since => $live->{_seq}) };
    my @from_live;
    while (defined(my $m = run { $backend->poll('chan3') })) { push @from_live, $m }
    is([map { $_->{n} } @from_live], [2, 3],
       'resuming from a live-delivered _seq replays exactly the messages after it');
};

done_testing;
