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

done_testing;
