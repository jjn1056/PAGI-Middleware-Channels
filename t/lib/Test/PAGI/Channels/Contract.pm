package Test::PAGI::Channels::Contract;
use strict;
use warnings;
use parent 'Exporter';
use Test2::V0;
use Future::AsyncAwait;
use Future;
use Future::IO;
use Time::HiRes ();

our @EXPORT_OK = qw(run_contract_tests);

sub _run(&);

# run_contract_tests($label, $factory, %opts)
#
# $label   - human-readable backend name, used in subtest names
# $factory - coderef returning a fresh, flushed backend on each call.
#            Accepts optional %args that are passed to the backend
#            constructor (e.g., $factory->(capacity => 3, history_size => 10))
# %opts    - reserved for future per-backend skips
#
# The factory MUST return a backend that has been flushed of any state
# left from prior runs. Memory: just `->new`. Redis: `->new` + `->flush->get`.
sub run_contract_tests {
    my ($label, $factory, %opts) = @_;

    subtest "$label - core (send/poll/FIFO/capacity)" => sub {
        _test_core_send_poll($factory);
        _test_core_fifo($factory);
        _test_core_capacity($factory);
    };

    subtest "$label - core (pubsub)" => sub {
        _test_pubsub_basic($factory);
        _test_pubsub_exclude($factory);
        _test_pubsub_full_drops($factory);
        _test_pubsub_unsubscribe($factory);
        _test_pubsub_idempotent($factory);
    };

    subtest "$label - core (next_message)" => sub {
        _test_next_message($factory);
    };

    subtest "$label - core (cleanup/flush)" => sub {
        _test_cleanup($factory);
        _test_flush($factory);
    };

    subtest "$label - core (validation)" => sub {
        _test_validation($factory);
    };

    # Capability subtests follow in subsequent tasks.
    # In Phase 1 we run them unconditionally because Memory and Redis
    # both implement everything. In Phase 3 we add `->does(...)` gating.
    subtest "$label - presence" => sub {
        _test_presence($factory);
    };

    subtest "$label - history" => sub {
        _test_history($factory);
    };

    subtest "$label - delayed" => sub {
        _test_delayed($factory);
    };

    subtest "$label - pattern subs" => sub {
        _test_pattern_subs($factory);
    };
}

sub _test_core_send_poll {
    my ($factory) = @_;
    my $backend = $factory->();

    my $msg = _run { $backend->poll('test.channel') };
    is($msg, undef, 'poll on empty channel returns undef');

    _run { $backend->send('test.channel', { type => 'test', data => 1 }) };

    $msg = _run { $backend->poll('test.channel') };
    is($msg, { type => 'test', data => 1 }, 'poll returns sent message');

    $msg = _run { $backend->poll('test.channel') };
    is($msg, undef, 'poll after consume returns undef');
}

sub _test_core_fifo {
    my ($factory) = @_;
    my $backend = $factory->();

    _run { $backend->send('ch', { type => 'msg', n => 1 }) };
    _run { $backend->send('ch', { type => 'msg', n => 2 }) };
    _run { $backend->send('ch', { type => 'msg', n => 3 }) };

    is(_run { $backend->poll('ch') }->{n}, 1, 'first message');
    is(_run { $backend->poll('ch') }->{n}, 2, 'second message');
    is(_run { $backend->poll('ch') }->{n}, 3, 'third message');
    is(_run { $backend->poll('ch') }, undef, 'queue empty');
}

sub _test_core_capacity {
    my ($factory) = @_;
    my $backend = $factory->(capacity => 3);

    _run { $backend->send('ch', { type => 'msg', n => 1 }) };
    _run { $backend->send('ch', { type => 'msg', n => 2 }) };
    _run { $backend->send('ch', { type => 'msg', n => 3 }) };

    my $result = _run {
        $backend->send('ch', { type => 'msg', n => 4 })->catch(sub {
            my ($cat) = @_;
            return { error => $cat };
        });
    };
    is($result->{error}, 'ChannelFull', 'send to full channel fails');
}

sub _test_pubsub_basic {
    my ($factory) = @_;
    my $backend = $factory->();

    # Subscribe two channels to a topic
    _run { $backend->subscribe('ch1', 'room.general') };
    _run { $backend->subscribe('ch2', 'room.general') };

    # Publish to topic
    _run { $backend->publish('room.general', { type => 'chat', text => 'hello' }) };

    # Both receive
    my $msg1 = _run { $backend->poll('ch1') };
    my $msg2 = _run { $backend->poll('ch2') };

    is($msg1->{text}, 'hello', 'ch1 received');
    is($msg2->{text}, 'hello', 'ch2 received');
}

sub _test_pubsub_exclude {
    my ($factory) = @_;
    my $backend = $factory->();

    _run { $backend->subscribe('ch1', 'room') };
    _run { $backend->subscribe('ch2', 'room') };
    _run { $backend->subscribe('ch3', 'room') };

    # Publish excluding ch2
    _run { $backend->publish('room', { type => 'msg' }, exclude => 'ch2') };

    ok(_run { $backend->poll('ch1') }, 'ch1 received');
    is(_run { $backend->poll('ch2') }, undef, 'ch2 excluded');
    ok(_run { $backend->poll('ch3') }, 'ch3 received');
}

sub _test_pubsub_full_drops {
    my ($factory) = @_;
    my $backend = $factory->(capacity => 1);

    _run { $backend->subscribe('ch1', 'room') };

    # Fill ch1
    _run { $backend->send('ch1', { type => 'fill' }) };

    # Publish should not die even though ch1 is full
    my $ok = _run { $backend->publish('room', { type => 'dropped' }) };
    is($ok, 1, 'publish succeeds even with full subscriber');

    # ch1 still only has original message
    is(_run { $backend->poll('ch1') }->{type}, 'fill', 'original message');
    is(_run { $backend->poll('ch1') }, undef, 'broadcast was dropped');
}

sub _test_pubsub_unsubscribe {
    my ($factory) = @_;
    my $backend = $factory->();

    _run { $backend->subscribe('ch1', 'room') };
    _run { $backend->unsubscribe('ch1', 'room') };
    _run { $backend->publish('room', { type => 'msg' }) };

    is(_run { $backend->poll('ch1') }, undef, 'unsubscribed channel does not receive');
}

sub _test_pubsub_idempotent {
    my ($factory) = @_;
    my $backend = $factory->();

    _run { $backend->subscribe('ch1', 'room') };
    _run { $backend->subscribe('ch1', 'room') };  # duplicate
    _run { $backend->publish('room', { type => 'msg' }) };

    ok(_run { $backend->poll('ch1') }, 'received once');
    is(_run { $backend->poll('ch1') }, undef, 'no duplicate');
}

sub _test_next_message {
    my ($factory) = @_;

    subtest 'next_message returns queued message immediately' => sub {
        my $backend = $factory->();
        _run { $backend->send('ch1', { type => 'test', text => 'hello' }) };

        my $msg = _run { $backend->next_message('ch1') };
        is($msg->{type}, 'test', 'got queued message');
        is($msg->{text}, 'hello', 'correct payload');
    };

    subtest 'next_message waits and resolves when send delivers' => sub {
        my $backend = $factory->();

        _run {
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
        my $backend = $factory->();

        # Subscribe ch1 to a topic so publish reaches it
        _run { $backend->subscribe('ch1', 'room') };

        _run {
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
        my $backend = $factory->();

        my $f = $backend->next_message('ch1');
        ok(!$f->is_ready, 'future is pending');

        $f->cancel;
        ok($f->is_cancelled, 'future cancelled');
    };

    subtest 'cleanup cancels waiters for channel' => sub {
        my $backend = $factory->();

        my $f = $backend->next_message('ch1');
        ok(!$f->is_ready, 'waiter pending before cleanup');

        _run { $backend->cleanup('ch1') };
        ok($f->is_cancelled, 'waiter cancelled by cleanup');
    };

    subtest 'flush cancels all waiters' => sub {
        my $backend = $factory->();

        my $f1 = $backend->next_message('ch1');
        my $f2 = $backend->next_message('ch2');

        _run { $backend->flush() };
        ok($f1->is_cancelled, 'ch1 waiter cancelled');
        ok($f2->is_cancelled, 'ch2 waiter cancelled');
    };

    subtest 'next_message works after prior cancel' => sub {
        my $backend = $factory->();

        # Cancel a pending next_message
        my $f = $backend->next_message('ch1');
        $f->cancel;

        # Now send and retrieve normally
        _run { $backend->send('ch1', { type => 'after-cancel' }) };
        my $msg = _run { $backend->next_message('ch1') };
        is($msg->{type}, 'after-cancel', 'next_message works after prior cancel');
    };
}

sub _test_cleanup {
    my ($factory) = @_;

    subtest 'cleanup removes channel from all groups' => sub {
        my $backend = $factory->();

        _run { $backend->subscribe('ch1', 'room1') };
        _run { $backend->subscribe('ch1', 'room2') };
        _run { $backend->subscribe('ch1', 'room3') };

        _run { $backend->cleanup('ch1') };

        # Publish to all rooms - ch1 should not receive
        _run { $backend->publish('room1', { type => 'msg' }) };
        _run { $backend->publish('room2', { type => 'msg' }) };
        _run { $backend->publish('room3', { type => 'msg' }) };

        is(_run { $backend->poll('ch1') }, undef, 'ch1 removed from all groups');
    };

    subtest 'cleanup removes pending messages' => sub {
        my $backend = $factory->();

        _run { $backend->send('ch1', { type => 'msg1' }) };
        _run { $backend->send('ch1', { type => 'msg2' }) };

        _run { $backend->cleanup('ch1') };

        is(_run { $backend->poll('ch1') }, undef, 'queue cleared');
    };

    subtest 'cleanup removes pattern subscriptions' => sub {
        my $backend = $factory->();

        _run { $backend->psubscribe('ch1', 'events.*') };
        _run { $backend->cleanup('ch1') };
        _run { $backend->publish('events.click', { type => 'msg' }) };

        is(_run { $backend->poll('ch1') }, undef, 'pattern subscription removed');
    };
}

sub _test_flush {
    my ($factory) = @_;
    my $backend = $factory->(history_size => 10);

    _run { $backend->subscribe('ch1', 'room') };
    _run { $backend->psubscribe('ch2', 'events.*') };
    _run { $backend->send('ch1', { type => 'msg' }) };
    _run { $backend->publish('room', { type => 'msg' }) };

    _run { $backend->flush() };

    # Observable behavior after flush: queues empty, subscriptions gone
    is(_run { $backend->poll('ch1') }, undef, 'queues cleared');
    _run { $backend->publish('room', { type => 'after-flush' }) };
    is(_run { $backend->poll('ch1') }, undef, 'subscription cleared');
    _run { $backend->publish('events.test', { type => 'after-flush' }) };
    is(_run { $backend->poll('ch2') }, undef, 'pattern subscription cleared');
}

sub _test_validation {
    my ($factory) = @_;
    my $backend = $factory->();

    # Channel name validation
    like(
        dies { _run { $backend->send('', { type => 'x' }) } },
        qr/InvalidChannelName/,
        'empty channel name rejected'
    );
    like(
        dies { _run { $backend->send('a' x 101, { type => 'x' }) } },
        qr/InvalidChannelName/,
        'over-length channel name rejected'
    );
    like(
        dies { _run { $backend->send('bad name with spaces', { type => 'x' }) } },
        qr/InvalidChannelName/,
        'channel name with disallowed chars rejected'
    );

    # Message validation
    like(
        dies { _run { $backend->send('ch', 'not-a-hashref') } },
        qr/InvalidMessage/,
        'non-hashref message rejected'
    );
    like(
        dies { _run { $backend->send('ch', { no_type => 1 }) } },
        qr/InvalidMessage/,
        'message missing type rejected'
    );

    # Topic validation (subscribe and publish use the same rules)
    like(
        dies { _run { $backend->subscribe('ch', '') } },
        qr/InvalidChannelName/,
        'subscribe rejects empty topic'
    );
    like(
        dies { _run { $backend->publish('', { type => 'x' }) } },
        qr/InvalidChannelName/,
        'publish rejects empty topic'
    );
}

sub _test_presence {
    my ($factory) = @_;

    subtest 'explicit track/untrack' => sub {
        my $backend = $factory->();
        $backend->set_channel_id('worker.1');

        _run { $backend->track('workers.pool', { worker_id => 1, started => 1000 }) };

        my @presence = _run { $backend->list_presence('workers.pool') };
        is(scalar @presence, 1, 'one presence entry');
        is($presence[0]->{worker_id}, 1, 'correct data');

        _run { $backend->untrack('workers.pool') };

        @presence = _run { $backend->list_presence('workers.pool') };
        is(scalar @presence, 0, 'presence removed');
    };

    subtest 'subscribe with presence option' => sub {
        my $backend = $factory->();
        $backend->set_channel_id('user.alice');

        _run { $backend->subscribe('user.alice', 'chat.room1',
            presence => { user => 'alice', status => 'online' }
        )};

        my @presence = _run { $backend->list_presence('chat.room1') };
        is(scalar @presence, 1, 'presence tracked via subscribe');
        is($presence[0]->{user}, 'alice', 'correct user');
        is($presence[0]->{status}, 'online', 'correct status');
    };

    subtest 'presence events on join/leave' => sub {
        my $backend = $factory->();

        # Subscribe ch1 first (to receive events)
        $backend->set_channel_id('ch1');
        _run { $backend->subscribe('ch1', 'room', presence => { user => 'ch1' }) };

        # Now ch2 joins - ch1 should get presence.join event
        $backend->set_channel_id('ch2');
        _run { $backend->subscribe('ch2', 'room', presence => { user => 'ch2' }) };

        # Check ch1 received join event
        my $event = _run { $backend->poll('ch1') };
        is($event->{type}, 'presence.join', 'presence.join event');
        is($event->{presence}{user}, 'ch2', 'correct joiner');

        # ch2 leaves - ch1 should get presence.leave event
        _run { $backend->unsubscribe('ch2', 'room') };

        $event = _run { $backend->poll('ch1') };
        is($event->{type}, 'presence.leave', 'presence.leave event');
        is($event->{presence}{user}, 'ch2', 'correct leaver');
    };

    subtest 'list_presence returns all current' => sub {
        my $backend = $factory->();

        $backend->set_channel_id('u1');
        _run { $backend->subscribe('u1', 'room', presence => { name => 'Alice' }) };

        $backend->set_channel_id('u2');
        _run { $backend->subscribe('u2', 'room', presence => { name => 'Bob' }) };

        $backend->set_channel_id('u3');
        _run { $backend->subscribe('u3', 'room', presence => { name => 'Carol' }) };

        my @presence = _run { $backend->list_presence('room') };
        is(scalar @presence, 3, 'three users present');

        my @names = sort map { $_->{name} } @presence;
        is(\@names, ['Alice', 'Bob', 'Carol'], 'correct names');
    };

    subtest 'count_presence returns zero for unknown topic' => sub {
        my $backend = $factory->();
        my $count = _run { $backend->count_presence('no.such.topic') };
        is($count, 0, 'zero for unknown topic');
    };

    subtest 'count_presence returns number of non-expired entries' => sub {
        my $backend = $factory->();

        $backend->set_channel_id('u1');
        _run { $backend->subscribe('u1', 'room', presence => { name => 'Alice' }) };

        $backend->set_channel_id('u2');
        _run { $backend->subscribe('u2', 'room', presence => { name => 'Bob' }) };

        my $count = _run { $backend->count_presence('room') };
        is($count, 2, 'two users present');

        _run { $backend->unsubscribe('u2', 'room') };
        $count = _run { $backend->count_presence('room') };
        is($count, 1, 'one user after unsubscribe');
    };

    subtest 'list_presence with limit — under limit succeeds' => sub {
        my $backend = $factory->();

        $backend->set_channel_id('u1');
        _run { $backend->subscribe('u1', 'lim.room', presence => { n => 1 }) };
        $backend->set_channel_id('u2');
        _run { $backend->subscribe('u2', 'lim.room', presence => { n => 2 }) };

        my @presence = _run { $backend->list_presence('lim.room', limit => 5) };
        is(scalar @presence, 2, 'returns 2 entries when under limit of 5');
    };

    subtest 'list_presence with limit — over limit croaks' => sub {
        my $backend = $factory->();

        $backend->set_channel_id('u1');
        _run { $backend->subscribe('u1', 'big.room', presence => { n => 1 }) };
        $backend->set_channel_id('u2');
        _run { $backend->subscribe('u2', 'big.room', presence => { n => 2 }) };
        $backend->set_channel_id('u3');
        _run { $backend->subscribe('u3', 'big.room', presence => { n => 3 }) };

        my $err = dies {
            _run { $backend->list_presence('big.room', limit => 2) };
        };
        like($err, qr/exceeds limit/, 'croaks with helpful message');
        like($err, qr/scan_presence/, 'message mentions scan_presence');
    };

    subtest 'scan_presence with cursor 0 returns all entries for small set' => sub {
        my $backend = $factory->();

        for my $i (1..3) {
            $backend->set_channel_id("u$i");
            _run { $backend->subscribe("u$i", 'scan.room', presence => { n => $i }) };
        }

        my ($cursor, @entries) = _run { $backend->scan_presence('scan.room', cursor => 0, count => 10) };
        is($cursor, 0, 'cursor 0 means done');
        is(scalar @entries, 3, 'all 3 entries returned');
    };

    subtest 'scan_presence paginates correctly' => sub {
        my $backend = $factory->();

        for my $i (1..5) {
            $backend->set_channel_id("u$i");
            _run { $backend->subscribe("u$i", 'page.room', presence => { n => $i }) };
        }

        my @all;
        my $cursor = 0;
        do {
            my @batch;
            ($cursor, @batch) = _run { $backend->scan_presence('page.room', cursor => $cursor, count => 2) };
            push @all, @batch;
        } while ($cursor);

        is(scalar @all, 5, 'collected all 5 entries across pages');
    };

    subtest 'scan_presence on empty topic returns empty' => sub {
        my $backend = $factory->();
        my ($cursor, @entries) = _run { $backend->scan_presence('empty.room', cursor => 0, count => 10) };
        is($cursor, 0, 'cursor 0');
        is(scalar @entries, 0, 'no entries');
    };
}
sub _test_history {
    my ($factory) = @_;

    subtest 'subscribe_with_history receives last N messages' => sub {
        my $backend = $factory->(history_size => 10);

        # Publish some messages first (no subscribers yet)
        _run { $backend->publish('chat.room', { type => 'msg', n => 1 }) };
        _run { $backend->publish('chat.room', { type => 'msg', n => 2 }) };
        _run { $backend->publish('chat.room', { type => 'msg', n => 3 }) };

        # Now subscribe with history
        _run { $backend->subscribe_with_history('ch1', 'chat.room', 10) };

        # Should have received historical messages
        is(_run { $backend->poll('ch1') }->{n}, 1, 'history msg 1');
        is(_run { $backend->poll('ch1') }->{n}, 2, 'history msg 2');
        is(_run { $backend->poll('ch1') }->{n}, 3, 'history msg 3');
        is(_run { $backend->poll('ch1') }, undef, 'no more');
    };

    subtest 'history respects count limit' => sub {
        my $backend = $factory->(history_size => 100);

        for my $n (1..10) {
            _run { $backend->publish('room', { type => 'msg', n => $n }) };
        }

        # Request only last 3
        _run { $backend->subscribe_with_history('ch1', 'room', 3) };

        is(_run { $backend->poll('ch1') }->{n}, 8, 'only last 3: msg 8');
        is(_run { $backend->poll('ch1') }->{n}, 9, 'only last 3: msg 9');
        is(_run { $backend->poll('ch1') }->{n}, 10, 'only last 3: msg 10');
        is(_run { $backend->poll('ch1') }, undef, 'no more');
    };

    subtest 'history buffer respects global limit' => sub {
        my $backend = $factory->(history_size => 5);

        for my $n (1..10) {
            _run { $backend->publish('room', { type => 'msg', n => $n }) };
        }

        # Only last 5 are retained
        _run { $backend->subscribe_with_history('ch1', 'room', 100) };

        is(_run { $backend->poll('ch1') }->{n}, 6, 'buffer only has 6-10');
        is(_run { $backend->poll('ch1') }->{n}, 7, 'msg 7');
        is(_run { $backend->poll('ch1') }->{n}, 8, 'msg 8');
        is(_run { $backend->poll('ch1') }->{n}, 9, 'msg 9');
        is(_run { $backend->poll('ch1') }->{n}, 10, 'msg 10');
        is(_run { $backend->poll('ch1') }, undef, 'no more');
    };

    subtest 'new messages after subscribe arrive normally' => sub {
        my $backend = $factory->(history_size => 10);

        _run { $backend->publish('room', { type => 'history', n => 1 }) };
        _run { $backend->subscribe_with_history('ch1', 'room', 10) };

        # Consume history
        _run { $backend->poll('ch1') };

        # New message
        _run { $backend->publish('room', { type => 'live', n => 2 }) };

        my $msg = _run { $backend->poll('ch1') };
        is($msg->{type}, 'live', 'live message received');
        is($msg->{n}, 2, 'correct content');
    };
}
sub _test_delayed {
    my ($factory) = @_;

    subtest 'send_delayed delivers after delay' => sub {
        my $backend = $factory->();

        # Send with 0.1 second delay
        _run { $backend->send_delayed('ch1', { type => 'delayed' }, 0.1) };

        # Not delivered immediately
        is(_run { $backend->poll('ch1') }, undef, 'not delivered immediately');

        # Process delayed messages
        _run { $backend->process_delayed() };
        is(_run { $backend->poll('ch1') }, undef, 'still not delivered');

        # Wait and process again
        _run { Future::IO->sleep(0.15) };
        _run { $backend->process_delayed() };

        my $msg = _run { $backend->poll('ch1') };
        is($msg->{type}, 'delayed', 'delivered after delay');
    };

    subtest 'publish_delayed delivers to all subscribers after delay' => sub {
        my $backend = $factory->();

        _run { $backend->subscribe('ch1', 'room') };
        _run { $backend->subscribe('ch2', 'room') };

        _run { $backend->publish_delayed('room', { type => 'broadcast' }, 0.1) };

        # Not delivered immediately
        is(_run { $backend->poll('ch1') }, undef, 'ch1 not delivered yet');
        is(_run { $backend->poll('ch2') }, undef, 'ch2 not delivered yet');

        # Wait and process
        _run { Future::IO->sleep(0.15) };
        _run { $backend->process_delayed() };

        is(_run { $backend->poll('ch1') }->{type}, 'broadcast', 'ch1 received');
        is(_run { $backend->poll('ch2') }->{type}, 'broadcast', 'ch2 received');
    };

    subtest 'multiple delayed messages in order' => sub {
        my $backend = $factory->();

        _run { $backend->send_delayed('ch', { type => 'msg', n => 1 }, 0.05) };
        _run { $backend->send_delayed('ch', { type => 'msg', n => 2 }, 0.15) };
        _run { $backend->send_delayed('ch', { type => 'msg', n => 3 }, 0.10) };

        # Wait for all
        _run { Future::IO->sleep(0.2) };
        _run { $backend->process_delayed() };

        # Should arrive in delay order: 1, 3, 2
        is(_run { $backend->poll('ch') }->{n}, 1, 'first (0.05s)');
        is(_run { $backend->poll('ch') }->{n}, 3, 'second (0.10s)');
        is(_run { $backend->poll('ch') }->{n}, 2, 'third (0.15s)');
    };

    subtest 'poll() delivers due delayed messages without manual pump' => sub {
        my $backend = $factory->();

        _run { $backend->send_delayed('ch1', { type => 'reminder' }, 0.05) };

        # Nothing before the delay elapses
        is(_run { $backend->poll('ch1') }, undef, 'not delivered before delay');

        # Wait past the delay
        _run { Future::IO->sleep(0.1) };

        # poll() itself should pump the delayed queue — no manual process_delayed
        my $msg = _run { $backend->poll('ch1') };
        is($msg->{type}, 'reminder', 'delayed message pumped by poll()');
    };

    subtest 'cleanup removes delayed messages targeting the channel' => sub {
        my $backend = $factory->();

        _run { $backend->send_delayed('ch1', { type => 'delayed1' }, 0.1) };
        _run { $backend->send_delayed('ch2', { type => 'delayed2' }, 0.1) };
        _run { $backend->publish_delayed('topic1', { type => 'pub' }, 0.1) };

        # Subscribe ch3 to topic1 so we can observe the publish-delayed survives
        _run { $backend->subscribe('ch3', 'topic1') };

        _run { $backend->cleanup('ch1') };

        # Wait past delay, pump
        _run { Future::IO->sleep(0.2) };
        _run { $backend->process_delayed() };

        is(_run { $backend->poll('ch1') }, undef, 'ch1 delayed removed by cleanup');
        is(_run { $backend->poll('ch2') }->{type}, 'delayed2', 'ch2 delayed survives');
        is(_run { $backend->poll('ch3') }->{type}, 'pub', 'publish_delayed survives');
    };
}
sub _test_pattern_subs {
    my ($factory) = @_;

    subtest 'single-level wildcard (*)' => sub {
        my $backend = $factory->();

        # chat.* matches chat.room1, chat.general, NOT chat.room1.messages
        _run { $backend->psubscribe('ch1', 'chat.*') };

        _run { $backend->publish('chat.room1', { type => 'msg', n => 1 }) };
        _run { $backend->publish('chat.general', { type => 'msg', n => 2 }) };
        _run { $backend->publish('chat.room1.messages', { type => 'msg', n => 3 }) };
        _run { $backend->publish('notifications', { type => 'msg', n => 4 }) };

        is(_run { $backend->poll('ch1') }->{n}, 1, 'chat.room1 matched');
        is(_run { $backend->poll('ch1') }->{n}, 2, 'chat.general matched');
        is(_run { $backend->poll('ch1') }, undef, 'chat.room1.messages NOT matched');
    };

    subtest 'multi-level wildcard (**)' => sub {
        my $backend = $factory->();

        # notifications.** matches notifications, notifications.user, notifications.user.123
        _run { $backend->psubscribe('ch1', 'notifications.**') };

        _run { $backend->publish('notifications', { type => 'msg', n => 1 }) };
        _run { $backend->publish('notifications.user', { type => 'msg', n => 2 }) };
        _run { $backend->publish('notifications.user.123.email', { type => 'msg', n => 3 }) };
        _run { $backend->publish('alerts', { type => 'msg', n => 4 }) };

        is(_run { $backend->poll('ch1') }->{n}, 1, 'notifications matched');
        is(_run { $backend->poll('ch1') }->{n}, 2, 'notifications.user matched');
        is(_run { $backend->poll('ch1') }->{n}, 3, 'notifications.user.123.email matched');
        is(_run { $backend->poll('ch1') }, undef, 'alerts NOT matched');
    };

    subtest 'punsubscribe' => sub {
        my $backend = $factory->();

        _run { $backend->psubscribe('ch1', 'events.*') };
        _run { $backend->punsubscribe('ch1', 'events.*') };
        _run { $backend->publish('events.click', { type => 'msg' }) };

        is(_run { $backend->poll('ch1') }, undef, 'punsubscribed pattern no longer matches');
    };

    subtest 'mixed exact and pattern subscriptions' => sub {
        my $backend = $factory->();

        # Exact subscription
        _run { $backend->subscribe('ch1', 'room.vip') };
        # Pattern subscription
        _run { $backend->psubscribe('ch1', 'room.*') };

        _run { $backend->publish('room.vip', { type => 'msg' }) };

        # Should only receive once (dedup)
        ok(_run { $backend->poll('ch1') }, 'received message');
        is(_run { $backend->poll('ch1') }, undef, 'no duplicate from pattern');
    };
}

# Helper used by every subtest: synchronously run a Future-returning coderef.
sub _run(&) {
    my ($code) = @_;
    my $f = $code->();
    return $f->get if $f && ref($f) && $f->can('isa') && $f->isa('Future');
    return $f;
}

1;
