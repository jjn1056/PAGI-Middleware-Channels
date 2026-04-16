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

sub _test_presence        { ok(1, 'placeholder - Task 1.3') }
sub _test_history         { ok(1, 'placeholder - Task 1.4') }
sub _test_delayed         { ok(1, 'placeholder - Task 1.5') }
sub _test_pattern_subs    { ok(1, 'placeholder - Task 1.6') }

# Helper used by every subtest: synchronously run a Future-returning coderef.
sub _run(&) {
    my ($code) = @_;
    my $f = $code->();
    return $f->get if $f && ref($f) && $f->can('isa') && $f->isa('Future');
    return $f;
}

1;
