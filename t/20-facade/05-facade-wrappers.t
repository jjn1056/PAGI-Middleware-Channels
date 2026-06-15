use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;
use Future::AsyncAwait;

init_loop();

use PAGI::Channel;
use PAGI::Middleware::Channels::Backend::Memory;

subtest 'unsubscribe emits presence.leave when subscriber was tracked' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new;
    my $alice = PAGI::Channel->new(backend => $backend, channel_name => 'alice.conn');
    my $bob   = PAGI::Channel->new(backend => $backend, channel_name => 'bob.conn');

    # bob subscribes first so he can observe events
    run { $bob->subscribe('room') };

    # alice subscribes with presence
    run { $alice->subscribe('room', presence => { user => 'alice' }) };

    # Drain bob's join event
    while (run { $backend->poll('bob.conn') }) {}

    # alice unsubscribes
    run { $alice->unsubscribe('room') };

    # bob should have received a presence.leave event
    my $event = run { $backend->poll('bob.conn') };
    is($event->{type}, 'presence.leave', 'presence.leave emitted on unsubscribe');
    is($event->{presence}{user}, 'alice', 'leave event carries alice data');
};

subtest 'unsubscribe without presence does not emit leave' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new;
    my $alice = PAGI::Channel->new(backend => $backend, channel_name => 'alice.conn');
    my $bob   = PAGI::Channel->new(backend => $backend, channel_name => 'bob.conn');

    run { $bob->subscribe('room') };
    run { $alice->subscribe('room') };  # no presence
    run { $alice->unsubscribe('room') };

    is(run { $backend->poll('bob.conn') }, undef, 'bob got nothing');
};

subtest 'capability croak — facade rejects unsupported ops' => sub {
    # A minimal fake backend with no roles.
    package T::NullBackend {
        sub new { bless {}, shift }
        sub does { 0 }
    }

    my $ch = PAGI::Channel->new(
        backend      => T::NullBackend->new,
        channel_name => 'x',
    );

    like(
        dies { run { $ch->psubscribe('chat.*') } },
        qr/PatternSubs capability/,
        'psubscribe croaks without PatternSubs role'
    );
    like(
        dies { run { $ch->track('t', { x => 1 }) } },
        qr/Presence capability/,
        'track croaks without Presence role'
    );
    like(
        dies { run { $ch->send('ch', { type => 'x' }, delay => 1) } },
        qr/Delayed capability/,
        'send with delay croaks without Delayed role'
    );
    like(
        dies { run { $ch->subscribe('t', history => 5) } },
        qr/History capability/,
        'subscribe with history croaks without History role'
    );
    like(
        dies { run { $ch->subscribe('t', since => 1) } },
        qr/History capability/,
        'subscribe with since croaks without History role'
    );
};

subtest 'Django compat aliases' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new;
    my $ch = PAGI::Channel->new(backend => $backend, channel_name => 'group.add.conn');

    # group_add == subscribe
    run { $ch->group_add('room') };
    run { $backend->publish('room', { type => 'x' }) };
    ok(run { $backend->poll('group.add.conn') }, 'group_add subscribes');

    # group_send == publish
    run { $ch->group_send('room', { type => 'sent-via-alias' }) };
    my $msg = run { $backend->poll('group.add.conn') };
    is($msg->{type}, 'sent-via-alias', 'group_send publishes');

    # group_discard == unsubscribe
    run { $ch->group_discard('room') };
    run { $backend->publish('room', { type => 'after-discard' }) };
    is(run { $backend->poll('group.add.conn') }, undef, 'group_discard unsubscribes');
};

subtest 'subscribe(history => N) delegates via facade' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new(history_size => 10);
    my $ch = PAGI::Channel->new(backend => $backend, channel_name => 'hist.conn');

    run { $backend->publish('room', { type => 'msg', n => $_ }) } for 1..3;

    run { $ch->subscribe('room', history => 10) };

    is(run { $backend->poll('hist.conn') }->{n}, 1, 'history msg 1 via facade');
    is(run { $backend->poll('hist.conn') }->{n}, 2, 'history msg 2 via facade');
    is(run { $backend->poll('hist.conn') }->{n}, 3, 'history msg 3 via facade');
};

subtest 'send(delay => N) delegates to send_delayed' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new;
    my $ch = PAGI::Channel->new(backend => $backend, channel_name => 'delay.conn');

    run { $ch->send('delay.conn', { type => 'd' }, delay => 0.05) };
    is(run { $backend->poll('delay.conn') }, undef, 'not delivered immediately');

    run { Future::IO->sleep(0.1) };
    run { $backend->process_delayed() };

    my $msg = run { $backend->poll('delay.conn') };
    is($msg->{type}, 'd', 'delivered after delay via facade');
};

subtest 'send(delay => 0) is immediate, not delayed' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new;
    my $ch = PAGI::Channel->new(backend => $backend, channel_name => 'immediate.conn');

    run { $ch->send('immediate.conn', { type => 'now' }, delay => 0) };

    # Immediate => available on poll without process_delayed.
    my $msg = run { $backend->poll('immediate.conn') };
    is($msg->{type}, 'now', 'delay => 0 is treated as immediate send');
};

subtest 'publish(delay => N) delegates to publish_delayed' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new;
    my $ch = PAGI::Channel->new(backend => $backend, channel_name => 'pub.conn');

    run { $backend->subscribe('sub1', 'room') };
    run { $ch->publish('room', { type => 'broadcast' }, delay => 0.05) };

    is(run { $backend->poll('sub1') }, undef, 'not delivered immediately');

    run { Future::IO->sleep(0.1) };
    run { $backend->process_delayed() };

    my $msg = run { $backend->poll('sub1') };
    is($msg->{type}, 'broadcast', 'delivered after delay via facade');
};

subtest 'facade subscribe passes since-cursor through to the backend' => sub {
    my $backend = PAGI::Middleware::Channels::Backend::Memory->new(history_size => 10);
    my $ch = PAGI::Channel->new(backend => $backend, channel_name => 'c1');
    my $c1 = run { $backend->_record_history('room', { type => 'msg', n => 1 }) };
    run { $backend->_record_history('room', { type => 'msg', n => 2 }) };

    run { $ch->subscribe('room', since => $c1) };
    my $msg = run { $backend->poll('c1') };
    is($msg->{n}, 2, 'only messages after the cursor are replayed via the facade');
    ok($msg->{_seq} > $c1, 'replayed message carries an advanced _seq');
    is(run { $backend->poll('c1') }, undef, 'nothing before/at the cursor replayed');
};

done_testing;
