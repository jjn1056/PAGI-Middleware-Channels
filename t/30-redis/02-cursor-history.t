use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run skip_without_redis make_redis);
use Test2::V0;

init_loop();

SKIP: {
    skip_without_redis();

    require PAGI::Middleware::Channels::Backend::Redis;

    my $redis = make_redis(prefix => "test:cursor:$$:");
    my $b = PAGI::Middleware::Channels::Backend::Redis->new(
        redis        => $redis,
        history_size => 10,
    );
    run { $b->flush };

    my $c1 = run { $b->_record_history('room', { type => 'msg', n => 1 }) };
    my $c2 = run { $b->_record_history('room', { type => 'msg', n => 2 }) };
    ok($c1, 'record returns a stream-id cursor');
    ok($c2 ne $c1, 'cursors differ and advance');

    my @after = run { $b->read_history('room', 100, since => $c1) };
    is([map { $_->{n} } @after], [2], 'since replays strictly after the cursor');
    is($after[0]{_seq}, $c2, 'replayed message carries its _seq cursor');

    my @caught_up = run { $b->read_history('room', 100, since => $c2) };
    is(scalar @caught_up, 0, 'caught-up returns nothing');

    is(run { $b->_record_history('room', { type => 'presence.join' }) }, undef,
       'presence events are not recorded');

    # 'room' has entries, but a no-since read with an undef count must be a
    # safe empty list, not a cryptic 'COUNT undef' Redis error.
    is([run { $b->read_history('room', undef) }], [],
       'no-since read with undef count returns empty (no Redis error)');
}

done_testing;
