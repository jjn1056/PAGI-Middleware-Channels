use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

ok(lives { require PAGI::Channels::Backend::Memory }, 'require Memory backend') or diag($@);

subtest 'send and poll' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    # Initially empty
    my $msg = run { $backend->poll('test.channel') };
    is($msg, undef, 'poll on empty channel returns undef');

    # Send message
    run { $backend->send('test.channel', { type => 'test', data => 1 }) };

    # Poll receives it
    $msg = run { $backend->poll('test.channel') };
    is($msg, { type => 'test', data => 1 }, 'poll returns sent message');

    # Now empty again
    $msg = run { $backend->poll('test.channel') };
    is($msg, undef, 'poll after consume returns undef');
};

subtest 'FIFO ordering' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new();

    run { $backend->send('ch', { type => 'msg', n => 1 }) };
    run { $backend->send('ch', { type => 'msg', n => 2 }) };
    run { $backend->send('ch', { type => 'msg', n => 3 }) };

    is(run { $backend->poll('ch') }->{n}, 1, 'first message');
    is(run { $backend->poll('ch') }->{n}, 2, 'second message');
    is(run { $backend->poll('ch') }->{n}, 3, 'third message');
    is(run { $backend->poll('ch') }, undef, 'queue empty');
};

subtest 'capacity limit' => sub {
    my $backend = PAGI::Channels::Backend::Memory->new(capacity => 3);

    run { $backend->send('ch', { type => 'msg', n => 1 }) };
    run { $backend->send('ch', { type => 'msg', n => 2 }) };
    run { $backend->send('ch', { type => 'msg', n => 3 }) };

    # Fourth should fail
    my $result = run {
        $backend->send('ch', { type => 'msg', n => 4 })->catch(sub {
            my ($cat) = @_;
            return { error => $cat };
        });
    };
    is($result->{error}, 'ChannelFull', 'send to full channel fails');
};

done_testing;
