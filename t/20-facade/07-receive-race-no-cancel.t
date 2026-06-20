# t/20-facade/07-receive-race-no-cancel.t
#
# Regression: when a channel message wins the race against a pending protocol
# receive, the wrapper must NOT cancel the protocol future. Cancelling the
# $receive future ends an HTTP/SSE stream -- on the reference server the next
# $receive->() then yields a cancelled future and the app dies "after response
# started". A mock receive cannot reproduce that downstream breakage, so we
# assert the root-cause invariant directly: the protocol future stays uncancelled.
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Middleware::Channels;
use PAGI::Middleware::Channels::Backend::Memory;
use Future::AsyncAwait;
use PAGI::Channel;
use Future;

subtest 'channel message winning the race does not cancel the protocol receive' => sub {
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    my @protocol_futures;            # every future handed out by the underlying receive
    my $receive = sub {
        my $f = Future->new;         # pending: no protocol event arrives in this test
        push @protocol_futures, $f;
        return $f;
    };
    my $send = async sub { };

    my $got;
    my $inner_app = async sub {
        my ($scope, $rcv, $snd) = @_;
        my $ch = PAGI::Channel->from($scope);
        my $cn = $ch->channel_name;

        # Deliver a channel message on the next loop tick -- after we suspend on
        # the wrapped receive, so the message wins the race (not the fast path).
        $loop->later(sub { $ch->backend->send($cn, { type => 'chat.msg' })->retain });

        $got = await $rcv->();
    };

    my $wrapped = $channels->wrap($inner_app);
    my $app_f = $wrapped->({ type => 'http' }, $receive, $send);
    $loop->await($app_f);   # drive the loop until the app finishes (it suspends on receive)
    $app_f->get;            # surface any exception

    is($got->{type}, 'chat.msg', 'channel message was delivered');
    is(scalar(@protocol_futures), 1, 'exactly one protocol receive was opened');
    ok(!$protocol_futures[0]->is_cancelled,
        'the pending protocol future was NOT cancelled (the stream stays alive)');
};

done_testing;
