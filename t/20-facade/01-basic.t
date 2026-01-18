use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Channels;

subtest 'basic send/subscribe/publish flow' => sub {
    my $channels = PAGI::Channels->new();

    # Simulate two connections
    my $scope1 = {};
    my $scope2 = {};

    # Wrap creates channel names and injects scope keys
    # For now test backend directly via facade methods

    run { $channels->backend->subscribe('ch1', 'room') };
    run { $channels->backend->subscribe('ch2', 'room') };

    run { $channels->backend->publish('room', { type => 'msg', text => 'hello' }) };

    my $msg1 = $channels->backend->poll('ch1');
    my $msg2 = $channels->backend->poll('ch2');

    is($msg1->{text}, 'hello', 'ch1 received');
    is($msg2->{text}, 'hello', 'ch2 received');
};

subtest 'default memory backend' => sub {
    my $channels = PAGI::Channels->new();
    isa_ok($channels->backend, 'PAGI::Channels::Backend::Memory');
};

subtest 'env var backend selection' => sub {
    local $ENV{PAGI_CHANNELS_BACKEND} = 'memory://';
    my $channels = PAGI::Channels->new();
    isa_ok($channels->backend, 'PAGI::Channels::Backend::Memory');
};

done_testing;
