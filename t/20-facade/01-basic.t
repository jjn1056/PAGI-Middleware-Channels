use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop run);
use Test2::V0;

my $loop = init_loop();

use PAGI::Middleware::Channels;
use PAGI::Middleware::Channels::Backend::Memory;

subtest 'new() requires a backend instance' => sub {
    like(
        dies { PAGI::Middleware::Channels->new() },
        qr/backend/,
        'dies without backend arg',
    );
};

subtest 'basic send/subscribe/publish flow' => sub {
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    run { $channels->backend->subscribe('ch1', 'room') };
    run { $channels->backend->subscribe('ch2', 'room') };

    run { $channels->backend->publish('room', { type => 'msg', text => 'hello' }) };

    my $msg1 = run { $channels->backend->poll('ch1') };
    my $msg2 = run { $channels->backend->poll('ch2') };

    is($msg1->{text}, 'hello', 'ch1 received');
    is($msg2->{text}, 'hello', 'ch2 received');
};

subtest 'backend accessor returns the passed instance' => sub {
    my $memory = PAGI::Middleware::Channels::Backend::Memory->new;
    my $channels = PAGI::Middleware::Channels->new(backend => $memory);
    is($channels->backend, $memory, 'backend accessor returns exact instance');
};

done_testing;
