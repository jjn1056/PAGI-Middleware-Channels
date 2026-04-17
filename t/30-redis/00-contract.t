# t/30-redis/00-contract.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop skip_without_redis make_redis);
use Test::PAGI::Channels::Contract qw(run_contract_tests);
use Test2::V0;

init_loop();

SKIP: {
    skip_without_redis();

    require PAGI::Middleware::Channels::Backend::Redis;

    my $counter = 0;
    run_contract_tests('Redis', sub {
        my (%args) = @_;
        # Use a per-instance prefix so capacity-overriding tests with
        # different config get isolated keyspaces.
        $counter++;
        my $redis = make_redis(prefix => "test:contract:$$:$counter:");
        my $backend = PAGI::Middleware::Channels::Backend::Redis->new(
            redis => $redis,
            %args,
        );
        $backend->flush->get;
        return $backend;
    });
}

done_testing;
