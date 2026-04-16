# t/10-memory/00-contract.t
use strict;
use warnings;
use Test::Lib;
use Test::PAGI::Channels qw(init_loop);
use Test::PAGI::Channels::Contract qw(run_contract_tests);
use PAGI::Middleware::Channels::Backend::Memory;
use Test2::V0;

init_loop();

run_contract_tests('Memory', sub {
    my (%args) = @_;
    return PAGI::Middleware::Channels::Backend::Memory->new(%args);
});

done_testing;
