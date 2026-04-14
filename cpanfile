# cpanfile - PAGI-Channels dependencies

# Core
requires 'perl', '5.018';
requires 'Future::AsyncAwait', '0.66';
requires 'Future::IO', '0.15';
requires 'Role::Tiny', '2.002004';
requires 'JSON::MaybeXS', '1.004005';
requires 'namespace::clean';

# Optional serializer for higher throughput (not yet wired)
recommends 'Sereal::Encoder', '5.004';
recommends 'Sereal::Decoder', '5.004';

# Testing
on 'test' => sub {
    requires 'Test2::V0';
    requires 'Test::Lib';
    requires 'IO::Async', '0.802';
    requires 'Future::IO::Impl::IOAsync', '0.805';
    # Async::Redis is used by the Redis-backend test files to construct
    # an instance to pass into the backend. PAGI::Channels itself has
    # no runtime dependency on Async::Redis — users bring their own
    # client at call-site.
    recommends 'Async::Redis', '0.001005';
    requires 'Devel::Cover';
};
