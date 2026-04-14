# cpanfile - PAGI-Channels dependencies

# Core
requires 'perl', '5.018';
requires 'Future::AsyncAwait', '0.66';
requires 'Future::IO', '0.15';
requires 'Role::Tiny', '2.002004';
requires 'JSON::MaybeXS', '1.004005';
requires 'namespace::clean';

# Optional Redis backend
recommends 'Async::Redis', '0.001003';  # Latest from CPAN
recommends 'Sereal::Encoder', '5.004';
recommends 'Sereal::Decoder', '5.004';

# Testing
on 'test' => sub {
    requires 'Test2::V0';
    requires 'Test::Lib';
    requires 'IO::Async', '0.802';
    requires 'Future::IO::Impl::IOAsync', '0.805';
    requires 'Devel::Cover';
};
