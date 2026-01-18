package Test::PAGI::Channels;
use strict;
use warnings;
use parent 'Exporter';
use Test2::V0;
use Future::IO;

our @EXPORT_OK = qw(
    init_loop
    get_loop
    run
    skip_without_redis
    redis_host
    redis_port
);
our %EXPORT_TAGS = (all => \@EXPORT_OK);

my $loop;

sub init_loop {
    require IO::Async::Loop;
    require Future::IO::Impl::IOAsync;
    $loop = IO::Async::Loop->new;
    # Future::IO::Impl::IOAsync auto-configures on load,
    # but we need to ensure the loop instance is shared
    Future::IO::Impl::IOAsync->APPLY($loop);
    return $loop;
}

sub get_loop { $loop }

sub run(&) {
    my ($code) = @_;
    my $f = $code->();
    return $f->get if $f && $f->isa('Future');
    return $f;
}

sub redis_host { $ENV{REDIS_HOST} // 'localhost' }
sub redis_port { $ENV{REDIS_PORT} // 6379 }

sub skip_without_redis {
    my $host = redis_host();
    my $port = redis_port();

    require IO::Socket::INET;
    my $sock = IO::Socket::INET->new(
        PeerHost => $host,
        PeerPort => $port,
        Timeout  => 2,
    );

    unless ($sock) {
        skip_all("Redis not available at $host:$port");
        return;
    }
    close $sock;
    return 1;
}

1;
