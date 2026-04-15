use Test2::V0;
use Test::Lib;
use PAGI::Middleware::Channels::Backend;

# A bare subclass that inherits but implements nothing. Every abstract
# method on the base class must croak when called — this pins the
# "abstract" contract we are about to introduce.
package TestBackend::Bare {
    use parent -norequire, 'PAGI::Middleware::Channels::Backend';
    sub new { bless {}, shift }
}

my $b = TestBackend::Bare->new;

my @methods = qw(
    send poll subscribe unsubscribe publish flush cleanup
    psubscribe punsubscribe track untrack list_presence
    send_delayed publish_delayed subscribe_with_history
);

for my $m (@methods) {
    like
        dies { $b->$m() },
        qr/abstract method/i,
        "$m croaks as abstract on bare subclass";
}

# Direct instantiation of the abstract base must also croak
like
    dies { PAGI::Middleware::Channels::Backend->new() },
    qr/abstract/i,
    "cannot instantiate abstract base class directly";

done_testing;
