use Test2::V0;
use Test::Lib;
use PAGI::Middleware::Channels::Backend;

# A bare subclass that inherits from the base. Every CORE abstract method
# must croak when called on a subclass that doesn't override.
package TestBackend::Bare {
    use parent -norequire, 'PAGI::Middleware::Channels::Backend';
    # No methods overridden.
}

my $b = TestBackend::Bare->new;

# CORE 8 methods: must croak as abstract on a bare subclass
my @core = qw(
    send poll next_message
    subscribe unsubscribe publish
    flush cleanup
);

for my $m (@core) {
    like
        dies { $b->$m() },
        qr/abstract method/i,
        "core method '$m' croaks as abstract on bare subclass";
}

# Direct instantiation of the abstract base must also croak
like
    dies { PAGI::Middleware::Channels::Backend->new() },
    qr/abstract/i,
    "cannot instantiate abstract base class directly";

# Capability methods (presence/history/delayed/patternsubs) are NOT in the
# core contract. A bare subclass shouldn't be expected to provide them.
ok(!$b->can('track'), 'bare subclass has no track method (Presence-only)');
ok(!$b->can('read_history'), 'bare subclass has no read_history (History-only)');
ok(!$b->can('schedule_delayed'), 'bare subclass has no schedule_delayed (Delayed-only)');
ok(!$b->can('psubscribe'), 'bare subclass has no psubscribe (PatternSubs-only)');

# Memory backend should declare all four capabilities.
require PAGI::Middleware::Channels::Backend::Memory;
my $m = PAGI::Middleware::Channels::Backend::Memory->new;
ok($m->does('PAGI::Middleware::Channels::Backend::Role::Presence'),
   'Memory does Presence');
ok($m->does('PAGI::Middleware::Channels::Backend::Role::History'),
   'Memory does History');
ok($m->does('PAGI::Middleware::Channels::Backend::Role::Delayed'),
   'Memory does Delayed');
ok($m->does('PAGI::Middleware::Channels::Backend::Role::PatternSubs'),
   'Memory does PatternSubs');

done_testing;
