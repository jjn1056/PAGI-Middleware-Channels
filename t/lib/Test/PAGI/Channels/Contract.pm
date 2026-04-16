package Test::PAGI::Channels::Contract;
use strict;
use warnings;
use parent 'Exporter';
use Test2::V0;
use Future::AsyncAwait;
use Future;
use Future::IO;
use Time::HiRes ();

our @EXPORT_OK = qw(run_contract_tests);

# run_contract_tests($label, $factory, %opts)
#
# $label   - human-readable backend name, used in subtest names
# $factory - coderef returning a fresh, flushed backend on each call
# %opts    - reserved for future per-backend skips
#
# The factory MUST return a backend that has been flushed of any state
# left from prior runs. Memory: just `->new`. Redis: `->new` + `->flush->get`.
sub run_contract_tests {
    my ($label, $factory, %opts) = @_;

    subtest "$label - core (send/poll/FIFO/capacity)" => sub {
        _test_core_send_poll($factory);
        _test_core_fifo($factory);
        _test_core_capacity($factory);
    };

    subtest "$label - core (pubsub)" => sub {
        _test_pubsub_basic($factory);
        _test_pubsub_exclude($factory);
        _test_pubsub_full_drops($factory);
        _test_pubsub_unsubscribe($factory);
        _test_pubsub_idempotent($factory);
    };

    subtest "$label - core (next_message)" => sub {
        _test_next_message($factory);
    };

    subtest "$label - core (cleanup/flush)" => sub {
        _test_cleanup($factory);
        _test_flush($factory);
    };

    subtest "$label - core (validation)" => sub {
        _test_validation($factory);
    };

    # Capability subtests follow in subsequent tasks.
    # In Phase 1 we run them unconditionally because Memory and Redis
    # both implement everything. In Phase 3 we add `->does(...)` gating.
    subtest "$label - presence" => sub {
        _test_presence($factory);
    };

    subtest "$label - history" => sub {
        _test_history($factory);
    };

    subtest "$label - delayed" => sub {
        _test_delayed($factory);
    };

    subtest "$label - pattern subs" => sub {
        _test_pattern_subs($factory);
    };
}

# Subtest implementations are filled in by Tasks 1.2 - 1.6.
# Stub them so the module loads:
sub _test_core_send_poll  { ok(1, 'placeholder - Task 1.2') }
sub _test_core_fifo       { ok(1, 'placeholder - Task 1.2') }
sub _test_core_capacity   { ok(1, 'placeholder - Task 1.2') }
sub _test_pubsub_basic    { ok(1, 'placeholder - Task 1.2') }
sub _test_pubsub_exclude  { ok(1, 'placeholder - Task 1.2') }
sub _test_pubsub_full_drops { ok(1, 'placeholder - Task 1.2') }
sub _test_pubsub_unsubscribe { ok(1, 'placeholder - Task 1.2') }
sub _test_pubsub_idempotent { ok(1, 'placeholder - Task 1.2') }
sub _test_next_message    { ok(1, 'placeholder - Task 1.2') }
sub _test_cleanup         { ok(1, 'placeholder - Task 1.2') }
sub _test_flush           { ok(1, 'placeholder - Task 1.2') }
sub _test_validation      { ok(1, 'placeholder - Task 1.2') }
sub _test_presence        { ok(1, 'placeholder - Task 1.3') }
sub _test_history         { ok(1, 'placeholder - Task 1.4') }
sub _test_delayed         { ok(1, 'placeholder - Task 1.5') }
sub _test_pattern_subs    { ok(1, 'placeholder - Task 1.6') }

# Helper used by every subtest: synchronously run a Future-returning coderef.
sub _run(&) {
    my ($code) = @_;
    my $f = $code->();
    return $f->get if $f && ref($f) && $f->can('isa') && $f->isa('Future');
    return $f;
}

1;
