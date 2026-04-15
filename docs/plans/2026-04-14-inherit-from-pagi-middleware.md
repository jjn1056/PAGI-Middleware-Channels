# Inherit from PAGI::Middleware

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `PAGI::Middleware::Channels` a proper subclass of `PAGI::Middleware`, adopt the parent class's config storage convention, and use the parent's `modify_scope` helper to fix a scope-mutation bug in the current implementation.

**Architecture:** Three concrete changes to `lib/PAGI/Middleware/Channels.pm`: (1) `use parent 'PAGI::Middleware'`, (2) the backend-required validation moves from a custom `new` override into a tiny `_init` hook (so the parent's standard `new` runs unmodified and stores config at `$self->{config}{backend}`), and (3) `wrap()` builds a new scope via `modify_scope` instead of mutating the caller's hashref. The `_counter` per-instance state moves into `_init` since `$self->{config}` is now reserved for parent-managed config. A regression test in `t/20-facade/02-wrap.t` pins the non-mutation contract going forward.

**Tech Stack:** Perl 5.18+, Future::AsyncAwait, PAGI::Middleware (250-line base class shipping with the PAGI distribution; intent per project owner is for PAGI to split into smaller pieces, so the current heavy-dep weight is temporary).

**Perlbrew:** `source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default`

**Test setup:** `docker compose -f t/docker-compose.yml up -d` (Redis on `localhost:6379`).

**Test command:** `REDIS_HOST=localhost prove -lr t/`

**Verification gates (REQUIRED at end of every task):**

1. `prove -lr t/` — ALL 70+ tests pass (current baseline is 16 files / 69 tests; this plan adds one test).
2. `podchecker lib/PAGI/Middleware/Channels.pm` — clean.
3. No regressions in scope-keys present in inner-app's view: both `pagi.channels` and `pagi.channel` still reach the inner app, just via the new scope hashref.

---

## File structure

| File | Change |
|---|---|
| `t/20-facade/02-wrap.t` | Add subtest verifying outer-scope non-mutation (Task 1 RED → Task 2 GREEN). |
| `lib/PAGI/Middleware/Channels.pm` | Inherit, replace `new` with `_init`, update accessor + internal refs, switch `wrap` to `modify_scope`. |
| `cpanfile` | Add `requires 'PAGI::Middleware';` to top-level requires. |

---

### Task 1: failing test for outer-scope non-mutation (RED)

**Files:**
- Modify: `t/20-facade/02-wrap.t`

**Context:** The current `wrap()` mutates the outer scope hashref by directly assigning `$scope->{'pagi.channels'}` and `$scope->{'pagi.channel'}`. The `PAGI::Middleware` parent class explicitly recommends `modify_scope` (returns a new hashref) precisely to avoid this side effect on the caller. We pin that contract with a test now, then fix in Task 2.

- [ ] **Step 1: Write the failing test.**

Add this subtest to `t/20-facade/02-wrap.t` just before the `done_testing;` line at the end, after the existing `'cleanup on app exit'` subtest. The class names below match what the rest of the file uses post-rename — drop in as-is:

```perl
subtest 'wrap does not mutate the outer scope' => sub {
    my $channels = PAGI::Middleware::Channels->new(
        backend => PAGI::Middleware::Channels::Backend::Memory->new,
    );

    my $original_scope = { type => 'websocket', path => '/test' };
    my @original_keys  = sort keys %$original_scope;

    my $inner_app = async sub { };

    my $wrapped = $channels->wrap($inner_app);

    my $receive = async sub { { type => 'websocket.disconnect' } };
    my $send    = async sub { };

    run { $wrapped->($original_scope, $receive, $send) };

    my @keys_after = sort keys %$original_scope;
    is(\@keys_after, \@original_keys, 'outer scope keys unchanged');
    ok(!exists $original_scope->{'pagi.channels'}, 'pagi.channels not leaked into outer scope');
    ok(!exists $original_scope->{'pagi.channel'},  'pagi.channel not leaked into outer scope');
};
```

- [ ] **Step 2: Run it to verify it fails.**

Run: `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && prove -lv t/20-facade/02-wrap.t 2>&1 | tail -25'`

Expected: the new subtest FAILS with assertions like `pagi.channels not leaked into outer scope` failing, because the current `wrap()` directly assigns into the original `$scope` hashref.

- [ ] **Step 3: Commit the failing test.**

Don't fix yet — Task 2 fixes. Commit the RED test on its own so the bug is documented in git history.

```bash
git add t/20-facade/02-wrap.t
git commit -m "test(wrap): pin outer-scope non-mutation contract (RED)

Currently fails: wrap() mutates the caller's scope hashref by
directly assigning pagi.channels / pagi.channel keys into it.
Task 2 fixes by adopting PAGI::Middleware's modify_scope helper."
```

---

### Task 2: inherit from PAGI::Middleware and use modify_scope (GREEN)

**Files:**
- Modify: `lib/PAGI/Middleware/Channels.pm`

**Context:** Three coupled changes that all depend on each other so they ship as one commit: (1) inherit, (2) move backend-validation into `_init`, (3) use `modify_scope` in `wrap`.

After this change, `$self` looks like:
```
{
    config => { backend => $backend_instance },  # parent-managed
    _counter => 0,                                # our per-instance state
}
```

The `backend` accessor reads `$self->{config}{backend}`. Internal references switch from `$self->{_backend}` to `$self->backend` (use the accessor — cleaner and shorter).

- [ ] **Step 1: Add the parent declaration and remove the custom `new`.**

In `lib/PAGI/Middleware/Channels.pm`, change:

```perl
package PAGI::Middleware::Channels;
use strict;
use warnings;
use Future::AsyncAwait;
use Future;
use Future::IO;

our $VERSION = '0.001';

sub new {
    my ($class, %args) = @_;

    my $backend = $args{backend}
        or die "PAGI::Middleware::Channels: 'backend' argument required "
             . "(a PAGI::Middleware::Channels::Backend instance)";

    return bless {
        _backend => $backend,
        _counter => 0,
    }, $class;
}

sub backend { shift->{_backend} }
```

to:

```perl
package PAGI::Middleware::Channels;
use strict;
use warnings;
use parent 'PAGI::Middleware';
use Future::AsyncAwait;
use Future;
use Future::IO;

our $VERSION = '0.001';

# Parent's new() stores all args in $self->{config} and calls _init().
sub _init {
    my ($self, $config) = @_;

    $config->{backend}
        or die "PAGI::Middleware::Channels: 'backend' argument required "
             . "(a PAGI::Middleware::Channels::Backend instance)";

    $self->{_counter} = 0;
}

sub backend { $_[0]->{config}{backend} }
```

- [ ] **Step 2: Update `wrap()` to use `modify_scope` and the `backend` accessor.**

Replace the entire `wrap` method body (currently uses `$scope->{...} = ...` mutation and `$self->{_backend}` direct hash access):

```perl
sub wrap {
    my ($self, $inner_app) = @_;

    return async sub {
        my ($scope, $receive, $send) = @_;

        my $channel_name = $self->_generate_channel_name();

        my $new_scope = $self->modify_scope($scope, {
            'pagi.channels' => $self->_create_channel_interface($channel_name),
            'pagi.channel'  => $channel_name,
        });

        my $wrapped_receive = async sub {
            if (my $msg = await $self->backend->poll($channel_name)) {
                return $msg;
            }

            my $protocol_f = $receive->();

            while (!$protocol_f->is_ready) {
                await Future::IO->sleep(0.1);

                if (my $msg = await $self->backend->poll($channel_name)) {
                    return $msg;
                }
            }

            return $protocol_f->get;
        };

        my $err;
        eval { await $inner_app->($new_scope, $wrapped_receive, $send) };
        $err = $@;

        await $self->backend->cleanup($channel_name);

        die $err if $err;
    };
}
```

Three substantive changes versus the prior body: (1) builds `$new_scope` via `modify_scope` instead of mutating `$scope`; (2) inner_app is called with `$new_scope`; (3) all `$self->{_backend}` references swap to `$self->backend` (accessor).

- [ ] **Step 3: Update `_create_channel_interface` to use the accessor.**

Replace:

```perl
sub _create_channel_interface {
    my ($self, $channel_name) = @_;

    require PAGI::Channels;
    $self->{_backend}->set_channel_id($channel_name);

    return PAGI::Channels->new(
        backend      => $self->{_backend},
        channel_name => $channel_name,
    );
}
```

with:

```perl
sub _create_channel_interface {
    my ($self, $channel_name) = @_;

    require PAGI::Channels;
    $self->backend->set_channel_id($channel_name);

    return PAGI::Channels->new(
        backend      => $self->backend,
        channel_name => $channel_name,
    );
}
```

`_generate_channel_name` stays as-is — it uses `$self->{_counter}` which is still a direct-hash key (not config), and `_init` initializes it.

- [ ] **Step 4: Update POD where it implies our own `new` shape.**

In the existing POD (after `__END__`), find the `=head2 new` section. Update the description to mention parent inheritance:

Replace:

```pod
=head2 new

    my $channels = PAGI::Middleware::Channels->new(
        backend => $backend_instance,
    );

The C<backend> argument is B<required> and must be a
L<PAGI::Middleware::Channels::Backend> instance (e.g.,
L<PAGI::Middleware::Channels::Backend::Memory> or
L<PAGI::Middleware::Channels::Backend::Redis>).

This module does no backend construction of its own — callers wire
up the backend (and, for Redis, the underlying L<Async::Redis>
client) explicitly. The distribution has no runtime dependency on
any Redis client.
```

with:

```pod
=head2 new

    my $channels = PAGI::Middleware::Channels->new(
        backend => $backend_instance,
    );

Inherited from L<PAGI::Middleware>. The C<backend> argument is
B<required> and must be a L<PAGI::Middleware::Channels::Backend>
instance (e.g., L<PAGI::Middleware::Channels::Backend::Memory> or
L<PAGI::Middleware::Channels::Backend::Redis>). Required-argument
validation runs in C<_init>; missing C<backend> dies.

This module does no backend construction of its own — callers wire
up the backend (and, for Redis, the underlying L<Async::Redis>
client) explicitly. The distribution has no runtime dependency on
any Redis client.
```

- [ ] **Step 5: Add a brief INHERITANCE section to POD.**

Just above the `=head1 SEE ALSO` line near the end of the file, add:

```pod
=head1 INHERITANCE

Inherits from L<PAGI::Middleware>. The parent class provides:

=over 4

=item * Standard C<new(%config)> constructor that stores config at
C<< $self->{config} >> and calls C<_init>.

=item * C<modify_scope($scope, \%additions)> — non-mutating scope
augmentation (used by C<wrap()> to inject C<pagi.channels> and
C<pagi.channel> without side-effecting the caller's scope hashref).

=item * C<intercept_send>, C<buffer_request_body>, C<call> — not
currently used by Channels but available for future extension.

=back

```

- [ ] **Step 6: Run the suite.**

Run: `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && REDIS_HOST=localhost prove -lr t/ 2>&1 | tail -10'`

Expected: 16 files, **70 tests** (was 69 before Task 1 added one), all pass. The Task 1 RED test now passes.

- [ ] **Step 7: POD check.**

Run: `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && podchecker lib/PAGI/Middleware/Channels.pm 2>&1'`

Expected: `pod syntax OK`.

- [ ] **Step 8: Commit.**

```bash
git add lib/PAGI/Middleware/Channels.pm
git commit -m "refactor(middleware): inherit from PAGI::Middleware; use modify_scope (GREEN)

PAGI::Middleware::Channels now uses parent 'PAGI::Middleware' for
the standard middleware base class. Backend-required validation
moves from a custom new() override into the _init hook so the
parent's standard new() handles config storage at \$self->{config}.

wrap() switches from direct \$scope mutation to modify_scope, fixing
the bug pinned by the RED test added in the previous commit. Inner
app now sees a fresh scope hashref containing 'pagi.channels' and
'pagi.channel' alongside whatever the caller passed in; the outer
scope is unmodified.

Internal \$self->{_backend} references replaced with the public
backend() accessor (now reading \$self->{config}{backend}). The
_counter per-instance state stays as a direct hash key, initialized
in _init.

POD updated to note inheritance and document the available parent-
class methods."
```

---

### Task 3: add `PAGI::Middleware` to dependencies

**Files:**
- Modify: `cpanfile`

**Context:** The distribution now depends on `PAGI::Middleware` (which currently ships in the PAGI distribution; per project intent that distribution will be split into smaller parts, but for now installing this dep pulls in the full PAGI dist). Pin to no specific version since `PAGI::Middleware.pm` does not declare its own `$VERSION` — the resolver picks whatever the latest dist that provides the module is.

- [ ] **Step 1: Add the require to `cpanfile`.**

Open `/Users/jnapiorkowski/Desktop/PAGI-Channels/cpanfile`. After the existing `# Core` block (the section with `requires 'perl' …` through `requires 'namespace::clean';`), add a new block:

```perl
# Middleware base class (currently ships in the PAGI distribution;
# intent is for PAGI to be split into smaller pieces over time).
requires 'PAGI::Middleware';
```

The resulting top section should look like:

```perl
# cpanfile - PAGI-Middleware-Channels dependencies

# Core
requires 'perl', '5.018';
requires 'Future::AsyncAwait', '0.66';
requires 'Future::IO', '0.15';
requires 'Role::Tiny', '2.002004';
requires 'JSON::MaybeXS', '1.004005';
requires 'namespace::clean';

# Middleware base class (currently ships in the PAGI distribution;
# intent is for PAGI to be split into smaller pieces over time).
requires 'PAGI::Middleware';

# Optional serializer for higher throughput (not yet wired)
recommends 'Sereal::Encoder', '5.004';
recommends 'Sereal::Decoder', '5.004';
```

- [ ] **Step 2: Run the suite to confirm nothing broke.**

Run: `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && REDIS_HOST=localhost prove -lr t/ 2>&1 | tail -5'`

Expected: 16 files, 70 tests, all pass.

- [ ] **Step 3: Commit.**

```bash
git add cpanfile
git commit -m "deps(cpanfile): add PAGI::Middleware as required dependency

Channels middleware now inherits from PAGI::Middleware. PAGI ships
that module today; per project intent PAGI will be split into
smaller pieces over time, at which point this dep can be narrowed."
```

---

### Task 4: final verification

**Files:** none — verification only.

- [ ] **Step 1: Tests.**

```bash
bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && REDIS_HOST=localhost prove -lr t/ 2>&1 | tail -5'
```

Expected: `Files=16, Tests=70, … Result: PASS`.

- [ ] **Step 2: Confirm inheritance works at runtime.**

```bash
bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && perl -Ilib -MPAGI::Middleware::Channels -e "print +PAGI::Middleware::Channels->isa(q[PAGI::Middleware]) ? qq[isa OK\n] : qq[isa FAIL\n]"'
```

Expected: `isa OK`.

- [ ] **Step 3: Confirm `modify_scope` is available via inheritance.**

```bash
bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && perl -Ilib -MPAGI::Middleware::Channels -e "print +PAGI::Middleware::Channels->can(q[modify_scope]) ? qq[modify_scope inherited\n] : qq[modify_scope MISSING\n]"'
```

Expected: `modify_scope inherited`.

- [ ] **Step 4: Confirm scope is not mutated end-to-end.**

```bash
bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && prove -lv t/20-facade/02-wrap.t 2>&1 | grep -E "(wrap does not mutate|All tests successful|Result)"'
```

Expected: shows the `wrap does not mutate the outer scope` subtest passing and overall PASS.

- [ ] **Step 5: POD check on the modified file.**

```bash
bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && podchecker lib/PAGI/Middleware/Channels.pm 2>&1'
```

Expected: `pod syntax OK`.

- [ ] **Step 6: Grep audit — no stale `_backend` direct-hash references in the modified module.**

```bash
grep -n '_backend' lib/PAGI/Middleware/Channels.pm
```

Expected: zero hits. (All access goes through the `backend()` accessor or the `$self->{config}{backend}` storage.)

- [ ] **Step 7: If anything fails, fix and commit; otherwise this task is complete.**

---

## Summary

| Task | Scope | Est lines changed |
|------|-------|-------------------|
| 1 | RED test for outer-scope non-mutation | ~25 in 1 test file |
| 2 | Inherit from PAGI::Middleware + use modify_scope + accessor cleanup + POD | ~50 in `lib/PAGI/Middleware/Channels.pm` |
| 3 | `cpanfile` adds `requires 'PAGI::Middleware'` | ~3 |
| 4 | Verification | 0 |

Total: ~80 lines across 3 files, 3 commits + verification.
