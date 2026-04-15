# Convert Backend Role to Class Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert `PAGI::Middleware::Channels::Backend` from a Role::Tiny role to a plain base class with abstract-method stubs, and update the Memory and Redis backends to inherit from it.

**Architecture:** The Backend "interface" carries no shared behavior today — it is just a list of `requires` method names. Replacing it with a base class whose abstract methods `croak "abstract method"` gives the same contract in a form that matches the rest of the distribution (`PAGI::Middleware::Channels` already inherits from `PAGI::Middleware` via `use parent`). Subclasses swap `Role::Tiny::With` + `with '...'` for `use parent '...'`. We drop Role::Tiny from the runtime dep graph.

**Trade-off accepted:** We lose Role::Tiny's compile-time `requires` enforcement. A subclass that forgets to implement `poll` will now fail at runtime on first call instead of at `with` time. The test suite exercises every backend method on both backends, so the gap is caught by the test run, not in production.

**Tech Stack:** Perl 5.18+, `use parent`, `Carp::croak`. No new deps. Removes `Role::Tiny` from direct runtime use.

---

### Task 1: Add regression test pinning the abstract contract

**Files:**
- Create: `t/10-memory/08-abstract-backend.t`

- [ ] **Step 1: Write the failing test**

Create `t/10-memory/08-abstract-backend.t`:

```perl
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
        qr/abstract/i,
        "$m croaks as abstract on bare subclass";
}

done_testing;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && prove -lv t/10-memory/08-abstract-backend.t'`

Expected: FAIL. The role currently has no `new`, no abstract stubs, and `use parent` on a role package will not produce the croaking behavior the test asserts. This is the RED state that justifies the conversion.

- [ ] **Step 3: Commit the failing test**

```bash
git add t/10-memory/08-abstract-backend.t
git commit -m "test(backend): pin abstract-method contract (RED)"
```

---

### Task 2: Convert Backend.pm from role to class with abstract stubs

**Files:**
- Modify: `lib/PAGI/Middleware/Channels/Backend.pm` (full rewrite of code section; POD kept)

- [ ] **Step 1: Rewrite the file**

Replace the contents of `lib/PAGI/Middleware/Channels/Backend.pm` with:

```perl
package PAGI::Middleware::Channels::Backend;
use strict;
use warnings;
use Carp ();

# Abstract base class for channel-layer backends. Subclasses MUST
# override every method below. Each default implementation croaks so
# missing overrides surface immediately on first call.

sub new {
    my ($class, %args) = @_;
    return bless { %args }, $class;
}

my @ABSTRACT = qw(
    send poll subscribe unsubscribe publish flush cleanup
    psubscribe punsubscribe track untrack list_presence
    send_delayed publish_delayed subscribe_with_history
);

for my $method (@ABSTRACT) {
    no strict 'refs';
    *{__PACKAGE__ . "::$method"} = sub {
        my $self = shift;
        my $class = ref($self) || $self;
        Carp::croak("$class must implement abstract method '$method'");
    };
}

1;

__END__

=head1 NAME

PAGI::Middleware::Channels::Backend - Abstract base class for channel layer backends

=head1 DESCRIPTION

Subclasses must override every method listed below. The default
implementations croak; there is no shared behavior in this base class.

=head1 REQUIRED METHODS

=head2 Core

=over 4

=item send($channel, $message) -> Future

=item poll($channel) -> Future($message | undef)

=item subscribe($channel, $topic, %opts) -> Future

=item unsubscribe($channel, $topic) -> Future

=item publish($topic, $message, %opts) -> Future

=item flush() -> Future

=item cleanup($channel) -> Future

=back

=head2 Pattern Subscriptions

=over 4

=item psubscribe($channel, $pattern) -> Future

=item punsubscribe($channel, $pattern) -> Future

=back

=head2 Presence

=over 4

=item track($topic, $presence_data) -> Future

=item untrack($topic) -> Future

=item list_presence($topic) -> Future[@presences]

=back

=head2 Delayed Messages

=over 4

=item send_delayed($channel, $message, $delay_seconds) -> Future

=item publish_delayed($topic, $message, $delay_seconds) -> Future

=back

=head2 History

=over 4

=item subscribe_with_history($channel, $topic, $history_count, %opts) -> Future

=back

=cut
```

- [ ] **Step 2: Run the regression test from Task 1**

Run: `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && prove -lv t/10-memory/08-abstract-backend.t'`

Expected: PASS. All 15 abstract methods croak with `/abstract/i`.

- [ ] **Step 3: Run the full memory suite to confirm no regressions yet**

Memory and Redis backends still `with` the role at this point, which will fail loading because the package is no longer a role. That is expected — the next task fixes it. Do **not** run the full suite here; just confirm Task 1's test passes in isolation.

- [ ] **Step 4: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend.pm
git commit -m "refactor(backend): convert Backend from role to abstract class (GREEN)"
```

---

### Task 3: Switch Memory backend to inheritance

**Files:**
- Modify: `lib/PAGI/Middleware/Channels/Backend/Memory.pm` (lines 1-10)

- [ ] **Step 1: Replace `Role::Tiny::With` + `with` with `use parent`**

Change the top of the file from:

```perl
package PAGI::Middleware::Channels::Backend::Memory;
use strict;
use warnings;
use Future::AsyncAwait;
use Future;
use Role::Tiny::With;
use Time::HiRes ();
use namespace::clean;

with 'PAGI::Middleware::Channels::Backend';
```

to:

```perl
package PAGI::Middleware::Channels::Backend::Memory;
use strict;
use warnings;
use parent 'PAGI::Middleware::Channels::Backend';
use Future::AsyncAwait;
use Future;
use Time::HiRes ();
use namespace::clean;
```

Rationale for ordering: `use parent` comes early so `@ISA` is established before any code in the file references it. `namespace::clean` stays at the end so it only strips helpers imported after it.

- [ ] **Step 2: Run the memory test suite**

Run: `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && prove -lr t/10-memory/'`

Expected: All tests pass (including the new `08-abstract-backend.t`). If anything fails, the root cause is almost certainly a method Memory was relying on Role::Tiny to provide — investigate before proceeding.

- [ ] **Step 3: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend/Memory.pm
git commit -m "refactor(backend): Memory inherits from Backend base class"
```

---

### Task 4: Switch Redis backend to inheritance

**Files:**
- Modify: `lib/PAGI/Middleware/Channels/Backend/Redis.pm` (lines 1-12)

- [ ] **Step 1: Replace `Role::Tiny::With` + `with` with `use parent`**

Change the top of the file from:

```perl
# lib/PAGI/Middleware/Channels/Backend/Redis.pm
package PAGI::Middleware::Channels::Backend::Redis;
use strict;
use warnings;
use Future::AsyncAwait;
use Future;
use Role::Tiny::With;
use JSON::MaybeXS qw(encode_json decode_json);
use Time::HiRes ();
use namespace::clean;

with 'PAGI::Middleware::Channels::Backend';
```

to:

```perl
# lib/PAGI/Middleware/Channels/Backend/Redis.pm
package PAGI::Middleware::Channels::Backend::Redis;
use strict;
use warnings;
use parent 'PAGI::Middleware::Channels::Backend';
use Future::AsyncAwait;
use Future;
use JSON::MaybeXS qw(encode_json decode_json);
use Time::HiRes ();
use namespace::clean;
```

- [ ] **Step 2: Run the Redis test suite**

Ensure Redis is running: `cd t && docker compose up -d && cd ..`

Run: `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && REDIS_HOST=localhost prove -lr t/30-redis/'`

Expected: All Redis backend tests pass.

- [ ] **Step 3: Commit**

```bash
git add lib/PAGI/Middleware/Channels/Backend/Redis.pm
git commit -m "refactor(backend): Redis inherits from Backend base class"
```

---

### Task 5: Drop Role::Tiny from declared runtime deps

**Files:**
- Modify: `dist.ini`
- Modify: `cpanfile`

- [ ] **Step 1: Audit remaining Role::Tiny usage**

Run: `bash -c "source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && grep -rn 'Role::Tiny' lib/ t/ cpanfile dist.ini"`

Expected result: zero matches under `lib/`. Any matches in `t/` are acceptable only if they belong to code that genuinely needs role composition; if not, remove them. Matches in `cpanfile` / `dist.ini` are what we are about to clean up.

If any `lib/` match remains, STOP — a prior task was incomplete.

- [ ] **Step 2: Remove `Role::Tiny` from `[Prereqs]` in `dist.ini`**

Current `[Prereqs]` block contains:

```
Role::Tiny = 2.002004
```

Delete that exact line. Leave the other entries untouched. `AutoPrereqs` will no longer pick up Role::Tiny because nothing under `lib/` uses it.

- [ ] **Step 3: Remove `Role::Tiny` from `cpanfile` if present**

Check `cpanfile` for a `requires 'Role::Tiny'` line; if present, delete it. If absent (because AutoPrereqs is the source of truth), skip this step.

- [ ] **Step 4: Run the full test suite**

```bash
cd t && docker compose up -d && cd ..
bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.40.0@default && REDIS_HOST=localhost prove -lr t/'
```

Expected: every test file passes, same count as before the refactor plus one new file (`08-abstract-backend.t`).

- [ ] **Step 5: Commit**

```bash
git add dist.ini cpanfile
git commit -m "deps: drop Role::Tiny from runtime prereqs"
```

---

## Self-Review Checklist

- Spec coverage: every subsystem touched by the role removal has a task (base class, Memory, Redis, deps, regression test). ✓
- No placeholders: every step shows full code or exact command. ✓
- Type consistency: abstract method list in Task 1 test and Task 2 base class match exactly (15 methods, identical names). ✓
- The `@ABSTRACT` list in Backend.pm is the single source of truth for which methods croak; the test iterates the same names independently, so a drift between them shows up as a test failure.
