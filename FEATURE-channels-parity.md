# PAGI-Middleware-Channels — Outstanding Work

**What this is:** An inventory of what's *not* done in PAGI-Middleware-Channels, organized so we can decide what to tackle next and what to defer. The four headline v1 features (presence, pattern subscriptions, delayed messages, message history) are implemented in both the Memory and Redis backends. What follows is the list of gaps.

**Verification state (this session):**

- Memory + facade tests: **pass** (36 tests, 9 files).
- Redis tests: **not run this session** (Docker not up). Last Redis-touching commit: `6e6a98a "Fix async poll handling for Redis backend"`.

---

## 1. Code gaps

### 1.1 Sereal serializer — recommended but not wired

`cpanfile` has `recommends 'Sereal::Encoder'` / `'Sereal::Decoder'`, and the original brainstorm listed "JSON default, Sereal optional." Nothing in `lib/` imports or uses Sereal — `JSON::MaybeXS` is the only serializer. Pick one:

- **Drop** from `recommends` (simple, honest about current scope).
- **Wire in** behind a `serializer => 'sereal'` constructor option on both backends.

---

## 2. Release engineering (CPAN readiness)

All of these block an honest `cpanm PAGI::Middleware::Channels`.

### 2.1 No `Changes` file
`@Basic` generates `LICENSE` but not `Changes`. Required for CPAN. Add `[NextRelease]` to `dist.ini` and bootstrap a `Changes` file.

### 2.2 `dist.ini` is minimal
Current: `[@Basic]`, `[AutoPrereqs]`, `[MetaJSON]`, `[PodSyntaxTests]`, `[Prereqs]`. Missing at minimum:

- `[@Git]` — tagged releases, clean-tree check
- `[NextRelease]` — Changes integration
- `[TestRelease]`, `[ConfirmRelease]`, `[UploadToCPAN]` — release pipeline
- `[MetaResources]` — bugtracker and repo URL (shows on MetaCPAN)
- `[OurPkgVersion]` or `[AutoVersion]` — version management across all packages
- `[PodCoverageTests]` — catches missing POD (see §3.2)
- `[ReadmeFromPod]` — generate `README.md` from main-module POD

### 2.3 No CI
No `.github/workflows/`, no Travis config. Minimum: one workflow that runs `prove -lr t/` on push, ideally across a Perl version matrix, with a Redis service container.

### 2.4 Version strategy
`$VERSION = '0.001'` hard-coded in `Channels.pm`; `Backend.pm`, `Backend::Memory`, `Backend::Redis` have no `$VERSION` at all. `[OurPkgVersion]` in `dist.ini` would fix this uniformly.

---

## 3. Documentation cleanup

### 3.1 `docs/design/channel-layer.mkdn` is stale
Says `Version 0.3 / Status: Design Document` and `v1: Memory Backend Only / Future: Redis Backend`. Redis is in v1. Either rewrite to reflect the shipped architecture or move to `docs/design/archive/`.

### 3.2 POD coverage is thin on the backends
`Backend::Memory` POD documents `send` and `poll` only — the other 13+ required methods (`publish`, `subscribe`, `track`, `psubscribe`, `process_delayed`, `subscribe_with_history`, etc.) have no POD. `Backend::Redis` POD: SYNOPSIS + constructor options now documented; method-level POD still missing. Enabling `[PodCoverageTests]` (§2.2) will surface the full list.

### 3.3 README
Quick-start is solid; doesn't mention `track` / `untrack` / `psubscribe` / `history` / Django aliases. Probably acceptable for a CPAN README — flag for later.

---

## 4. Deferred / explicit non-goals

Decided out of scope in the original brainstorm; remain out absent new signal:

- **Delay cancellation** — "No cancellation in v1" still holds.
- **Auth middleware** — separate concern, belongs in a different PAGI distribution.
- **CRDT-based presence (Phoenix-style)** — current model (broadcast `presence.join`/`presence.leave` + poll `list_presence`) is sufficient for target use cases. Revisit only if strong multi-region merge guarantees become a real ask.

---

## Appendix: v1 parity reference

Informational. The code is authoritative.

### vs. Django Channels
| Feature | Django | PAGI-Middleware-Channels |
|---|---|---|
| `group_add` / `group_discard` / `group_send` | Yes | Yes — real aliases for `subscribe` / `unsubscribe` / `publish` (`lib/PAGI/Channels.pm:76-78`) |
| Memory backend | Yes | Yes |
| Redis backend | Yes | Yes |
| Capacity / message expiry / group expiry | Yes | Yes |

### vs. Phoenix Channels
| Feature | Phoenix | PAGI-Middleware-Channels |
|---|---|---|
| Built-in presence | Yes (CRDT) | Yes (event-broadcast + poll) |
| Presence join/leave events | Yes | Yes |
| Presence metadata | Yes | Yes — arbitrary hash |
| Multi-node presence | Yes | Yes via Redis backend (non-CRDT) |

### Exceeds both
- Pattern subscriptions (`*` single segment, `**` recursive)
- Delayed messages
- Message history on subscribe
