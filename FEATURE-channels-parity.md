# PAGI-Channels — Outstanding Work

**What this is:** An inventory of what's *not* done in PAGI-Channels, organized so we can decide what to tackle next and what to defer. The four headline v1 features (presence, pattern subscriptions, delayed messages, message history) are implemented in both the Memory and Redis backends. What follows is the list of gaps.

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

## 2. Planned refactor (not started)

### 2.1 Redis backend: accept a pre-configured `Async::Redis` instance

Full plan: `docs/plans/2026-03-16-redis-backend-upgrade.md` (6 TDD tasks, ~135 lines across 4 files). Moves connection management, auto-reconnect, prefix handling, fork safety, and connection pooling from the backend into `Async::Redis`. Adds a `redis =>` constructor option; URI mode stays backward compatible. Deletes `_parse_uri`.

**Likely blocks a `0.01` CPAN release** — the current backend has no auto-reconnect, so a Redis restart kills the app.

---

## 3. Uncommitted WIP on `main` — decide: land or revert

| File | Change | Disposition |
|---|---|---|
| `lib/PAGI/Channels/Backend/Redis.pm` | `flush()` now calls `_ensure_connected` first | Correctness fix. |
| `examples/chat/app.pl` | Refactored onto `PAGI::WebSocket` + `PAGI::App::Router` (`/ws/chat/:room`); lifespan startup hook calls `$channels->backend->flush` to clear stale presence | Adds `PAGI::WebSocket` + `PAGI::App::Router` as example-only deps. Acceptable? |
| `examples/chat/public/js/app.js` | Client URL updated for path-param route | Paired with above. |
| `examples/chat/README.md` | Added `redis-cli FLUSHDB` note | Trivial. |
| `docs/plans/2026-03-16-redis-backend-upgrade.md` *(new)* | Plan for §2.1 | Commit with Task 1 or now as a pending marker. |

---

## 4. Release engineering (CPAN readiness)

All of these block an honest `cpanm PAGI::Channels`.

### 4.1 No `Changes` file
`@Basic` generates `LICENSE` but not `Changes`. Required for CPAN. Add `[NextRelease]` to `dist.ini` and bootstrap a `Changes` file.

### 4.2 `dist.ini` is minimal
Current: `[@Basic]`, `[AutoPrereqs]`, `[MetaJSON]`, `[PodSyntaxTests]`, `[Prereqs]`. Missing at minimum:

- `[@Git]` — tagged releases, clean-tree check
- `[NextRelease]` — Changes integration
- `[TestRelease]`, `[ConfirmRelease]`, `[UploadToCPAN]` — release pipeline
- `[MetaResources]` — bugtracker and repo URL (shows on MetaCPAN)
- `[OurPkgVersion]` or `[AutoVersion]` — version management across all packages
- `[PodCoverageTests]` — catches missing POD (see §5.2)
- `[ReadmeFromPod]` — generate `README.md` from main-module POD

### 4.3 No CI
No `.github/workflows/`, no Travis config. Minimum: one workflow that runs `prove -lr t/` on push, ideally across a Perl version matrix, with a Redis service container.

### 4.4 `Async::Redis` version pinning inconsistent
- `cpanfile`: `recommends 'Async::Redis', '0.001003'`
- `dist.ini`: no `Async::Redis` entry (core prereqs only)
- `docs/plans/2026-03-16-redis-backend-upgrade.md`: references `0.001005+`

Pick a minimum and normalize across both files.

### 4.5 Version strategy
`$VERSION = '0.001'` hard-coded in `Channels.pm`; `Backend.pm`, `Backend::Memory`, `Backend::Redis` have no `$VERSION` at all. `[OurPkgVersion]` in `dist.ini` would fix this uniformly.

---

## 5. Documentation cleanup

### 5.1 `docs/design/channel-layer.mkdn` is stale
Says `Version 0.3 / Status: Design Document` and `v1: Memory Backend Only / Future: Redis Backend`. Redis is in v1. Either rewrite to reflect the shipped architecture or move to `docs/design/archive/`.

### 5.2 POD coverage is thin on the backends
`Backend::Memory` POD documents `send` and `poll` only — the other 13+ required methods (`publish`, `subscribe`, `track`, `psubscribe`, `process_delayed`, `subscribe_with_history`, etc.) have no POD. `Backend::Redis` POD: same pattern, needs full audit. Enabling `[PodCoverageTests]` (§4.2) will surface the full list.

### 5.3 README
Quick-start is solid; doesn't mention `track` / `untrack` / `psubscribe` / `history` / Django aliases. Probably acceptable for a CPAN README — flag for later.

---

## 6. Deferred / explicit non-goals

Decided out of scope in the original brainstorm; remain out absent new signal:

- **Delay cancellation** — "No cancellation in v1" still holds.
- **Auth middleware** — separate concern, belongs in a different PAGI distribution.
- **CRDT-based presence (Phoenix-style)** — current model (broadcast `presence.join`/`presence.leave` + poll `list_presence`) is sufficient for target use cases. Revisit only if strong multi-region merge guarantees become a real ask.

---

## Appendix: v1 parity reference

Informational. The code is authoritative.

### vs. Django Channels
| Feature | Django | PAGI-Channels |
|---|---|---|
| `group_add` / `group_discard` / `group_send` | Yes | Yes — real aliases for `subscribe` / `unsubscribe` / `publish` (`Channels.pm:189-191`) |
| Memory backend | Yes | Yes |
| Redis backend | Yes | Yes |
| Capacity / message expiry / group expiry | Yes | Yes |

### vs. Phoenix Channels
| Feature | Phoenix | PAGI-Channels |
|---|---|---|
| Built-in presence | Yes (CRDT) | Yes (event-broadcast + poll) |
| Presence join/leave events | Yes | Yes |
| Presence metadata | Yes | Yes — arbitrary hash |
| Multi-node presence | Yes | Yes via Redis backend (non-CRDT) |

### Exceeds both
- Pattern subscriptions (`*` single segment, `**` recursive)
- Delayed messages
- Message history on subscribe
