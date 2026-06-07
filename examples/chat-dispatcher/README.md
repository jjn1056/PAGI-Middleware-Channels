# Chat Example — Dispatcher Style

Same chat as `examples/chat/`, rewritten on top of the
`PAGI::Context` event dispatcher (`on()` / `run()` / `on_error()`)
instead of a hand-rolled `while (my $event = await $receive->()) { ... }`
loop over the raw protocol coderef.

The two examples exist side by side so you can compare what the
dispatcher and `PAGI::Context` delegation buy in real handler code.

## Running

Same as `examples/chat/`:

**Terminal 1: Start Redis**
```bash
cd t && docker compose up -d
```

**Terminal 2: Start the app**
```bash
cd examples/chat-dispatcher
pagi-server --workers 4 app.pl
```

The UI is the same — `public/` is a symlink into `../chat/public`.

## What's different vs `examples/chat/`

The handler body shifts from a switch over `$event->{type}` to one
callback per event type, all chained off `$ctx`:

```perl
await $ctx
    ->on('websocket.receive' => async sub { ... })
    ->on('chat.message'      => async sub { ... })
    ->on('presence.join'     => async sub { ... })
    ->on('presence.leave'    => async sub { ... })
    ->on_error(sub { ... })
    ->run;
```

Concretely:

- **No manual loop or disconnect handling.** `run()` exits on
  `websocket.disconnect` automatically and resolves to `'disconnect'`.
- **No closing over `$ws`.** Protocol ops (`accept`, `send_json`)
  go directly on `$ctx`, which each callback already receives.
- **Centralised error handling.** A handler that throws routes through
  `on_error` instead of crashing the request.
- **Per-type callbacks.** Each event type has its own named callback
  rather than an `elsif` arm.

The `PAGI::Channel` integration is unchanged — `$ch->subscribe`,
`$ch->publish`, `$ch->list_presence` are exactly the same calls. The
dispatcher reads from the receive stream that Channels middleware has
already wrapped, so cross-process broadcast events appear as event
types like `chat.message` / `presence.join` / `presence.leave` and
get dispatched alongside the protocol's own `websocket.receive`.
