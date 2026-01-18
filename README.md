# PAGI-Channels

Cross-process messaging for PAGI applications. Exceeds Django Channels.

## Features

- **Presence Tracking** - Who's online (Phoenix-style)
- **Pattern Subscriptions** - `chat.*`, `events.**`
- **Delayed Messages** - Send after N seconds
- **Message History** - New subscribers get last N messages
- **Loop Agnostic** - Works with IO::Async, Mojo, UV

## Installation

```bash
cpanm PAGI::Channels
```

## Quick Start

```perl
use PAGI::Channels;

my $channels = PAGI::Channels->new(
    backend => 'redis://localhost:6379',
);

my $app = $channels->wrap(async sub {
    my ($scope, $receive, $send) = @_;

    my $ch = $scope->{'pagi.channels'};

    await $ch->subscribe("chat.room1",
        presence => { user => 'alice' }
    );

    while (1) {
        my $event = await $receive->();
        # Handle chat messages, presence events, etc.
    }
});
```

## Backends

### Memory (`memory://`)

Single-process, in-memory. Perfect for development and testing.

```perl
my $channels = PAGI::Channels->new(backend => 'memory://');
```

### Redis (`redis://host:port`)

Multi-process, multi-server. Requires Redis and [Async::Redis](https://metacpan.org/pod/Async::Redis).

```perl
my $channels = PAGI::Channels->new(backend => 'redis://localhost:6379');
```

## Examples

See the `examples/` directory:

- `chat-with-workers/` - Chat app with worker pool and presence
- `task-queue/` - Task queue with real-time progress updates

## Documentation

See `perldoc PAGI::Channels` for full documentation.

## Testing

```bash
# Start Redis
cd t && docker compose up -d && cd ..

# Run all tests
REDIS_HOST=localhost prove -lr t/

# Memory tests only (no Redis needed)
prove -l t/10-memory/
```

## License

Same as Perl itself.
