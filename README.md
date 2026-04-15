# PAGI-Middleware-Channels

Cross-process messaging middleware for PAGI applications. Exceeds Django Channels.

## Features

- **Presence Tracking** - Who's online (Phoenix-style)
- **Pattern Subscriptions** - `chat.*`, `events.**`
- **Delayed Messages** - Send after N seconds
- **Message History** - New subscribers get last N messages
- **Loop Agnostic** - Works with IO::Async, Mojo, UV

## Installation

```bash
cpanm PAGI::Middleware::Channels
```

## Quick Start

```perl
use PAGI::Middleware::Channels;
use PAGI::Middleware::Channels::Backend::Redis;
use Async::Redis;

my $redis = Async::Redis->new(
    uri       => 'redis://localhost:6379',
    prefix    => 'myapp:channels:',
    reconnect => 1,
);
$redis->connect->get;

my $channels = PAGI::Middleware::Channels->new(
    backend => PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis),
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

### Memory — `PAGI::Middleware::Channels::Backend::Memory`

Single-process, in-memory. Perfect for development and testing.

```perl
use PAGI::Middleware::Channels::Backend::Memory;

my $channels = PAGI::Middleware::Channels->new(
    backend => PAGI::Middleware::Channels::Backend::Memory->new,
);
```

### Redis — `PAGI::Middleware::Channels::Backend::Redis`

Multi-process, multi-server. Takes any client ducking the [Async::Redis](https://metacpan.org/pod/Async::Redis) interface; bring your own client and hand it to the backend. PAGI-Middleware-Channels itself has no runtime dependency on a Redis client.

```perl
use PAGI::Middleware::Channels::Backend::Redis;
use Async::Redis;

my $redis = Async::Redis->new(
    uri       => 'redis://localhost:6379',
    prefix    => 'myapp:channels:',
    reconnect => 1,
);
$redis->connect->get;

my $channels = PAGI::Middleware::Channels->new(
    backend => PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis),
);
```

## Examples

See the `examples/` directory:

- `chat-with-workers/` - Chat app with worker pool and presence
- `task-queue/` - Task queue with real-time progress updates

## Documentation

See `perldoc PAGI::Middleware::Channels` for full documentation.

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
