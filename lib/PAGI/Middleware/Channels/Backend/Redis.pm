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

# Defaults
use constant {
    DEFAULT_CAPACITY     => 100,
    DEFAULT_EXPIRY       => 60,
    DEFAULT_GROUP_EXPIRY => 86400,
    DEFAULT_MAX_SIZE     => 1_048_576,
    DEFAULT_HISTORY_SIZE => 0,
};

sub new {
    my ($class, %args) = @_;

    my $redis = $args{redis}
        or die "PAGI::Middleware::Channels::Backend::Redis: 'redis' argument required "
             . "(Async::Redis instance or compatible)";

    return bless {
        _redis       => $redis,
        _channel_id  => undef,
        capacity     => $args{capacity}     // DEFAULT_CAPACITY,
        expiry       => $args{expiry}       // DEFAULT_EXPIRY,
        group_expiry => $args{group_expiry} // DEFAULT_GROUP_EXPIRY,
        max_size     => $args{max_size}     // DEFAULT_MAX_SIZE,
        history_size => $args{history_size} // DEFAULT_HISTORY_SIZE,
    }, $class;
}

# Async::Redis does not prefix KEYS/SCAN patterns, so we prepend
# the client's own prefix when we need namespace-scoped lookups.
sub _redis_prefix { $_[0]->{_redis}{prefix} // '' }

sub set_channel_id {
    my ($self, $channel_id) = @_;
    $self->{_channel_id} = $channel_id;
}

sub channel_id { shift->{_channel_id} }

# Key helpers — structural only. Top-level namespace comes from
# the Async::Redis instance's own prefix.
sub _queue_key    { 'q:'   . $_[1] }
sub _group_key    { 'g:'   . $_[1] }
sub _presence_key { 'p:'   . $_[1] }
sub _history_key  { 'h:'   . $_[1] }
sub _pattern_key  { 'pat:' . $_[1] }
sub _delayed_key  { 'delayed' }

# Helper to convert pattern to regex (same as Memory backend)
sub _pattern_to_regex {
    my ($self, $pattern) = @_;

    # Escape special regex chars except our wildcards
    my $regex = quotemeta($pattern);

    # ** matches zero or more segments (including dots)
    # When ** follows a dot (e.g., "foo.**"), make the dot optional
    # so "foo.**" matches "foo", "foo.bar", "foo.bar.baz"
    $regex =~ s/\\\.\\\*\\\*/(\\..*)?\$/g;

    # Handle ** at the beginning or not preceded by a dot
    $regex =~ s/\\\*\\\*/.*/g;

    # * matches exactly one segment (no dots)
    $regex =~ s/\\\*/[^.]+/g;

    return qr/^$regex$/;
}

sub _topic_matches_pattern {
    my ($self, $topic, $pattern) = @_;
    my $regex = $self->_pattern_to_regex($pattern);
    return $topic =~ $regex;
}

# Helper to deliver to channel (checks capacity)
async sub _deliver_to_channel {
    my ($self, $channel, $message) = @_;

    my $qkey = $self->_queue_key($channel);
    my $len = await $self->{_redis}->llen($qkey);

    if ($len < $self->{capacity}) {
        my $json = encode_json($message);
        await $self->{_redis}->rpush($qkey, $json);
        await $self->{_redis}->expire($qkey, $self->{expiry});
        return 1;
    }
    return 0;  # Dropped due to full queue
}

# Core: send
async sub send {
    my ($self, $channel, $message) = @_;

    my $key = $self->_queue_key($channel);
    my $len = await $self->{_redis}->llen($key);

    if ($len >= $self->{capacity}) {
        return Future->fail('ChannelFull', 'channel', $channel);
    }

    my $json = encode_json($message);
    await $self->{_redis}->rpush($key, $json);
    await $self->{_redis}->expire($key, $self->{expiry});

    return 1;
}

# Core: poll
async sub poll {
    my ($self, $channel) = @_;

    # Drain any due delayed messages into their target queues first
    await $self->process_delayed;

    my $key = $self->_queue_key($channel);
    my $json = await $self->{_redis}->lpop($key);

    return undef unless defined $json;
    return decode_json($json);
}

# Subscribe
async sub subscribe {
    my ($self, $channel, $topic, %opts) = @_;

    my $key = $self->_group_key($topic);
    await $self->{_redis}->sadd($key, $channel);
    await $self->{_redis}->expire($key, $self->{group_expiry});

    # Handle presence option
    if (my $presence_data = $opts{presence}) {
        await $self->track($topic, $presence_data, $channel);

        # Broadcast join event (but we need to check if this is a new subscription)
        await $self->publish($topic, {
            type     => 'presence.join',
            topic    => $topic,
            presence => $presence_data,
        }, exclude => $channel);
    }

    return 1;
}

# Unsubscribe
async sub unsubscribe {
    my ($self, $channel, $topic) = @_;

    # Get presence data before removing (for leave event)
    my $pkey = $self->_presence_key($topic);
    my $json = await $self->{_redis}->hget($pkey, $channel);
    my $presence_data = $json ? decode_json($json) : undef;

    # Remove from group
    my $key = $self->_group_key($topic);
    await $self->{_redis}->srem($key, $channel);

    # Remove presence
    if ($presence_data) {
        await $self->{_redis}->hdel($pkey, $channel);

        # Broadcast leave event
        await $self->publish($topic, {
            type     => 'presence.leave',
            topic    => $topic,
            presence => $presence_data,
        }, exclude => $channel);
    }

    return 1;
}

# Pattern Subscribe
async sub psubscribe {
    my ($self, $channel, $pattern) = @_;

    # Store pattern association: channel -> pattern (using set to avoid duplicates)
    my $key = $self->_pattern_key($channel);
    await $self->{_redis}->sadd($key, $pattern);
    await $self->{_redis}->expire($key, $self->{group_expiry});

    return 1;
}

# Pattern Unsubscribe
async sub punsubscribe {
    my ($self, $channel, $pattern) = @_;

    my $key = $self->_pattern_key($channel);
    if (defined $pattern) {
        await $self->{_redis}->srem($key, $pattern);
    } else {
        await $self->{_redis}->del($key);
    }

    return 1;
}

# Publish
async sub publish {
    my ($self, $topic, $message, %opts) = @_;

    my $exclude = $opts{exclude} // [];
    $exclude = [$exclude] unless ref $exclude eq 'ARRAY';
    my %excluded = map { $_ => 1 } @$exclude;

    my %delivered;  # Track to avoid duplicates

    # Store in history buffer (if history enabled and not a presence event)
    if ($self->{history_size} > 0 && $message->{type} !~ /^presence\./) {
        my $history_key = $self->_history_key($topic);
        await $self->{_redis}->rpush($history_key, encode_json($message));
        await $self->{_redis}->ltrim($history_key, -$self->{history_size}, -1);
        await $self->{_redis}->expire($history_key, $self->{group_expiry});
    }

    # Direct group subscribers
    my $key = $self->_group_key($topic);
    my $members_ref = await $self->{_redis}->smembers($key);
    my @members = ref $members_ref eq 'ARRAY' ? @$members_ref : ();

    for my $channel (@members) {
        next if $excluded{$channel};
        await $self->_deliver_to_channel($channel, $message);
        $delivered{$channel} = 1;
    }

    # Pattern subscribers - scan for all pattern keys
    my $pattern_keys_ref = await $self->{_redis}->keys($self->_redis_prefix . 'pat:*');
    my @pattern_keys = ref $pattern_keys_ref eq 'ARRAY' ? @$pattern_keys_ref : ();

    for my $pkey (@pattern_keys) {
        # Extract channel from key (KEYS returns absolute keys including
        # the client's prefix; everything after 'pat:' is our channel name).
        my ($channel) = $pkey =~ /pat:(.+)$/;
        next unless $channel;
        next if $excluded{$channel};
        next if $delivered{$channel};  # Already delivered via exact match

        # Use the relative key — Async::Redis will re-apply its prefix.
        my $patterns_ref = await $self->{_redis}->smembers($self->_pattern_key($channel));
        my @patterns = ref $patterns_ref eq 'ARRAY' ? @$patterns_ref : ();

        for my $pattern (@patterns) {
            if ($self->_topic_matches_pattern($topic, $pattern)) {
                await $self->_deliver_to_channel($channel, $message);
                $delivered{$channel} = 1;
                last;  # Only deliver once per channel
            }
        }
    }

    return 1;
}

# Flush all data under the Async::Redis instance's prefix.
# Note: this deletes EVERYTHING under that prefix, not just our
# structural keys — the caller should use a dedicated prefix if
# they want isolation from other keyspaces.
async sub flush {
    my ($self) = @_;

    my $prefix = $self->_redis_prefix;
    my $keys_ref = await $self->{_redis}->keys($prefix . '*');
    my @abs_keys = ref $keys_ref eq 'ARRAY' ? @$keys_ref : ();
    return 1 unless @abs_keys;

    # Strip prefix so Async::Redis re-applies it exactly once on DEL.
    my @relative = map { my $k = $_; $k =~ s/^\Q$prefix\E// if length $prefix; $k } @abs_keys;
    await $self->{_redis}->del(@relative);

    return 1;
}

# Cleanup channel
async sub cleanup {
    my ($self, $channel) = @_;

    # Remove queue
    await $self->{_redis}->del($self->_queue_key($channel));

    # Remove from all groups (scan for membership) and handle presence
    my $group_keys_ref = await $self->{_redis}->keys($self->_redis_prefix . 'g:*');
    my @group_keys = ref $group_keys_ref eq 'ARRAY' ? @$group_keys_ref : ();

    for my $abs_key (@group_keys) {
        # KEYS returns absolute keys; extract topic after the 'g:' marker.
        my ($topic) = $abs_key =~ /g:(.+)$/;
        next unless $topic;

        my $gkey = $self->_group_key($topic);  # relative; Async::Redis prefixes.
        my $is_member = await $self->{_redis}->sismember($gkey, $channel);
        next unless $is_member;

        # Check for presence data
        my $pkey = $self->_presence_key($topic);
        my $json = await $self->{_redis}->hget($pkey, $channel);
        if ($json) {
            my $presence_data = decode_json($json);
            await $self->{_redis}->hdel($pkey, $channel);

            # Broadcast leave event
            await $self->publish($topic, {
                type     => 'presence.leave',
                topic    => $topic,
                presence => $presence_data,
            }, exclude => $channel);
        }

        await $self->{_redis}->srem($gkey, $channel);
    }

    # Remove pattern subscriptions
    await $self->{_redis}->del($self->_pattern_key($channel));

    return 1;
}

# Presence: track
async sub track {
    my ($self, $topic, $presence_data, $channel) = @_;

    $channel //= $self->{_channel_id}
        or die "track() requires channel_id or set_channel_id() first";

    my $key = $self->_presence_key($topic);

    my $json = encode_json($presence_data);
    await $self->{_redis}->hset($key, $channel, $json);
    await $self->{_redis}->expire($key, $self->{group_expiry});

    return 1;
}

# Presence: untrack
async sub untrack {
    my ($self, $topic) = @_;

    my $channel = $self->{_channel_id}
        or die "untrack() requires set_channel_id() first";

    my $key = $self->_presence_key($topic);
    await $self->{_redis}->hdel($key, $channel);

    return 1;
}

# Presence: list_presence
async sub list_presence {
    my ($self, $topic) = @_;

    my $key = $self->_presence_key($topic);
    my $data_ref = await $self->{_redis}->hgetall($key);

    # hgetall may return hashref { k => v } or arrayref [k, v, ...] depending on Redis lib version
    my @result;
    if (ref $data_ref eq 'HASH') {
        for my $channel (keys %$data_ref) {
            my $presence = decode_json($data_ref->{$channel});
            push @result, $presence;
        }
    }
    elsif (ref $data_ref eq 'ARRAY') {
        my @data = @$data_ref;
        while (my ($channel, $json) = splice(@data, 0, 2)) {
            my $presence = decode_json($json);
            push @result, $presence;
        }
    }

    return @result;
}

# Delayed: send_delayed
async sub send_delayed {
    my ($self, $channel, $message, $delay_seconds) = @_;

    my $delivery_time = Time::HiRes::time() + $delay_seconds;
    my $entry = encode_json({
        type    => 'send',
        target  => $channel,
        message => $message,
        id      => rand(),  # Unique identifier to allow multiple identical messages
    });

    await $self->{_redis}->zadd($self->_delayed_key, $delivery_time, $entry);

    return 1;
}

# Delayed: publish_delayed
async sub publish_delayed {
    my ($self, $topic, $message, $delay_seconds) = @_;

    my $delivery_time = Time::HiRes::time() + $delay_seconds;
    my $entry = encode_json({
        type    => 'publish',
        target  => $topic,
        message => $message,
        id      => rand(),  # Unique identifier
    });

    await $self->{_redis}->zadd($self->_delayed_key, $delivery_time, $entry);

    return 1;
}

# Delayed: process_delayed
async sub process_delayed {
    my ($self) = @_;

    my $now = Time::HiRes::time();

    # Get all entries with score <= now
    my $entries_ref = await $self->{_redis}->zrangebyscore(
        $self->_delayed_key, '-inf', $now
    );
    my @entries = ref $entries_ref eq 'ARRAY' ? @$entries_ref : ();

    my $processed = 0;
    for my $json (@entries) {
        my $entry = decode_json($json);

        if ($entry->{type} eq 'send') {
            await $self->send($entry->{target}, $entry->{message});
        }
        elsif ($entry->{type} eq 'publish') {
            await $self->publish($entry->{target}, $entry->{message});
        }

        # Remove processed entry
        await $self->{_redis}->zrem($self->_delayed_key, $json);
        $processed++;
    }

    return $processed;
}

# History: subscribe_with_history
async sub subscribe_with_history {
    my ($self, $channel, $topic, $count, %opts) = @_;

    # Get history first (before subscribing so we get messages in order)
    my $history_key = $self->_history_key($topic);
    my $history_ref = await $self->{_redis}->lrange($history_key, -$count, -1);
    my @history = ref $history_ref eq 'ARRAY' ? @$history_ref : ();

    # Deliver history to channel
    for my $json (@history) {
        my $msg = decode_json($json);
        await $self->_deliver_to_channel($channel, $msg);
    }

    # Now subscribe (with presence if provided)
    await $self->subscribe($channel, $topic, %opts);

    return 1;
}

1;

__END__

=encoding utf-8

=head1 NAME

PAGI::Middleware::Channels::Backend::Redis - Redis-backed channel backend for multi-process use

=head1 SYNOPSIS

    use Async::Redis;
    use PAGI::Middleware::Channels::Backend::Redis;

    my $redis = Async::Redis->new(
        uri       => 'redis://localhost:6379',
        prefix    => 'myapp:channels:',
        reconnect => 1,
    );
    $redis->connect->get;

    my $backend = PAGI::Middleware::Channels::Backend::Redis->new(
        redis    => $redis,
        capacity => 100,
    );

    await $backend->send('my.channel', { type => 'greeting', msg => 'hello' });
    my $msg = await $backend->poll('my.channel');

=head1 DESCRIPTION

Redis-backed channel backend for multi-process and multi-server use.

The backend takes a configured L<Async::Redis> instance (or any
object with a compatible interface) and uses it for all Redis
operations. Connection lifecycle, reconnect, prefix, and fork-safety
are the responsibility of the passed-in client, not the backend.

Structural keys used in Redis (relative to the client's prefix):

    q:<channel>     — channel message queue (LIST)
    g:<topic>       — subscription group membership (SET)
    p:<topic>       — presence data (HASH)
    h:<topic>       — history buffer (LIST)
    pat:<channel>   — pattern subscriptions for a channel (SET)
    delayed         — delayed-delivery queue (ZSET)

=head1 CONSTRUCTOR OPTIONS

=over 4

=item redis => $async_redis

B<Required.> An L<Async::Redis> instance (or anything that ducks the
same interface). The caller owns the connection lifecycle.

=item capacity => $int

Maximum number of messages per channel queue. Default: 100.

=item expiry => $seconds

Time-to-live for messages in seconds. Default: 60.

=item group_expiry => $seconds

Time-to-live for subscription group membership. Default: 86400 (1 day).

=item max_size => $bytes

Maximum size of serialized message. Default: 1048576 (1MB).

=item history_size => $int

Number of messages to retain for history feature. Default: 0 (disabled).

=back

=head1 NOTES ON FLUSH

C<flush()> deletes B<every> key matching the Redis client's prefix,
not only the structural keys this backend writes. Use a dedicated
prefix on your L<Async::Redis> instance if you want isolation from
other code sharing the same Redis.

=cut
