# lib/PAGI/Channels/Backend/Redis.pm
package PAGI::Channels::Backend::Redis;
use strict;
use warnings;
use Future::AsyncAwait;
use Future;
use Role::Tiny::With;
use JSON::MaybeXS qw(encode_json decode_json);
use namespace::clean;

with 'PAGI::Channels::Backend';

# Defaults
use constant {
    DEFAULT_CAPACITY     => 100,
    DEFAULT_EXPIRY       => 60,
    DEFAULT_GROUP_EXPIRY => 86400,
    DEFAULT_MAX_SIZE     => 1_048_576,
    DEFAULT_HISTORY_SIZE => 0,
    DEFAULT_PREFIX       => 'pagi:',
};

sub new {
    my ($class, %args) = @_;

    my $uri = $args{uri} // 'redis://localhost:6379';

    return bless {
        uri          => $uri,
        prefix       => $args{prefix}       // DEFAULT_PREFIX,
        capacity     => $args{capacity}     // DEFAULT_CAPACITY,
        expiry       => $args{expiry}       // DEFAULT_EXPIRY,
        group_expiry => $args{group_expiry} // DEFAULT_GROUP_EXPIRY,
        max_size     => $args{max_size}     // DEFAULT_MAX_SIZE,
        history_size => $args{history_size} // DEFAULT_HISTORY_SIZE,

        _redis       => undef,
        _channel_id  => undef,
        _connected   => 0,
    }, $class;
}

async sub connect {
    my ($self) = @_;

    require Async::Redis;

    # Parse URI for host/port
    my ($host, $port) = $self->_parse_uri($self->{uri});

    $self->{_redis} = Async::Redis->new(
        host => $host,
        port => $port,
    );

    await $self->{_redis}->connect();
    $self->{_connected} = 1;

    return 1;
}

sub _parse_uri {
    my ($self, $uri) = @_;

    if ($uri =~ m{^redis://([^:]+):(\d+)}) {
        return ($1, $2);
    }
    elsif ($uri =~ m{^redis://([^/]+)}) {
        return ($1, 6379);
    }
    return ('localhost', 6379);
}

async sub disconnect {
    my ($self) = @_;

    if ($self->{_redis}) {
        $self->{_redis}->disconnect();
        $self->{_redis} = undef;
    }
    $self->{_connected} = 0;

    return 1;
}

sub connected { shift->{_connected} }

sub set_channel_id {
    my ($self, $channel_id) = @_;
    $self->{_channel_id} = $channel_id;
}

sub channel_id { shift->{_channel_id} }

# Key helpers
sub _queue_key  { shift->{prefix} . 'q:' . shift }
sub _group_key  { shift->{prefix} . 'g:' . shift }
sub _presence_key { shift->{prefix} . 'p:' . shift }
sub _history_key  { shift->{prefix} . 'h:' . shift }

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

    return 1;
}

# Unsubscribe
async sub unsubscribe {
    my ($self, $channel, $topic) = @_;

    my $key = $self->_group_key($topic);
    await $self->{_redis}->srem($key, $channel);

    return 1;
}

# Publish
async sub publish {
    my ($self, $topic, $message, %opts) = @_;

    my $exclude = $opts{exclude} // [];
    $exclude = [$exclude] unless ref $exclude eq 'ARRAY';
    my %excluded = map { $_ => 1 } @$exclude;

    my $key = $self->_group_key($topic);
    my @members = await $self->{_redis}->smembers($key);

    for my $channel (@members) {
        next if $excluded{$channel};

        # Silently drop if full
        my $qkey = $self->_queue_key($channel);
        my $len = await $self->{_redis}->llen($qkey);

        if ($len < $self->{capacity}) {
            my $json = encode_json($message);
            await $self->{_redis}->rpush($qkey, $json);
            await $self->{_redis}->expire($qkey, $self->{expiry});
        }
    }

    return 1;
}

# Flush all data
async sub flush {
    my ($self) = @_;

    # Delete all keys with our prefix
    my @keys = await $self->{_redis}->keys($self->{prefix} . '*');

    if (@keys) {
        await $self->{_redis}->del(@keys);
    }

    return 1;
}

# Cleanup channel
async sub cleanup {
    my ($self, $channel) = @_;

    # Remove queue
    await $self->{_redis}->del($self->_queue_key($channel));

    # Remove from all groups (scan for membership)
    my @group_keys = await $self->{_redis}->keys($self->{prefix} . 'g:*');
    for my $key (@group_keys) {
        await $self->{_redis}->srem($key, $channel);
    }

    return 1;
}

# Advanced features - basic stubs (implement in next tasks)
async sub psubscribe { 1 }
async sub punsubscribe { 1 }
async sub track { 1 }
async sub untrack { 1 }
async sub list_presence { [] }
async sub send_delayed { 1 }
async sub publish_delayed { 1 }
async sub subscribe_with_history { 1 }
sub process_delayed { 1 }

1;
