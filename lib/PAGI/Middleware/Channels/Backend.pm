package PAGI::Middleware::Channels::Backend;
use strict;
use warnings;
use Carp ();

# Abstract base class for channel-layer backends. Subclasses MUST
# override every method below. Each default implementation croaks so
# missing overrides surface immediately on first call.

# Shared default configuration values. Subclasses inherit these.
use constant {
    DEFAULT_CAPACITY     => 100,
    DEFAULT_EXPIRY       => 60,
    DEFAULT_GROUP_EXPIRY => 86400,
    DEFAULT_MAX_SIZE     => 1_048_576,
    DEFAULT_HISTORY_SIZE => 0,
};

sub new {
    my ($class, %args) = @_;
    Carp::croak("$class is abstract and cannot be instantiated directly")
        if $class eq __PACKAGE__;

    return bless {
        capacity     => $args{capacity}     // DEFAULT_CAPACITY,
        expiry       => $args{expiry}       // DEFAULT_EXPIRY,
        group_expiry => $args{group_expiry} // DEFAULT_GROUP_EXPIRY,
        max_size     => $args{max_size}     // DEFAULT_MAX_SIZE,
        history_size => $args{history_size} // DEFAULT_HISTORY_SIZE,
        # subclass-specific args remain in %args; subclasses store what they need
        %args,
    }, $class;
}

# Validation helpers. Backends MUST call these from their public entry
# methods. A backend with stricter substrate constraints (e.g., a Pusher
# backend with its own naming rules) MAY override _validate_channel to
# add more checks; it MUST NOT loosen them.

sub _validate_channel {
    my ($self, $channel) = @_;
    die "InvalidChannelName: empty"     unless defined $channel && length $channel;
    die "InvalidChannelName: too long"  if length $channel > 100;
    die "InvalidChannelName: bad chars" unless $channel =~ /^[\w.\-:]+$/;
}

# Topics use the same naming rules as channels.
sub _validate_topic { goto &_validate_channel }

sub _validate_message {
    my ($self, $message) = @_;
    die "InvalidMessage: not a hashref" unless ref $message eq 'HASH';
    die "InvalidMessage: missing type"  unless defined $message->{type};

    if ($self->{max_size}) {
        # Lazy require: keeps this base JSON-free when max_size is disabled,
        # so Memory-only users don't pay for JSON::MaybeXS.
        require JSON::MaybeXS;
        my $size = length(JSON::MaybeXS::encode_json($message));
        die "MessageTooLarge: $size bytes exceeds max_size $self->{max_size}"
            if $size > $self->{max_size};
    }
}

# Glob-pattern to regex compilation. Used by PatternSubs-capable backends
# in both psubscribe (compile) and publish (match). Lives on the base so
# any backend can call it without depending on the PatternSubs role.
#
# Pattern syntax:
#   *  matches exactly one segment (no dots)
#   ** matches zero or more segments (including dots)
#   When ** follows a dot, the dot is optional, so "foo.**" matches
#   "foo", "foo.bar", "foo.bar.baz".
sub _pattern_to_regex {
    my ($self, $pattern) = @_;
    my $regex = quotemeta($pattern);
    $regex =~ s/\\\.\\\*\\\*/(\\..*)?\$/g;   # ".**" - dot optional
    $regex =~ s/\\\*\\\*/.*/g;               # "**" anywhere else
    $regex =~ s/\\\*/[^.]+/g;                # "*" - one segment
    return qr/^$regex$/;
}

# Normalize the publish-time `exclude` option to a hash for fast lookup.
# Accepts undef, scalar, or arrayref.
sub _normalize_exclude {
    my ($self, $exclude) = @_;
    return {} unless defined $exclude;
    $exclude = [$exclude] unless ref $exclude eq 'ARRAY';
    return { map { $_ => 1 } @$exclude };
}

# Construct the standard presence-event hashref used in subscribe-with-presence
# and unsubscribe-with-presence broadcasts.
sub _make_presence_event {
    my ($self, $topic, $type, $presence_data) = @_;
    return {
        type     => $type,             # 'presence.join' or 'presence.leave'
        topic    => $topic,
        presence => $presence_data,
    };
}

my @ABSTRACT = qw(
    send poll next_message
    subscribe unsubscribe publish
    flush cleanup
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
implementations croak. The base class provides shared configuration
constants and a C<new> constructor that populates common config keys.

=head1 SHARED CONFIGURATION

The following configuration keys are accepted by C<new> and pre-populated
in every backend instance. Subclasses read them via C<$self-E<gt>{key}>.

=over 4

=item capacity => $int

Maximum number of messages per channel queue. Default: 100.

=item expiry => $seconds

Time-to-live for messages in seconds. Default: 60.

=item group_expiry => $seconds

Time-to-live for subscription group membership. Default: 86400 (1 day).

=item max_size => $bytes

Maximum size of a serialized message. Default: 1_048_576 (1 MB).

=item history_size => $int

Number of messages to retain for the history feature. Default: 0 (disabled).

=back

=head1 REQUIRED METHODS

Subclasses must implement these eight core methods. All other behavior is
provided via capability roles (see L</CAPABILITY ROLES>).

=over 4

=item send($channel, $message) -> Future

=item poll($channel) -> Future($message | undef)

=item next_message($channel) -> Future($message)

Waits until a message is available on the channel, then returns it.
Unlike C<poll>, which returns C<undef> immediately when the queue is
empty, C<next_message> blocks (asynchronously) until a message arrives.
The returned Future may be cancelled to abort the wait.

=item subscribe($channel, $topic, %opts) -> Future

=item unsubscribe($channel, $topic) -> Future

=item publish($topic, $message, %opts) -> Future

=item flush() -> Future

=item cleanup($channel) -> Future

=back

=head1 CAPABILITY ROLES

Optional behavior is provided by capability roles in
C<PAGI::Middleware::Channels::Backend::Role::*>:

=over 4

=item L<Role::Presence|PAGI::Middleware::Channels::Backend::Role::Presence> - track, untrack, list_presence, count_presence, scan_presence

=item L<Role::History|PAGI::Middleware::Channels::Backend::Role::History> - subscribe_with_history, _record_history, read_history

=item L<Role::Delayed|PAGI::Middleware::Channels::Backend::Role::Delayed> - send_delayed, publish_delayed, schedule_delayed, process_delayed

=item L<Role::PatternSubs|PAGI::Middleware::Channels::Backend::Role::PatternSubs> - psubscribe, punsubscribe

=back

Backends declare capabilities via C<< with 'PAGI::Middleware::Channels::Backend::Role::Presence' >> etc.

=head1 SHARED UTILITIES

Protected methods available to all subclasses:

=over 4

=item _validate_channel($name) — dies with C<InvalidChannelName> if invalid

=item _validate_topic($name) — alias for _validate_channel (same rules)

=item _validate_message($msg) — dies with C<InvalidMessage> or C<MessageTooLarge>

=item _pattern_to_regex($pattern) — compiles glob pattern to regex

=item _normalize_exclude($exclude) — normalizes exclude option to hashref

=item _make_presence_event($topic, $type, $data) — builds presence event hashref

=back

=cut
