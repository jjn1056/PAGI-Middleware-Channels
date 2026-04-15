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
