#!/usr/bin/env perl
# examples/chat/app.pl
# Simple chat with presence tracking
#
# Usage:
#   PAGI_CHANNELS_BACKEND=redis://localhost:6379 pagi-server app.pl

use strict;
use warnings;
use Future::AsyncAwait;
use JSON::MaybeXS qw(encode_json);

use lib 'lib';
use PAGI::Channels;

my $channels = PAGI::Channels->new(
    backend => $ENV{PAGI_CHANNELS_BACKEND} // 'redis://localhost:6379',
);

my $chat_app = async sub {
    my ($scope, $receive, $send) = @_;
    return unless ($scope->{type} // '') eq 'websocket';

    my $ch = $scope->{'pagi.channels'};
    my $my_channel = $scope->{'pagi.channel'};
    my $room = $scope->{path_params}{room} // 'general';
    my $username = $scope->{query_params}{user} // 'anonymous';

    await $send->({ type => 'websocket.accept' });

    await $ch->subscribe("chat.$room",
        presence => { user => $username }
    );

    while (1) {
        my $event = await $receive->();
        my $type = $event->{type} // '';

        if ($type eq 'websocket.receive') {
            await $ch->publish("chat.$room", {
                type => 'chat.message',
                user => $username,
                text => $event->{text},
            }, exclude => $my_channel);
        }
        elsif ($type eq 'chat.message') {
            await $send->({
                type => 'websocket.send',
                text => encode_json($event),
            });
        }
        elsif ($type eq 'presence.join') {
            await $send->({
                type => 'websocket.send',
                text => encode_json({ type => 'user_joined', user => $event->{presence}{user} }),
            });
        }
        elsif ($type eq 'presence.leave') {
            await $send->({
                type => 'websocket.send',
                text => encode_json({ type => 'user_left', user => $event->{presence}{user} }),
            });
        }
        elsif ($type eq 'websocket.disconnect') {
            last;
        }
    }
};

$channels->wrap($chat_app);
