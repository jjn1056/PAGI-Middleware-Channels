#!/usr/bin/env perl
# examples/chat/app.pl
# Chat with presence tracking - demonstrates PAGI-Channels
#
# Usage:
#   cd examples/chat
#   PAGI_CHANNELS_BACKEND=redis://localhost:6379 pagi-server --workers 4 app.pl
#
# Then open http://localhost:5000 in your browser

use strict;
use warnings;
use Future::AsyncAwait;
use JSON::MaybeXS qw(encode_json decode_json);
use File::Basename qw(dirname);
use File::Spec;

use PAGI::Channels;
use PAGI::App::File;

my $channels = PAGI::Channels->new(
    backend => $ENV{PAGI_CHANNELS_BACKEND} // 'redis://localhost:6379',
);

# Static file server for public/ directory
my $static = PAGI::App::File->new(
    root => File::Spec->catdir(dirname(__FILE__), 'public'),
)->to_app;

# WebSocket chat handler
async sub handle_websocket {
    my ($scope, $receive, $send) = @_;

    my $ch = $scope->{'pagi.channels'};
    my $my_channel = $scope->{'pagi.channel'};

    # Parse query string (PAGI provides query_string, not query_params for WebSocket)
    my $qs = $scope->{query_string} // '';
    my ($username) = $qs =~ /(?:^|&)user=([^&]*)/;
    my ($room) = $qs =~ /(?:^|&)room=([^&]*)/;
    $username = $username // 'anonymous';
    $room = $room // 'general';

    await $send->({ type => 'websocket.accept' });

    # Subscribe to room with presence tracking
    await $ch->subscribe("chat.$room",
        presence => { user => $username }
    );

    # Send current users to the new user
    my @users = await $ch->list_presence("chat.$room");
    await $send->({
        type => 'websocket.send',
        text => encode_json({
            type  => 'users',
            users => \@users,
            room  => $room,
        }),
    });

    while (1) {
        my $event = await $receive->();
        my $type = $event->{type} // '';

        if ($type eq 'websocket.receive') {
            # Parse incoming message
            my $msg = eval { decode_json($event->{text}) } // { text => $event->{text} };

            # Publish to room (excluding sender)
            await $ch->publish("chat.$room", {
                type => 'chat.message',
                user => $username,
                text => $msg->{text} // $event->{text},
            }, exclude => $my_channel);
        }
        elsif ($type eq 'chat.message') {
            # Forward channel message to WebSocket
            await $send->({
                type => 'websocket.send',
                text => encode_json($event),
            });
        }
        elsif ($type eq 'presence.join') {
            await $send->({
                type => 'websocket.send',
                text => encode_json({
                    type => 'user_joined',
                    user => $event->{presence}{user},
                }),
            });
        }
        elsif ($type eq 'presence.leave') {
            await $send->({
                type => 'websocket.send',
                text => encode_json({
                    type => 'user_left',
                    user => $event->{presence}{user},
                }),
            });
        }
        elsif ($type eq 'websocket.disconnect') {
            last;
        }
    }
}

# Main app - wrap with channels
my $app = $channels->wrap(async sub {
    my ($scope, $receive, $send) = @_;
    my $type = $scope->{type} // '';
    my $path = $scope->{path} // '/';

    if ($type eq 'websocket' && $path =~ m{^/ws/chat(?:/|$)}) {
        return await handle_websocket($scope, $receive, $send);
    }

    if ($type eq 'http') {
        return await $static->($scope, $receive, $send);
    }
});

$app;
