#!/usr/bin/env perl
# examples/chat/app.pl
# Chat with presence tracking - demonstrates PAGI-Channels
#
# Usage:
#   cd examples/chat
#   PAGI_CHANNELS_BACKEND=redis://localhost:6379 pagi-server --workers 4 app.pl
#
# Then open http://localhost:8000 in your browser

use strict;
use warnings;
use Future::AsyncAwait;
use JSON::MaybeXS qw(encode_json decode_json);
use File::Basename qw(dirname);
use File::Spec;

use lib 'lib';
use PAGI::Channels;

my $channels = PAGI::Channels->new(
    backend => $ENV{PAGI_CHANNELS_BACKEND} // 'redis://localhost:6379',
);

# Get the public directory path
my $PUBLIC_DIR = File::Spec->catdir(dirname(__FILE__), 'public');

# MIME types for static files
my %MIME_TYPES = (
    html => 'text/html; charset=utf-8',
    css  => 'text/css; charset=utf-8',
    js   => 'application/javascript; charset=utf-8',
    json => 'application/json; charset=utf-8',
    png  => 'image/png',
    ico  => 'image/x-icon',
);

# Static file handler
async sub serve_static {
    my ($scope, $receive, $send, $path) = @_;

    $path = '/index.html' if $path eq '/';
    $path =~ s/\.\.//g;
    $path =~ s|//+|/|g;

    my $file_path = File::Spec->catfile($PUBLIC_DIR, $path);

    unless (-f $file_path && -r $file_path) {
        return await send_response($send, 404, 'text/plain', 'Not found');
    }

    my ($ext) = $file_path =~ /\.(\w+)$/;
    my $content_type = $MIME_TYPES{lc($ext // '')} // 'application/octet-stream';

    open my $fh, '<:raw', $file_path or return await send_response($send, 500, 'text/plain', 'Error');
    local $/;
    my $content = <$fh>;
    close $fh;

    await send_response($send, 200, $content_type, $content);
}

async sub send_response {
    my ($send, $status, $content_type, $body) = @_;

    await $send->({
        type    => 'http.response.start',
        status  => $status,
        headers => [
            ['content-type', $content_type],
            ['content-length', length($body)],
        ],
    });
    await $send->({
        type => 'http.response.body',
        body => $body,
    });
}

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
        return await serve_static($scope, $receive, $send, $path);
    }
});

$app;
