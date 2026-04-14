#!/usr/bin/env perl
# examples/chat/app.pl
# Chat with presence tracking - demonstrates PAGI-Channels
#
# Usage:
#   cd examples/chat
#   pagi-server --workers 4 app.pl
#
# Then open http://localhost:5000 in your browser

use strict;
use warnings;
use Future::AsyncAwait;
use JSON::MaybeXS qw(decode_json);
use File::Basename qw(dirname);
use File::Spec;

use PAGI::Channels;
use PAGI::Channels::Backend::Redis;
use PAGI::WebSocket;
use PAGI::App::Router;
use PAGI::App::File;
use Async::Redis;

my $redis = Async::Redis->new(
    uri       => $ENV{PAGI_REDIS_URI} // 'redis://localhost:6379',
    prefix    => 'chat:',
    reconnect => 1,
);
# Connection happens in the lifespan.startup hook below,
# so the loop is running when we wait for it.

my $channels = PAGI::Channels->new(
    backend => PAGI::Channels::Backend::Redis->new(redis => $redis),
);

# WebSocket chat handler
async sub handle_websocket {
    my ($scope, $receive, $send) = @_;

    my $ws = PAGI::WebSocket->new($scope, $receive, $send);
    my $ch = $scope->{'pagi.channels'};
    my $my_channel = $scope->{'pagi.channel'};

    # Room from path param, username from query string
    my $room = $ws->path_param('room') // 'general';
    my $username = $ws->query('user') // 'anonymous';

    await $ws->accept;

    # Subscribe to room with presence tracking
    await $ch->subscribe("chat.$room",
        presence => { user => $username }
    );

    # Send current users to the new user
    my @users = await $ch->list_presence("chat.$room");
    await $ws->send_json({
        type  => 'users',
        users => \@users,
        room  => $room,
    });

    # Main event loop - handle both WebSocket and channel events
    while (my $event = await $receive->()) {
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
            await $ws->send_json($event);
        }
        elsif ($type eq 'presence.join') {
            await $ws->send_json({
                type => 'user_joined',
                user => $event->{presence}{user},
            });
        }
        elsif ($type eq 'presence.leave') {
            await $ws->send_json({
                type => 'user_left',
                user => $event->{presence}{user},
            });
        }
        elsif ($type eq 'websocket.disconnect') {
            last;
        }
    }
}

# Build router
my $router = PAGI::App::Router->new;

# WebSocket route with room as path parameter
$router->websocket('/ws/chat/:room' => \&handle_websocket);

# Static files fallback
$router->mount('/' => PAGI::App::File->new(
    root => File::Spec->catdir(dirname(__FILE__), 'public'),
)->to_app);

# Wrap with channels
my $channels_app = $channels->wrap($router->to_app);

# Lifespan-aware wrapper - clears stale presence on startup
my $app = async sub {
    my ($scope, $receive, $send) = @_;

    if ($scope->{type} eq 'lifespan') {
        while (my $event = await $receive->()) {
            if ($event->{type} eq 'lifespan.startup') {
                await $redis->connect;
                await $channels->backend->flush;
                await $send->({ type => 'lifespan.startup.complete' });
            }
            elsif ($event->{type} eq 'lifespan.shutdown') {
                await $send->({ type => 'lifespan.shutdown.complete' });
                last;
            }
        }
        return;
    }

    await $channels_app->($scope, $receive, $send);
};

$app;
