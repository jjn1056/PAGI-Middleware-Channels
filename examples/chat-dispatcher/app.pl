#!/usr/bin/env perl
# examples/chat-dispatcher/app.pl
#
# Same chat as examples/chat/app.pl, rewritten on top of the
# PAGI::Context event dispatcher (on/run/on_error) instead of a hand-
# rolled while/elsif loop over the raw $receive coderef.
#
# Compare with examples/chat/app.pl side-by-side to see what the
# dispatcher and Context delegation buy in real handler code.
#
# Usage:
#   cd examples/chat-dispatcher
#   pagi-server --workers 4 app.pl

use strict;
use warnings;
use Future::AsyncAwait;
use JSON::MaybeXS qw(decode_json);
use File::Basename qw(dirname);
use File::Spec;

use PAGI::Middleware::Channels;
use PAGI::Channel;
use PAGI::Middleware::Channels::Backend::Redis;
use PAGI::Context;
use PAGI::App::Router;
use PAGI::App::File;
use Async::Redis;

my $redis = Async::Redis->new(
    uri       => $ENV{PAGI_REDIS_URI} // 'redis://localhost:6379',
    prefix    => 'chat:',
    reconnect => 1,
);

my $channels = PAGI::Middleware::Channels->new(
    backend => PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis),
);

# WebSocket chat handler — dispatcher style.
async sub handle_websocket {
    my ($scope, $receive, $send) = @_;

    my $ctx        = PAGI::Context->new($scope, $receive, $send);
    my $ch         = PAGI::Channel->from($ctx);
    my $my_channel = $ch->channel_name;

    my $room     = $ctx->path_param('room', strict => 0) // 'general';
    my $username = $ctx->query('user') // 'anonymous';

    await $ctx->accept;

    await $ch->subscribe("chat.$room",
        presence => { user => $username }
    );

    # Send the current roster to the new arrival.
    my @users = await $ch->list_presence("chat.$room");
    await $ctx->send_json({
        type  => 'users',
        users => \@users,
        room  => $room,
    });

    await $ctx
        ->on('websocket.receive' => async sub {
            my ($ctx, $event) = @_;
            my $msg = eval { decode_json($event->{text}) }
                  // { text => $event->{text} };

            await $ch->publish("chat.$room", {
                type => 'chat.message',
                user => $username,
                text => $msg->{text} // $event->{text},
            }, exclude => $my_channel);
        })
        ->on('chat.message' => async sub {
            my ($ctx, $event) = @_;
            await $ctx->send_json($event);
        })
        ->on('presence.join' => async sub {
            my ($ctx, $event) = @_;
            await $ctx->send_json({
                type => 'user_joined',
                user => $event->{presence}{user},
            });
        })
        ->on('presence.leave' => async sub {
            my ($ctx, $event) = @_;
            await $ctx->send_json({
                type => 'user_left',
                user => $event->{presence}{user},
            });
        })
        ->on_error(sub {
            my ($ctx, $err, $source) = @_;
            warn "[chat $source] $err";
        })
        ->run;
}

my $router = PAGI::App::Router->new;
$router->websocket('/ws/chat/:room' => \&handle_websocket);
$router->mount('/' => PAGI::App::File->new(
    root => File::Spec->catdir(dirname(__FILE__), 'public'),
)->to_app);

my $channels_app = $channels->wrap($router->to_app);

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
