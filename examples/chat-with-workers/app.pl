#!/usr/bin/env perl
# examples/chat-with-workers/app.pl
# Chat application with worker pool for background tasks
#
# Usage with PAGI::Server:
#   PAGI_CHANNELS_BACKEND=redis://localhost:6379 pagi-server app.pl
#
# The server provides the event loop - this file just defines the app.

use strict;
use warnings;
use Future::AsyncAwait;
use JSON::MaybeXS qw(encode_json decode_json);

use lib 'lib';
use PAGI::Channels;

my $channels = PAGI::Channels->new(
    backend => $ENV{PAGI_CHANNELS_BACKEND} // 'redis://localhost:6379',
);

# Chat room handler
my $chat_app = async sub {
    my ($scope, $receive, $send) = @_;
    return unless ($scope->{type} // '') eq 'websocket';

    my $ch = $scope->{'pagi.channels'};
    my $my_channel = $scope->{'pagi.channel'};
    my $room = $scope->{path_params}{room} // 'general';
    my $username = $scope->{query_params}{user} // 'anonymous';

    # Accept WebSocket
    await $send->({ type => 'websocket.accept' });

    # Subscribe with presence
    await $ch->subscribe("chat.$room",
        presence => { user => $username, channel => $my_channel }
    );

    # Main event loop - errors propagate to PAGI::Server
    while (1) {
        my $event = await $receive->();

        if (($event->{type} // '') eq 'websocket.receive') {
            my $data = decode_json($event->{text});

            if ($data->{action} eq 'message') {
                # Broadcast to room
                await $ch->publish("chat.$room", {
                    type => 'chat.message',
                    user => $username,
                    text => $data->{text},
                    timestamp => time(),
                }, exclude => $my_channel);
            }
            elsif ($data->{action} eq 'process_image') {
                # Send to worker pool
                await $ch->send('worker.pool', {
                    type     => 'task.process_image',
                    image_id => $data->{image_id},
                    reply_to => $my_channel,
                });
            }
        }
        elsif (($event->{type} // '') eq 'chat.message') {
            # Forward to client
            await $send->({
                type => 'websocket.send',
                text => encode_json($event),
            });
        }
        elsif (($event->{type} // '') eq 'presence.join') {
            await $send->({
                type => 'websocket.send',
                text => encode_json({
                    type => 'user_joined',
                    user => $event->{presence}{user},
                }),
            });
        }
        elsif (($event->{type} // '') eq 'presence.leave') {
            await $send->({
                type => 'websocket.send',
                text => encode_json({
                    type => 'user_left',
                    user => $event->{presence}{user},
                }),
            });
        }
        elsif (($event->{type} // '') eq 'task.result') {
            await $send->({
                type => 'websocket.send',
                text => encode_json($event),
            });
        }
        elsif (($event->{type} // '') eq 'websocket.disconnect') {
            last;
        }
    }

    # Cleanup happens automatically via wrap()
};

my $app = $channels->wrap($chat_app);

# Export the app for PAGI::Server
# Run with: pagi-server examples/chat-with-workers/app.pl
$app;
