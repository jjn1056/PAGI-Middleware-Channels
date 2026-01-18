#!/usr/bin/env perl
# examples/chat-with-workers/worker.pl
# Background worker for processing tasks
#
# This is a standalone worker process, not a PAGI app.
# It creates its own event loop since it doesn't run under PAGI::Server.
#
# Usage:
#   PAGI_CHANNELS_BACKEND=redis://localhost:6379 perl worker.pl

use strict;
use warnings;
use Future::AsyncAwait;
use IO::Async::Loop;
use Future::IO::Impl::IOAsync;
use Future::IO;

use lib 'lib';
use PAGI::Channels;

# Standalone worker - create our own event loop
my $loop = IO::Async::Loop->new;
Future::IO::Impl::IOAsync->APPLY($loop);

my $channels = PAGI::Channels->new(
    backend => $ENV{PAGI_CHANNELS_BACKEND} // 'redis://localhost:6379',
);

my $worker_id = $$;

async sub run_worker {
    my $backend = $channels->backend;

    # Connect if Redis backend
    if ($backend->can('connect')) {
        await $backend->connect();
    }

    # Track worker presence
    $backend->set_channel_id("worker.$worker_id");
    await $backend->track('workers.pool', {
        worker_id => $worker_id,
        started   => time(),
        status    => 'idle',
    });

    print "Worker $worker_id started\n";

    while (1) {
        # Poll for tasks
        my $task = $backend->poll('worker.pool');

        if ($task && ($task->{type} // '') eq 'task.process_image') {
            print "Processing image: $task->{image_id}\n";

            # Update presence to show busy
            await $backend->track('workers.pool', {
                worker_id => $worker_id,
                status    => 'processing',
                task      => $task->{image_id},
            });

            # Simulate work
            await Future::IO->sleep(2);

            # Send result back
            await $backend->send($task->{reply_to}, {
                type     => 'task.result',
                image_id => $task->{image_id},
                status   => 'processed',
                url      => "https://cdn.example.com/$task->{image_id}.jpg",
            });

            # Update presence back to idle
            await $backend->track('workers.pool', {
                worker_id => $worker_id,
                status    => 'idle',
            });
        }
        else {
            # No task, wait a bit
            await Future::IO->sleep(0.1);
        }
    }
}

run_worker()->get;
