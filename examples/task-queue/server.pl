#!/usr/bin/env perl
# examples/task-queue/server.pl
# Task queue with real-time progress updates
#
# This is a standalone CLI tool (worker/demo), not a PAGI app.
# It creates its own event loop since it doesn't run under PAGI::Server.
#
# Usage:
#   perl server.pl worker  - Run as background worker
#   perl server.pl demo    - Submit task and watch progress

use strict;
use warnings;
use Future::AsyncAwait;
use IO::Async::Loop;
use Future::IO::Impl::IOAsync;
use Future::IO;
use JSON::MaybeXS qw(encode_json decode_json);

use lib 'lib';
use PAGI::Middleware::Channels;
use PAGI::Middleware::Channels::Backend::Redis;
use Async::Redis;

# Standalone tool - create our own event loop
my $loop = IO::Async::Loop->new;
Future::IO::Impl::IOAsync->APPLY($loop);

my $redis = Async::Redis->new(
    uri       => $ENV{PAGI_REDIS_URI} // 'redis://localhost:6379',
    prefix    => 'task-queue:',
    reconnect => 1,
);
$redis->connect->get;

my $channels = PAGI::Middleware::Channels->new(
    backend => PAGI::Middleware::Channels::Backend::Redis->new(redis => $redis),
);

my $task_counter = 0;

# HTTP-style task submission handler
async sub submit_task {
    my ($backend, $task_type, $data) = @_;

    $task_counter++;
    my $task_id = sprintf("%d.%d", $$, $task_counter);

    # Queue the task
    await $backend->send('tasks.queue', {
        type    => $task_type,
        task_id => $task_id,
        data    => $data,
    });

    return $task_id;
}

# Worker that processes tasks
async sub run_worker {
    my $backend = $channels->backend;

    # Connect if Redis backend
    if ($backend->can('connect')) {
        await $backend->connect();
    }

    my $worker_id = $$;
    print "Worker $worker_id started\n";

    while (1) {
        my $task = $backend->poll('tasks.queue');

        if ($task && $task->{type}) {
            my $task_id = $task->{task_id};
            print "Worker $worker_id processing task $task_id\n";

            # Simulate progress updates
            for my $percent (0, 25, 50, 75, 100) {
                await $backend->publish("task.$task_id.progress", {
                    type    => 'progress',
                    task_id => $task_id,
                    percent => $percent,
                });

                await Future::IO->sleep(0.5) if $percent < 100;
            }

            # Send completion
            await $backend->publish("task.$task_id.**", {
                type    => 'complete',
                task_id => $task_id,
                result  => { processed => 1, worker => $worker_id },
            });

            print "Worker $worker_id completed task $task_id\n";
        }
        else {
            await Future::IO->sleep(0.1);
        }
    }
}

# Demo: Submit a task and watch progress
async sub demo {
    my $backend = $channels->backend;

    # Connect if Redis backend
    if ($backend->can('connect')) {
        await $backend->connect();
    }

    # Create a client channel
    $backend->set_channel_id('demo.client');

    # Submit a task
    my $task_id = await submit_task($backend, 'process_data', { items => 100 });
    print "Submitted task: $task_id\n";

    # Subscribe to progress updates
    await $backend->psubscribe('demo.client', "task.$task_id.**");

    # Watch for updates
    for my $i (1..20) {
        my $msg = $backend->poll('demo.client');
        if ($msg) {
            if ($msg->{type} eq 'progress') {
                print "Progress: $msg->{percent}%\n";
            }
            elsif ($msg->{type} eq 'complete') {
                print "Task complete! Result: " . encode_json($msg->{result}) . "\n";
                last;
            }
        }
        await Future::IO->sleep(0.2);
    }

    await $backend->punsubscribe('demo.client');
}

if (@ARGV && $ARGV[0] eq 'worker') {
    run_worker()->get;
}
elsif (@ARGV && $ARGV[0] eq 'demo') {
    demo()->get;
}
else {
    print "Usage:\n";
    print "  $0 worker  - Run as worker\n";
    print "  $0 demo    - Run demo (submit task and watch progress)\n";
}
