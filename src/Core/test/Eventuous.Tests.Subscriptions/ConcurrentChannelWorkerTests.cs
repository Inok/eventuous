// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Collections.Concurrent;
using System.Threading.Channels;
using Eventuous.Subscriptions.Channels;
using Shouldly;

namespace Eventuous.Tests.Subscriptions;

/// <summary>
/// Pins the shutdown contract of the reader loop behind <c>AsyncHandlingFilter</c>: an idle reader is woken
/// by the channel completing rather than by the worker's token, so each of these would hang or lose
/// elements if the loop stopped honouring one of the two.
/// </summary>
public class ConcurrentChannelWorkerTests {
    static readonly TimeSpan Prompt = TimeSpan.FromSeconds(2);

    [Test]
    [Arguments(1)]
    [Arguments(4)]
    public async Task Idle_worker_stops_promptly_on_dispose(int readers, CancellationToken ct) {
        var worker = new ConcurrentChannelWorker<int>(CreateChannel(readers), (_, _) => default, readers);

        // Long enough for every reader to be parked on the empty channel.
        await Task.Delay(100, ct);

        // Well inside the ten seconds a reader that missed the completion would take to be cancelled.
        await worker.DisposeAsync().AsTask().WaitAsync(Prompt, ct);
    }

    [Test]
    public async Task Idle_worker_stops_promptly_after_processing(CancellationToken ct) {
        var processed = 0;

        var worker = new ConcurrentChannelWorker<int>(
            CreateChannel(1),
            (_, _) => {
                Interlocked.Increment(ref processed);

                return default;
            },
            1
        );

        for (var i = 0; i < 3; i++) {
            (await worker.Write(i, ct)).ShouldBeTrue();
            var expected = i + 1;
            (await Wait.Until(() => Volatile.Read(ref processed) == expected, Prompt)).ShouldBeTrue("each element should be picked up from an idle channel");
        }

        await worker.DisposeAsync().AsTask().WaitAsync(Prompt, ct);
    }

    [Test]
    public async Task Queued_elements_are_drained_on_graceful_dispose(CancellationToken ct) {
        const int count = 5;

        var gate      = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processed = new ConcurrentQueue<int>();
        var cancelled = 0;

        var worker = new ConcurrentChannelWorker<int>(
            CreateChannel(1),
            async (element, token) => {
                await gate.Task;
                if (token.IsCancellationRequested) Interlocked.Increment(ref cancelled);
                processed.Enqueue(element);
            },
            1
        );

        for (var i = 0; i < count; i++) (await worker.Write(i, ct)).ShouldBeTrue();

        var disposing = worker.DisposeAsync().AsTask();

        await Task.Delay(100, ct);
        disposing.IsCompleted.ShouldBeFalse("dispose must wait for the elements queued before it");
        (await worker.Write(count, ct)).ShouldBeFalse("a stopping worker takes nothing new");

        gate.SetResult();
        await disposing.WaitAsync(Prompt, ct);

        processed.ShouldBe(Enumerable.Range(0, count));
        cancelled.ShouldBe(0, "a drain inside the deadline runs on a live token");
    }

    [Test]
    public async Task Elements_from_several_writers_are_all_processed_by_several_readers(CancellationToken ct) {
        const int readers   = 4;
        const int writers   = 4;
        const int perWriter = 500;

        var processed = new ConcurrentBag<int>();

        var worker = new ConcurrentChannelWorker<int>(
            CreateChannel(readers),
            async (element, _) => {
                // Yields now and then, so readers go back to the channel both synchronously and not.
                if (element % 7 == 0) await Task.Yield();
                processed.Add(element);
            },
            readers
        );

        await Task.WhenAll(
            Enumerable.Range(0, writers)
                .Select(w => Task.Run(
                        async () => {
                            for (var i = 0; i < perWriter; i++) (await worker.Write(w * perWriter + i, ct)).ShouldBeTrue();
                        },
                        ct
                    )
                )
        );

        await worker.DisposeAsync().AsTask().WaitAsync(Prompt, ct);

        processed.Order().ShouldBe(Enumerable.Range(0, writers * perWriter));
    }

    /// <summary>
    /// The drain is bounded: once the deadline cancels the worker's token, what's still queued is left
    /// unprocessed (for the handling filter, never acknowledged and so redelivered) rather than handed
    /// to the processor with a dead token, and dispose still completes.
    /// </summary>
    [Test]
    public async Task Elements_still_queued_when_the_drain_deadline_passes_are_left_unprocessed(CancellationToken ct) {
        var started = new ConcurrentQueue<int>();

        var worker = new ConcurrentChannelWorker<int>(
            CreateChannel(1),
            async (element, token) => {
                started.Enqueue(element);

                // Returns normally once cancelled, so it's the loop that has to notice the token.
                try {
                    await Task.Delay(Timeout.Infinite, token);
                } catch (OperationCanceledException) { }
            },
            1
        );

        for (var i = 0; i < 3; i++) (await worker.Write(i, ct)).ShouldBeTrue();

        // Ten seconds of drain deadline, plus room.
        await worker.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(20), ct);

        started.ShouldBe([0]);
    }

    static Channel<int> CreateChannel(int readers)
        => Channel.CreateBounded<int>(new BoundedChannelOptions(10 * readers) { SingleReader = readers == 1 });
}
