// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Collections.Concurrent;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Checkpoints;
using Eventuous.Subscriptions.Context;
using Eventuous.Subscriptions.Filters;
using Eventuous.Tools;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;

namespace Eventuous.Tests.Subscriptions;

/// <summary>
/// A context without a payload (empty data, unknown type, failed deserialization, a checkpoint-reached
/// marker) is never handled, but it still has to be accounted for: marked ignored and, where positions
/// are checkpointed, committed — a position that's never committed is a gap the checkpoint can't cross.
/// </summary>
public class PayloadlessContextTests {
    const string Id = "payload-less";

    [Test]
    public async Task Checkpointed_subscription_ignores_it_and_commits_its_position(CancellationToken ct) {
        var (subscription, handler, committed) = await StartCheckpointed(ct);

        var context = subscription.CreateContext(0, null);
        await subscription.Deliver(context);

        handler.Positions.ShouldBeEmpty("a context without a payload must not reach the handler");
        ShouldBeIgnoredBySubscription(context);
        context.LogContext.ShouldNotBeNull("a context delivered without a log context gets the subscription's");

        (await Wait.Until(() => committed.Contains((ulong?)0), TimeSpan.FromSeconds(5)))
            .ShouldBeTrue("the position of an ignored context should still be committed");

        await subscription.Unsubscribe(_ => { }, ct);
    }

    /// <summary>
    /// Sequences are contiguous per run, so if the middle one were never committed the checkpoint would
    /// stay parked at the first event however many followed.
    /// </summary>
    [Test]
    public async Task Checkpoint_advances_past_one_sitting_between_two_events(CancellationToken ct) {
        var (subscription, handler, committed) = await StartCheckpointed(ct);

        var first  = subscription.CreateContext(0, new { Position = 0 });
        var middle = subscription.CreateContext(1, null);
        var last   = subscription.CreateContext(2, new { Position = 2 });

        await subscription.Deliver(first);
        await subscription.Deliver(middle);
        await subscription.Deliver(last);

        (await Wait.Until(() => committed.Contains((ulong?)2), TimeSpan.FromSeconds(5)))
            .ShouldBeTrue($"the checkpoint should reach the last event, but only got to [{string.Join(", ", committed)}]");

        handler.Positions.ShouldBe([0UL, 2UL]);
        ShouldBeIgnoredBySubscription(middle);
        first.WasIgnored().ShouldBeFalse();
        last.WasIgnored().ShouldBeFalse();

        await subscription.Unsubscribe(_ => { }, ct);
    }

    [Test]
    public async Task Plain_subscription_ignores_it_without_entering_the_pipe() {
        var filter  = new CountingFilter();
        var handler = new RecordingHandler();
        var logs    = new CapturingLoggerFactory(LogLevel.Trace);

        await using var subscription = new PlainSubscription(new ConsumePipe().AddFilterFirst(filter).AddDefaultConsumer(handler), loggerFactory: logs);

        var context = CreateContext(0, 0, null);
        await subscription.Deliver(context);

        filter.Seen.ShouldBe(0, "a context without a payload must not enter the pipe");
        handler.Positions.ShouldBeEmpty();
        ShouldBeIgnoredBySubscription(context);
        logs.Contains("Received TestEvent").ShouldBeTrue();
        logs.Contains("payload-less ignored TestEvent").ShouldBeTrue();

        var real = CreateContext(1, 1, new { Position = 1 });
        await subscription.Deliver(real);

        filter.Seen.ShouldBe(1);
        handler.Positions.ShouldBe([1UL]);
    }

    [Test]
    public async Task Plain_subscription_acknowledges_one_that_carries_its_own_acknowledgement() {
        var handler = new RecordingHandler();
        var acked   = new List<IMessageConsumeContext>();

        await using var subscription = new PlainSubscription(new ConsumePipe().AddDefaultConsumer(handler));

        var context = new AsyncConsumeContext(
            CreateContext(0, 0, null),
            ctx => {
                acked.Add(ctx);

                return default;
            },
            (_, e) => throw e
        );

        await subscription.Deliver(context);

        acked.ShouldBe([context]);
        handler.Positions.ShouldBeEmpty();
        ShouldBeIgnoredBySubscription(context);
    }

    /// <summary>
    /// Pins what happens today rather than what reads as intended: the failure is reported under the
    /// subscription's name, which already holds the ignored result, and results keep one entry per name.
    /// So the failure is logged but never recorded, and <c>ThrowOnError</c> has nothing to throw.
    /// </summary>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Failed_acknowledgement_is_logged_and_leaves_the_context_ignored(bool throwOnError) {
        var logs = new CapturingLoggerFactory();

        await using var subscription = new PlainSubscription(new ConsumePipe().AddDefaultConsumer(new RecordingHandler()), throwOnError, logs);

        var failure = new InvalidOperationException("ack failed");
        var context = new AsyncConsumeContext(CreateContext(0, 0, null), _ => throw failure, (_, e) => throw e);

        await subscription.Deliver(context);

        context.HasFailed().ShouldBeFalse();
        ShouldBeIgnoredBySubscription(context);
        logs.Contains("Message handling failed at payload-less").ShouldBeTrue();
        logs.Contains("ack failed").ShouldBeTrue();
    }

    /// <summary>
    /// An acknowledgement cancelled by the context's own token means the subscription is stopping, which
    /// isn't a failure of the message.
    /// </summary>
    [Test]
    public async Task Acknowledgement_cancelled_by_shutdown_is_not_a_failure() {
        using var cts = new CancellationTokenSource();

        await using var subscription = new PlainSubscription(new ConsumePipe().AddDefaultConsumer(new RecordingHandler()), true);

        var inner = CreateContext(0, 0, null);
        inner.CancellationToken = cts.Token;

        var context = new AsyncConsumeContext(
            inner,
            _ => {
                cts.Cancel();

                throw new OperationCanceledException(cts.Token);
            },
            (_, e) => throw e
        );

        await subscription.Deliver(context);

        context.HasFailed().ShouldBeFalse();
        ShouldBeIgnoredBySubscription(context);
    }

    static void ShouldBeIgnoredBySubscription(IMessageConsumeContext context) {
        context.WasIgnored().ShouldBeTrue();
        context.HandlingResults.GetResultsOf(EventHandlingStatus.Ignored).Single().HandlerType.ShouldBe(Id);
    }

    static async Task<(CheckpointedSubscription, RecordingHandler, ConcurrentQueue<ulong?>)> StartCheckpointed(CancellationToken ct) {
        var checkpointStore = new NoOpCheckpointStore();
        var committed       = new ConcurrentQueue<ulong?>();
        checkpointStore.CheckpointStored += (_, cp) => committed.Enqueue(cp.Position);

        var handler = new RecordingHandler();

        var options = new CheckpointedOptions {
            SubscriptionId            = Id,
            CheckpointCommitBatchSize = 1,
            CheckpointCommitDelayMs   = 10
        };

        var subscription = new CheckpointedSubscription(options, checkpointStore, new ConsumePipe().AddDefaultConsumer(handler));

        await subscription.Subscribe(_ => { }, (_, _, _) => { }, ct);

        return (subscription, handler, committed);
    }

    static MessageConsumeContext CreateContext(ulong position, ulong sequence, object? message)
        => new(
            Guid.NewGuid().ToString(),
            "TestEvent",
            "application/json",
            "test-stream",
            position,
            position,
            position,
            sequence,
            DateTime.UtcNow,
            message,
            null,
            Id,
            CancellationToken.None
        );

    record PlainOptions : SubscriptionOptions;

    record CheckpointedOptions : SubscriptionWithCheckpointOptions;

    sealed class PlainSubscription(ConsumePipe pipe, bool throwOnError = false, ILoggerFactory? loggerFactory = null)
        : EventSubscription<PlainOptions>(new() { SubscriptionId = Id, ThrowOnError = throwOnError }, pipe, loggerFactory ?? NullLoggerFactory.Instance, null) {
        protected override ValueTask Connect(SubscriptionRun run) => default;

        public ValueTask Deliver(IMessageConsumeContext context) {
            context.LogContext = Log;

            return Handler(context);
        }
    }

    /// <summary>
    /// Has no pump of its own: the test delivers into the connected run directly.
    /// </summary>
    sealed class CheckpointedSubscription(CheckpointedOptions options, ICheckpointStore checkpointStore, ConsumePipe pipe)
        : EventSubscriptionWithCheckpoint<CheckpointedOptions>(options, checkpointStore, pipe, 1, SubscriptionKind.All, NullLoggerFactory.Instance, null, null) {
        SubscriptionRun? _run;

        protected override async ValueTask Connect(SubscriptionRun run) {
            await GetCheckpoint(run).NoContext();

            Volatile.Write(ref _run, run);
        }

        // Without a log context, as a transport that never set one would deliver it.
        public MessageConsumeContext CreateContext(ulong position, object? message) {
            var run     = Volatile.Read(ref _run)!;
            var context = PayloadlessContextTests.CreateContext(position, run.NextSequence(), message);

            context.LogContext        = null!;
            context.CancellationToken = run.Token;

            return context;
        }

        public ValueTask Deliver(IMessageConsumeContext context) => HandleInternal(Volatile.Read(ref _run)!, context);
    }

    sealed class RecordingHandler : BaseEventHandler {
        readonly ConcurrentQueue<ulong> _positions = new();

        public ulong[] Positions => _positions.ToArray();

        public override ValueTask<EventHandlingStatus> HandleEvent(IMessageConsumeContext context) {
            _positions.Enqueue(context.GlobalPosition);

            return new(EventHandlingStatus.Success);
        }
    }

    sealed class CountingFilter : ConsumeFilter<IMessageConsumeContext> {
        int _seen;

        public int Seen => Volatile.Read(ref _seen);

        protected override ValueTask Send(IMessageConsumeContext context, LinkedListNode<IConsumeFilter>? next) {
            Interlocked.Increment(ref _seen);

            return next?.Value.Send(context, next.Next) ?? default;
        }
    }
}
