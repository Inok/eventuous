// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Collections.Concurrent;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Context;
using Eventuous.Subscriptions.Filters;
using Eventuous.Subscriptions.Logging;
using Shouldly;

namespace Eventuous.Tests.Subscriptions;

/// <summary>
/// The filter's reader keeps the logger context between messages instead of setting it anew for each, so
/// what's pinned here is that a message still never runs under the context of the one before it — the case
/// of a pipe shared by two subscriptions, whose messages interleave on one reader.
/// </summary>
public class AsyncHandlingFilterLogContextTests {
    [Test]
    public async Task Handler_sees_the_log_context_of_its_own_message() {
        var handler = new RecordingHandler();

        await using var pipe = new ConsumePipe().AddDefaultConsumer(handler).AddFilterFirst(new AsyncHandlingFilter(1));

        var first  = Logger.CreateContext("first", null);
        var second = Logger.CreateContext("second", null);

        // Repeats and switches both: a context kept from the previous message must be neither lost nor stale.
        LogContext[] sent = [first, first, second, first, second, second];

        for (var i = 0; i < sent.Length; i++) {
            await pipe.Send(CreateContext(i, sent[i]));
        }

        var handled = await Wait.Until(() => handler.Seen.Count == sent.Length, TimeSpan.FromSeconds(5));

        handled.ShouldBeTrue($"{handler.Seen.Count} of {sent.Length} messages handled");

        // One reader, so the order is the order sent.
        var seen = handler.Seen.ToArray();

        for (var i = 0; i < sent.Length; i++) {
            seen[i].Message.ShouldBeSameAs(sent[i], $"message {i} arrived out of order");
            seen[i].Current.ShouldBeSameAs(sent[i], $"message {i} was handled under another message's log context");
        }
    }

    static AsyncConsumeContext CreateContext(int position, LogContext logContext) {
        var context = new MessageConsumeContext(
            Guid.NewGuid().ToString(),
            "TestEvent",
            "application/json",
            "test-stream",
            (ulong)position,
            (ulong)position,
            (ulong)position,
            (ulong)position,
            DateTime.UtcNow,
            new { Number = position },
            new(),
            logContext.SubscriptionId,
            CancellationToken.None
        ) { LogContext = logContext };

        return new(context, _ => default, (_, _) => default);
    }

    sealed class RecordingHandler : BaseEventHandler {
        public ConcurrentQueue<(LogContext Message, LogContext Current)> Seen { get; } = new();

        public override ValueTask<EventHandlingStatus> HandleEvent(IMessageConsumeContext context) {
            Seen.Enqueue((context.LogContext, Logger.Current));

            return new(EventHandlingStatus.Success);
        }
    }
}
