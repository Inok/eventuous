// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Diagnostics;
using Eventuous.Diagnostics;
using Constants = Eventuous.Diagnostics.Tracing.Constants;

namespace Eventuous.Subscriptions.Filters;

using Context;
using Diagnostics;

public class TracingFilter : ConsumeFilter<IMessageConsumeContext> {
    readonly KeyValuePair<string, object?>[] _defaultTags;

    public TracingFilter(string consumerName) {
        var tags = new KeyValuePair<string, object?>[] { new(TelemetryTags.Eventuous.Consumer, consumerName) };

        _defaultTags = [.. tags, .. EventuousDiagnostics.Tags];
    }

    protected override ValueTask Send(IMessageConsumeContext context, LinkedListNode<IConsumeFilter>? next) {
        if (context.Message == null || next == null) return default;

        // The subscription's own activity is reused, not owned: disposing it would stop it before the
        // subscription is done with it, so only an activity started here gets disposed.
        var reuseCurrent = Activity.Current?.Context == context.ParentContext;

        var created = reuseCurrent
            ? null
            : SubscriptionActivity.Create(
                $"{Constants.Components.Consumer}.{context.SubscriptionId}/{context.MessageType}",
                ActivityKind.Consumer,
                context,
                _defaultTags
            );

        var reused = reuseCurrent ? Activity.Current : null;

        // Nobody listening, or sampled out: nothing to record after the next filter, so its task is returned
        // as is rather than awaited.
        if (reused == null && created == null) return next.Value.Send(context, next.Next);

        return SendTraced(context, next, reused, created);
    }

    static async ValueTask SendTraced(IMessageConsumeContext context, LinkedListNode<IConsumeFilter> next, Activity? reused, Activity? created) {
        // Started here, not by the caller: this method's execution context is restored when it returns, so the
        // started activity doesn't stay current for whoever called the filter.
        using var started = created?.Start();

        var activity = reused ?? started;

        if (activity?.IsAllDataRequested == true && context is AsyncConsumeContext asyncConsumeContext) {
            activity.SetContextTags(context)?.SetTag(TelemetryTags.Eventuous.Partition, asyncConsumeContext.PartitionId);
        }

        try {
            await next.Value.Send(context, next.Next).NoContext();

            if (activity != null) {
                if (context.WasIgnored()) {
                    activity.ActivityTraceFlags = ActivityTraceFlags.None;
                }

                // A handler failure is recorded with Nack, not thrown, and Nack has already set the error status
                if (!context.HasFailed()) activity.SetActivityStatus(ActivityStatus.Ok());
            }
        }
        catch (Exception e) {
            activity?.SetActivityStatus(ActivityStatus.Error(e, $"Error handling {context.MessageType}"));
            throw;
        }
    }
}
