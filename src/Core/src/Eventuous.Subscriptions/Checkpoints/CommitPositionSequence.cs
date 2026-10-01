// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Runtime.CompilerServices;

// ReSharper disable UseDeconstructionOnParameter

namespace Eventuous.Subscriptions.Checkpoints;

using Diagnostics;

public class CommitPositionSequence() : SortedSet<CommitPosition>(new PositionsComparer()) {
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CommitPosition FirstBeforeGap()
        => Count switch {
            0 => CommitPosition.None,
            1 => Min,
            _ => Get()
        };

    CommitPosition Get() {
        var  previous = default(CommitPosition);
        bool first    = true;

        // The set is ordered by Sequence; the struct enumerator avoids the boxed IEnumerable<T> one
        foreach (var current in this) {
            if (!first && previous.Sequence + 1 != current.Sequence) {
                SubscriptionsEventSource.Log.CheckpointGapDetected(previous, current);

                return previous;
            }

            previous = current;
            first    = false;
        }

        return Max;
    }

    /// <summary>
    /// Removes all positions with a sequence up to and including the given one. The set is ordered
    /// by sequence, so these are always a prefix; no predicate (and no closure) needed.
    /// </summary>
    internal void RemoveUpTo(ulong sequence) {
        // Min is default on an empty set, so the Count check is what stops the loop
        while (Count > 0 && Min.Sequence <= sequence) Remove(Min);
    }

    class PositionsComparer : IComparer<CommitPosition> {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(CommitPosition x, CommitPosition y) {
            if (x.Sequence == y.Sequence) return 0;

            return x.Sequence > y.Sequence ? 1 : -1;
        }
    }
}
