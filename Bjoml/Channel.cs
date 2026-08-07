// Copyright (C) 2026 Linus Björnstam <linus.internet@fastmail.se>
//
// This file is part of BjoML.
//
// BjoML is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// BjoML is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with BjoML.  If not, see <https://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using Microsoft.Extensions.ObjectPool;

namespace Bjoml;

/// <summary>
/// Shared operation pools, one set per element type rather than per channel.
///
/// A per-<c>Channel</c> pool gives no benefit to a program that creates many
/// short-lived channels — every channel starts with an empty pool and pays for the
/// pool object itself — while the operations are entirely channel-agnostic.
/// </summary>
internal static class OpPool<T>
{
    internal static readonly ObjectPool<PutOp<T>> Put = ObjectPool.Create<PutOp<T>>();
    internal static readonly ObjectPool<GetOp<T>> Get = ObjectPool.Create<GetOp<T>>();
}

public class Channel<T>
{
    // A channel is a rendezvous point. If a Send arrives first it parks in _putq;
    // if a Receive arrives first it parks in _getq. At most one queue is non-empty
    // in the steady state.
    internal readonly ConcurrentQueue<PutOp<T>> _putq = new();
    internal readonly ConcurrentQueue<GetOp<T>> _getq = new();

    /// <summary>
    /// Number of parked receive operations. Test-only observability.
    ///
    /// Tests need a genuine "the fiber has suspended here" signal. A handshake over
    /// another channel does NOT provide one: the matching loop dispatches the
    /// PARTNER's continuation inline from inside the awaiter's constructor, so the
    /// partner can observe the rendezvous before the fiber has advanced to its next
    /// await, let alone parked on it.
    ///
    /// Includes operations already won by another branch, so only trust it on a
    /// channel the test controls.
    /// </summary>
    internal int PendingReceiveCount => _getq.Count;

    /// <summary>Number of parked send operations. Test-only observability.</summary>
    internal int PendingSendCount => _putq.Count;

    private enum MatchOutcome
    {
        /// <summary>Paired successfully; both continuations have been dispatched.</summary>
        Matched,

        /// <summary>
        /// We could not claim our OWN state, meaning another thread is already
        /// fulfilling us. It owns our continuation now; we must not touch anything.
        /// </summary>
        Aborted,

        /// <summary>Opposing queue ran dry without a match.</summary>
        Exhausted,
    }

    // -----------------------------------------------------------------------
    // Send
    // -----------------------------------------------------------------------

    public void PublishSend(SyncState state, int eventId, T value, Action resumePut)
    {
        // FAST PATH: try to pair without ever touching the queue.
        if (TryMatchGet(state, eventId, value, resumePut) != MatchOutcome.Exhausted)
            return;

        // SLOW PATH. We must publish ourselves BEFORE searching again: if a sender
        // and a receiver both searched before enqueueing, both would see empty
        // queues, both would park, and neither would ever be found.
        var myOp = OpPool<T>.Put.Get();
        myOp.State = state;
        myOp.EventId = eventId;
        myOp.Value = value;
        myOp.ResumePut = resumePut;
        _putq.Enqueue(myOp);

        // If this pairs, myOp is left behind in _putq already marked synchronized;
        // the next passer-by reclaims it.
        TryMatchGet(state, eventId, value, resumePut);
    }

    private MatchOutcome TryMatchGet(SyncState state, int eventId, T value, Action resumePut)
    {
        List<GetOp<T>>? deferred = null;
        try
        {
            while (_getq.TryDequeue(out var getOp))
            {
                // A sync block may never pair with itself. Without this, a perfectly
                // legal (choose (send ch v) (recv ch)) livelocks a whole thread
                // forever: we claim our state W->C, then try to drive the SAME state
                // W->S, which fails because it is now C, so we reset, re-queue, yield
                // and repeat. Thread.Yield cannot break the tie because there is no
                // other party involved.
                if (ReferenceEquals(getOp.State, state))
                {
                    (deferred ??= new List<GetOp<T>>()).Add(getOp);
                    continue;
                }

                if (getOp.IsSynchronized)
                {
                    OpPool<T>.Get.Return(getOp);
                    continue;
                }

                // Claim our own state first, so nobody can fulfil us while we are
                // busy fulfilling this getOp.
                if (state.TryClaim())
                {
                    if (getOp.TrySync())
                    {
                        // Paired. Read everything off getOp before recycling it.
                        var getResume = getOp.ResumeGet;
                        var getState = getOp.State;
                        int getEventId = getOp.EventId;

                        // Drives both blocks to S and fires the nacks of every
                        // losing choose branch on both sides.
                        state.MarkSynchronized(eventId);
                        getState.MarkSynchronized(getEventId);

                        OpPool<T>.Get.Return(getOp);

                        Scheduler.Dispatch(resumePut);
                        Scheduler.Dispatch(getResume, value);
                        return MatchOutcome.Matched;
                    }

                    // The receiver was claimed by someone else, or already finished.
                    // Release our own claim so others can interact with us again.
                    state.ResetClaim();

                    if (getOp.IsSynchronized)
                    {
                        OpPool<T>.Get.Return(getOp);
                    }
                    else
                    {
                        // Claimed but not yet synchronized: put it back so it is not
                        // lost if the other thread backs off, and yield to avoid two
                        // threads endlessly swapping each other's operations.
                        _getq.Enqueue(getOp);
                        Thread.Yield();
                    }
                }
                else
                {
                    // Someone is fulfilling us right now. Abandon the search, but do
                    // not swallow the op we just dequeued.
                    if (getOp.IsSynchronized)
                        OpPool<T>.Get.Return(getOp);
                    else
                        _getq.Enqueue(getOp);

                    return MatchOutcome.Aborted;
                }
            }

            return MatchOutcome.Exhausted;
        }
        finally
        {
            // Must run on EVERY exit path, including both early returns above.
            // Dropping a deferred op would silently delete one of our own pending
            // choose branches, and the sync block would then block forever.
            if (deferred != null)
                foreach (var op in deferred) _getq.Enqueue(op);
        }
    }

    // -----------------------------------------------------------------------
    // Receive
    // -----------------------------------------------------------------------

    public void PublishReceive(SyncState state, int eventId, Action<T> resumeGet)
    {
        if (TryMatchPut(state, eventId, resumeGet) != MatchOutcome.Exhausted)
            return;

        var myOp = OpPool<T>.Get.Get();
        myOp.State = state;
        myOp.EventId = eventId;
        myOp.ResumeGet = resumeGet;
        _getq.Enqueue(myOp);

        TryMatchPut(state, eventId, resumeGet);
    }

    private MatchOutcome TryMatchPut(SyncState state, int eventId, Action<T> resumeGet)
    {
        List<PutOp<T>>? deferred = null;
        try
        {
            while (_putq.TryDequeue(out var putOp))
            {
                // See TryMatchGet: never pair a sync block with itself.
                if (ReferenceEquals(putOp.State, state))
                {
                    (deferred ??= new List<PutOp<T>>()).Add(putOp);
                    continue;
                }

                if (putOp.IsSynchronized)
                {
                    OpPool<T>.Put.Return(putOp);
                    continue;
                }

                if (state.TryClaim())
                {
                    if (putOp.TrySync())
                    {
                        T capturedValue = putOp.Value;
                        var putResume = putOp.ResumePut;
                        var putState = putOp.State;
                        int putEventId = putOp.EventId;

                        state.MarkSynchronized(eventId);
                        putState.MarkSynchronized(putEventId);

                        OpPool<T>.Put.Return(putOp);

                        Scheduler.Dispatch(putResume);
                        Scheduler.Dispatch(resumeGet, capturedValue);
                        return MatchOutcome.Matched;
                    }

                    state.ResetClaim();

                    if (putOp.IsSynchronized)
                    {
                        OpPool<T>.Put.Return(putOp);
                    }
                    else
                    {
                        _putq.Enqueue(putOp);
                        Thread.Yield();
                    }
                }
                else
                {
                    if (putOp.IsSynchronized)
                        OpPool<T>.Put.Return(putOp);
                    else
                        _putq.Enqueue(putOp);

                    return MatchOutcome.Aborted;
                }
            }

            return MatchOutcome.Exhausted;
        }
        finally
        {
            if (deferred != null)
                foreach (var op in deferred) _putq.Enqueue(op);
        }
    }
}
