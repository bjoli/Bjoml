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
using System.Threading;

namespace Bjoml;

/// <summary>
/// An intrusive singly-linked record for negative acknowledgements (NACKs).
/// Follows Hopac's interval-based choice tracking: each NACK guards an index interval [I0, I1).
/// </summary>
public sealed class NackNode
{
    public readonly Action Action;
    public readonly int I0;
    public int I1;
    public NackNode? Next;

    public NackNode(Action action, int i0, NackNode? next)
    {
        Action = action;
        I0 = i0;
        I1 = int.MaxValue;
        Next = next;
    }
}

/// <summary>
/// The lock-free state machine shared by every branch of one <c>Cml.Sync</c> block.
///
/// <list type="bullet">
/// <item><c>W</c> Waiting — pending, claimable by any matching thread.</item>
/// <item><c>C</c> Claimed — a thread is inspecting this state to pair it. Held
/// across a handful of instructions only, never across user code, which is what
/// makes spinning on it safe.</item>
/// <item><c>S</c> Synchronized — paired, terminal.</item>
/// </list>
///
/// Borrowed from Hopac Core: choice branches are indexed with sequential integers 0, 1, 2, ...
/// Subtrees are tracked as contiguous intervals [I0, I1). A nack fires only when the
/// winning branch index lies OUTSIDE [I0, I1), completely avoiding dictionary allocations,
/// tree hashing, and locks for ID generation.
/// </summary>
public class SyncState
{
    public const int W = 0;
    public const int C = 1;
    public const int S = 2;

    /// <summary>Root event index.</summary>
    public const int RootEventId = 0;

    private int _value = W;
    private int _eventIdCounter = RootEventId;
    private int _winningEventId = -1;

    // Intrusive singly-linked list of NACKs.
    // Allocated on demand only when withNack is actually used.
    private NackNode? _nacks;

    // Every channel this block parked an operation in, so that committing can drive
    // cleanup there (B7). A losing choose branch leaves its op in the channel's list,
    // and that op is otherwise only reclaimed if some LATER operation happens to walk
    // past it -- which never happens on a channel that is offered in a choose and then
    // goes quiet. Cleanup therefore has to be driven from the COMMITTING side: the
    // committing block is the only party that knows those branches just died.
    //
    // We deliberately store the CHANNEL, not the operation. A matcher can unlink and
    // recycle an op at any moment, so a stored op reference may already have been
    // handed to somebody else by the time we look at it. A redundant clean costs one
    // short list walk; a missed one leaks forever.
    //
    // Two inline slots, the second doubling as the overflow list for three or more.
    //
    // SyncState is allocated once per Cml.Sync, so every reference field added here
    // lands in the per-op allocation figure at 8 B/op each. That argues for packing
    // into as few fields as possible — but a single field forces a List as soon as a
    // block parks in TWO channels, which is exactly what an ordinary two-way choose
    // does. Warm, that List fires on most commits and costs 88 B/op and ~80 ns:
    //
    //   Select/Choose, median of 9 reps, steady state
    //     baseline, no cleanup tracking     130 ns/op    40 B/op
    //     one packed field (List fires)     213 ns/op   136 B/op
    //
    // So two slots. A two-way choose is the common shape and must not allocate; three
    // or more is rare enough to pay for a List.
    private IDeadEntrySink? _sink0;
    private object? _sink1OrRest;   // IDeadEntrySink, or List<IDeadEntrySink> for 3+

    /// <summary>The next event index to be minted.</summary>
    public int CurrentEventId => Volatile.Read(ref _eventIdCounter);

    /// <summary>The winning event ID that synchronized this state.</summary>
    public int WinningEventId => Volatile.Read(ref _winningEventId);

    /// <summary>Mint a fresh sequential leaf/branch id.</summary>
    public int NextEventId() => Interlocked.Increment(ref _eventIdCounter) - 1;

    // ---- state transitions -------------------------------------------------

    /// <summary>W -&gt; C. Take exclusive ownership so we can pair against another state.</summary>
    public bool TryClaim() => Interlocked.CompareExchange(ref _value, C, W) == W;

    /// <summary>
    /// W -&gt; S. Called on the OPPOSING party's state by a thread that already holds
    /// C on its own. Does not fire nacks; the caller follows up with
    /// <see cref="MarkSynchronized"/>.
    /// </summary>
    public bool TrySync() => Interlocked.CompareExchange(ref _value, S, W) == W;

    /// <summary>C -&gt; W. Release a claim we could not turn into a pairing.</summary>
    public void ResetClaim() => Volatile.Write(ref _value, W);

    public bool IsSynchronized => Volatile.Read(ref _value) == S;

    /// <summary>
    /// Note that this block is about to park an operation in <paramref name="sink"/>,
    /// so that committing can come back and clean it up.
    ///
    /// Returns false if the block has ALREADY committed, in which case the caller must
    /// NOT park: nothing is coming back to reclaim it. This subsumes the bare
    /// <see cref="IsSynchronized"/> check it replaces, and closes the race in it —
    /// here the test and the registration happen under one lock, so a commit cannot
    /// slip between them and strand the op we are about to enqueue.
    /// </summary>
    internal bool TryRegisterSink(IDeadEntrySink sink)
    {
        lock (this)
        {
            if (Volatile.Read(ref _value) == S) return false;

            // The reference checks also dedupe the common `choose [recv ch; send ch]`
            // shape for free. Only the two inline slots are checked; scanning the
            // overflow list would cost more than the redundant walk it saves.
            if (_sink0 == null) { _sink0 = sink; return true; }
            if (ReferenceEquals(_sink0, sink)) return true;

            if (_sink1OrRest == null) { _sink1OrRest = sink; return true; }
            if (ReferenceEquals(_sink1OrRest, sink)) return true;

            if (_sink1OrRest is List<IDeadEntrySink> list) list.Add(sink);
            else _sink1OrRest = new List<IDeadEntrySink> { (IDeadEntrySink)_sink1OrRest, sink };

            return true;
        }
    }

    /// <summary>
    /// Atomic W -&gt; C -&gt; S for events that commit on their own initiative rather
    /// than by pairing: <c>Always</c>, <c>Promise</c>, timers.
    ///
    /// Crucially it spins past a transient <c>C</c> instead of reporting failure.
    /// A plain <c>TryClaim</c> here silently DROPS the completion: a promise that
    /// lands on a thread-pool thread while the publishing thread happens to hold
    /// <c>C</c> for another branch would be discarded, and the choose would wait
    /// forever on an event that already fired.
    ///
    /// Returns false only when the block was genuinely won by someone else.
    /// </summary>
    public bool TryCommit(int eventId)
    {
        var sw = new SpinWait();
        while (true)
        {
            int v = Volatile.Read(ref _value);
            if (v == S) return false;                 // lost for real
            if (v == W && Interlocked.CompareExchange(ref _value, C, W) == W)
            {
                MarkSynchronized(eventId);
                return true;
            }
            sw.SpinOnce();                            // v == C: transient, retry
        }
    }

    /// <summary>
    /// Finish a synchronisation and fire the nacks of every losing subtree.
    ///
    /// PRECONDITION: the caller must already own this state, having driven it to
    /// <c>C</c> via <see cref="TryClaim"/>/<see cref="TryCommit"/> or to <c>S</c>
    /// via <see cref="TrySync"/>. Calling it on an unowned state blindly stomps
    /// another thread's claim.
    /// </summary>
    public void MarkSynchronized(int winningEventId)
    {
        _winningEventId = winningEventId;
        Volatile.Write(ref _value, S);

        NackNode? toFireHead = null;
        IDeadEntrySink? sink0;
        object? sink1OrRest;

        lock (this)
        {
            sink0 = _sink0;
            _sink0 = null;
            sink1OrRest = _sink1OrRest;
            _sink1OrRest = null;

            var curr = _nacks;
            _nacks = null;
            while (curr != null)
            {
                var next = curr.Next;
                if (winningEventId < curr.I0 || curr.I1 <= winningEventId)
                {
                    curr.Next = toFireHead;
                    toFireHead = curr;
                }
                curr = next;
            }
        }

        // Outside the lock, and this is load-bearing. Cleanup takes the channel's lock,
        // while the parking path takes the channel lock and then THIS lock (via
        // TryRegisterSink). Holding this lock while reaching for a channel lock would
        // invert that order and deadlock. Releasing first means the only nesting in the
        // system stays channel -> state, with no cycle.
        sink0?.NoteDeadEntry();
        if (sink1OrRest is IDeadEntrySink one) one.NoteDeadEntry();
        else if (sink1OrRest is List<IDeadEntrySink> many)
            foreach (var s in many) s.NoteDeadEntry();

        while (toFireHead != null)
        {
            Scheduler.Enqueue(toFireHead.Action);
            toFireHead = toFireHead.Next;
        }
    }

    /// <summary>
    /// Register a NACK callback for the interval starting at <paramref name="i0"/>.
    /// Returns the <see cref="NackNode"/> whose <c>I1</c> should be updated after
    /// publishing the guarded subtree.
    /// </summary>
    public NackNode? RegisterNack(int i0, Action nack)
    {
        lock (this)
        {
            if (!IsSynchronized)
            {
                var node = new NackNode(nack, i0, _nacks);
                _nacks = node;
                return node;
            }
        }

        // Another branch won before we finished publishing this one.
        // Fire it now if the winning event is outside [i0, +inf).
        if (_winningEventId < i0)
        {
            Scheduler.Enqueue(nack);
        }

        return null;
    }
}
