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
/// Event ids form a TREE, not a flat set: <c>choose</c> mints children,
/// <c>wrap</c>/<c>guard</c> pass the id through unchanged, and <c>withNack</c>
/// attaches a nack to an interior node. A nack must fire only when the winner lies
/// OUTSIDE the subtree it guards.
/// </summary>
public class SyncState
{
    public const int W = 0;
    public const int C = 1;
    public const int S = 2;

    /// <summary>Root of the event-id tree. Ancestor of every other id.</summary>
    public const int RootEventId = 0;

    private int _value = W;
    private int _eventIdCounter = RootEventId;

    // Both collections are allocated ON DEMAND, and this matters a lot: a SyncState
    // is created for EVERY Cml.Sync, including a bare channel receive that has no
    // choose branches and no nacks. Allocating a Dictionary and a List up front cost
    // ~136 bytes on every single message for state the overwhelming majority of
    // syncs never touch.
    //
    //   _parents is needed only by choose (GenerateChildId).
    //   _nacks   is needed only by withNack (RegisterNack).
    //
    // We lock on `this` rather than on a dedicated lock object, for the same reason:
    // a separate object would be one more allocation per sync. SyncState is a
    // synchronisation primitive that nothing else locks on, so the usual objection
    // to lock(this) does not apply here. One monitor covers both collections; they
    // are only ever taken together (MarkSynchronized walks the id tree while holding
    // the nack list) and a single reentrant monitor removes any ordering hazard.
    // Contention is per-sync-block, i.e. essentially nil.

    // child id -> parent id. Root is implicit and never present.
    private Dictionary<int, int>? _parents;

    // A LIST, not a dictionary keyed by id. Two withNacks can legitimately share an
    // id, because withNack publishes its generated event with its own id; keying by
    // id let the inner one silently overwrite and discard the outer one's nack.
    private List<(int EventId, Action Nack)>? _nacks;

    /// <summary>Mint a fresh id as a child of <paramref name="parentId"/>.</summary>
    public int GenerateChildId(int parentId)
    {
        int id = Interlocked.Increment(ref _eventIdCounter);
        lock (this) (_parents ??= new Dictionary<int, int>())[id] = parentId;
        return id;
    }

    /// <summary>
    /// Is <paramref name="candidate"/> equal to, or an ancestor of,
    /// <paramref name="winner"/>? If so, the winner is inside the candidate's
    /// subtree and the candidate's nack must NOT fire.
    /// </summary>
    private bool CoversWinner(int candidate, int winner)
    {
        lock (this)
        {
            int node = winner;
            while (true)
            {
                if (candidate == node) return true;
                if (node == RootEventId) return false;

                // No _parents means no choose ever minted a child, so the only id
                // that can cover the winner is the winner itself, already checked.
                if (_parents == null || !_parents.TryGetValue(node, out node)) return false;
            }
        }
    }

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
        Volatile.Write(ref _value, S);

        List<Action>? toFire = null;
        lock (this)
        {
            if (_nacks != null)
            {
                for (int i = 0; i < _nacks.Count; i++)
                {
                    var (id, nack) = _nacks[i];
                    if (!CoversWinner(id, winningEventId))
                        (toFire ??= new List<Action>()).Add(nack);
                }
                _nacks.Clear();
            }
        }

        if (toFire != null)
            foreach (var a in toFire) Scheduler.Enqueue(a);
    }

    public void RegisterNack(int eventId, Action nack)
    {
        lock (this)
        {
            if (!IsSynchronized)
            {
                (_nacks ??= new List<(int, Action)>()).Add((eventId, nack));
                return;
            }
        }

        // Another branch won before we finished publishing this one, so nobody will
        // ever walk the list on our behalf. Fire it now.
        Scheduler.Enqueue(nack);
    }
}
