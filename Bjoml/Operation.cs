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

using Microsoft.Extensions.ObjectPool;

namespace Bjoml;

public abstract class Operation
{
    // Every queued operation holds a reference to the SyncState of the 'Cml.Sync' block that created it.
    // By locking and mutating this shared state, we pair operations atomically.
    // Assigned by the channel immediately after renting from the pool, never by a
    // constructor, hence the null-forgiving initialiser.
    public SyncState State = null!;
    
    // The specific EventId for this branch in a 'Choose' block. 
    // This allows the SyncState to know which branch won, so it can fire the NACKs for the losers.
    public int EventId;

    public bool IsSynchronized => State.IsSynchronized;

    /// <summary>
    /// Drive this (the opposing party's) block straight to Synchronized. The caller
    /// must already hold a claim on its own block.
    /// </summary>
    public bool TrySync() => State.TrySync();
}

/// <summary>
/// Represents a pending 'Put' (Send) operation sitting in a channel's queue.
/// We implement IResettable so these objects can be reused by the ObjectPool, 
/// avoiding garbage collection overhead during millions of channel communications.
/// </summary>
public class PutOp<T> : Operation, IResettable
{
    public T Value = default!;
    public Action ResumePut = null!;

    public bool TryReset()
    {
        // Clear references so they can be garbage collected. 
        // This is crucial to avoid memory leaks of the captured values or continuations while the object sits in the pool.
        Value = default!;
        ResumePut = null!;
        State = null!; 
        EventId = 0;
        return true;
    }
}

/// <summary>
/// Represents a pending 'Get' (Receive) operation sitting in a channel's queue.
/// </summary>
public class GetOp<T> : Operation, IResettable
{
    public Action<T> ResumeGet = null!;

    public bool TryReset()
    {
        ResumeGet = null!;
        State = null!; 
        EventId = 0;
        return true;
    }
}