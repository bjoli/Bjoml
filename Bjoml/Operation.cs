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
    public SyncState State; 
    public int EventId;

    // Must be a field (not a property) for Interlocked.CompareExchange to take a ref
    internal Operation? Next;

    public bool IsSynchronized => State.IsSynchronized;
    public bool TryClaim() => State.TryClaim();
    public bool TrySync() => State.TrySync();
    public void MarkSynchronized() => State.MarkSynchronized(EventId);
    public void ResetClaim() => State.ResetClaim();
}

/// <summary>
/// Represents a pending 'Put' (Send) operation sitting in a channel's queue.
/// We implement IResettable so these objects can be reused by the ObjectPool, 
/// avoiding garbage collection overhead during millions of channel communications.
/// </summary>
public class PutOp<T> : Operation, IResettable
{
    public T Value;
    public Action ResumePut;

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
    public Action<T> ResumeGet;

    public bool TryReset()
    {
        ResumeGet = null!;
        State = null!; 
        EventId = 0;
        return true;
    }
}