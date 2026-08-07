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

namespace Bjoml;

public abstract class Operation
{
    public SyncState State = null!;
    public int EventId;

    public bool IsSynchronized => State.IsSynchronized;

    public bool TrySync() => State.TrySync();
}

public sealed class PutOp<T> : Operation
{
    private const int MaxCached = 64;
    [ThreadStatic] private static PutOp<T>? _free;
    [ThreadStatic] private static int _freeCount;

    public T Value = default!;
    public Action ResumePut = null!;
    public PutOp<T>? Next;

    public static PutOp<T> Rent(SyncState state, int eventId, T value, Action resumePut)
    {
        var op = _free;
        if (op is null)
        {
            return new PutOp<T>
            {
                State = state,
                EventId = eventId,
                Value = value,
                ResumePut = resumePut
            };
        }

        _free = op.Next;
        _freeCount--;
        op.Next = null;
        op.State = state;
        op.EventId = eventId;
        op.Value = value;
        op.ResumePut = resumePut;
        return op;
    }

    public void Recycle()
    {
        State = null!;
        Value = default!;
        ResumePut = null!;
        EventId = 0;

        if (_freeCount < MaxCached)
        {
            Next = _free;
            _free = this;
            _freeCount++;
        }
        else
        {
            Next = null;
        }
    }
}

public sealed class GetOp<T> : Operation
{
    private const int MaxCached = 64;
    [ThreadStatic] private static GetOp<T>? _free;
    [ThreadStatic] private static int _freeCount;

    public Action<T> ResumeGet = null!;
    public GetOp<T>? Next;

    public static GetOp<T> Rent(SyncState state, int eventId, Action<T> resumeGet)
    {
        var op = _free;
        if (op is null)
        {
            return new GetOp<T>
            {
                State = state,
                EventId = eventId,
                ResumeGet = resumeGet
            };
        }

        _free = op.Next;
        _freeCount--;
        op.Next = null;
        op.State = state;
        op.EventId = eventId;
        op.ResumeGet = resumeGet;
        return op;
    }

    public void Recycle()
    {
        State = null!;
        ResumeGet = null!;
        EventId = 0;

        if (_freeCount < MaxCached)
        {
            Next = _free;
            _free = this;
            _freeCount++;
        }
        else
        {
            Next = null;
        }
    }
}