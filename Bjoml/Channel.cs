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
using System.Runtime.CompilerServices;

namespace Bjoml;

public class Channel<T> : IEvent<T>
{
    private readonly object _lock = new();
    private PutOp<T>? _giversHead;
    private PutOp<T>? _giversTail;
    private GetOp<T>? _takersHead;
    private GetOp<T>? _takersTail;

    public void Publish(SyncState state, int eventId, Action<T> onSync)
    {
        PublishReceive(state, eventId, onSync);
    }

    public ChannelReceiveAwaiter<T> GetAwaiter() => new(this);

    public ChannelReceiveOperation<T> Receive() => new(this);

    public ChannelSendOperation<T> Send(T value) => new(this, value);

    /// <summary>
    /// Number of parked receive operations. Cleans stale entries and returns active count.
    /// </summary>
    internal int PendingReceiveCount
    {
        get
        {
            lock (_lock)
            {
                CleanTakers();
                int count = 0;
                var curr = _takersHead;
                while (curr != null)
                {
                    count++;
                    curr = curr.Next;
                }
                return count;
            }
        }
    }

    /// <summary>
    /// Number of parked send operations. Cleans stale entries and returns active count.
    /// </summary>
    internal int PendingSendCount
    {
        get
        {
            lock (_lock)
            {
                CleanGivers();
                int count = 0;
                var curr = _giversHead;
                while (curr != null)
                {
                    count++;
                    curr = curr.Next;
                }
                return count;
            }
        }
    }

    private void CleanTakers()
    {
        GetOp<T>? prev = null;
        GetOp<T>? curr = _takersHead;
        while (curr != null)
        {
            var next = curr.Next;
            if (curr.State != null && curr.IsSynchronized)
            {
                if (prev == null) _takersHead = next;
                else prev.Next = next;

                if (curr == _takersTail) _takersTail = prev;

                curr.Recycle();
            }
            else
            {
                prev = curr;
            }
            curr = next;
        }
    }

    private void CleanGivers()
    {
        PutOp<T>? prev = null;
        PutOp<T>? curr = _giversHead;
        while (curr != null)
        {
            var next = curr.Next;
            if (curr.State != null && curr.IsSynchronized)
            {
                if (prev == null) _giversHead = next;
                else prev.Next = next;

                if (curr == _giversTail) _giversTail = prev;

                curr.Recycle();
            }
            else
            {
                prev = curr;
            }
            curr = next;
        }
    }

    public void PublishSend(SyncState state, int eventId, T value, Action resumePut)
    {
        Action<T>? getResume = null;
        Action? directTakerResume = null;
        SyncState? getState = null;
        int getEventId = 0;
        bool matched = false;

        lock (_lock)
        {
            GetOp<T>? prev = null;
            GetOp<T>? curr = _takersHead;

            while (curr != null)
            {
                var next = curr.Next;

                if (curr.State != null && curr.IsSynchronized)
                {
                    if (prev == null) _takersHead = next;
                    else prev.Next = next;

                    if (curr == _takersTail) _takersTail = prev;

                    curr.Recycle();
                    curr = next;
                    continue;
                }

                if (curr.State != null && ReferenceEquals(curr.State, state))
                {
                    prev = curr;
                    curr = next;
                    continue;
                }

                if (state.TryClaim())
                {
                    if (curr.State != null)
                    {
                        if (curr.TrySync())
                        {
                            if (prev == null) _takersHead = next;
                            else prev.Next = next;

                            if (curr == _takersTail) _takersTail = prev;

                            getResume = curr.ResumeGet;
                            getState = curr.State;
                            getEventId = curr.EventId;
                            curr.Recycle();

                            matched = true;
                            break;
                        }

                        state.ResetClaim();
                        if (curr.IsSynchronized)
                        {
                            if (prev == null) _takersHead = next;
                            else prev.Next = next;

                            if (curr == _takersTail) _takersTail = prev;

                            curr.Recycle();
                            curr = next;
                            continue;
                        }
                    }
                    else
                    {
                        // Direct taker
                        if (prev == null) _takersHead = next;
                        else prev.Next = next;

                        if (curr == _takersTail) _takersTail = prev;

                        curr.DirectValue = value;
                        directTakerResume = curr.DirectResume;

                        matched = true;
                        break;
                    }
                }
                else
                {
                    return;
                }

                prev = curr;
                curr = next;
            }

            if (!matched)
            {
                if (state.IsSynchronized) return;

                var myOp = PutOp<T>.Rent(state, eventId, value, resumePut);
                if (_giversTail == null)
                {
                    _giversHead = _giversTail = myOp;
                }
                else
                {
                    _giversTail.Next = myOp;
                    _giversTail = myOp;
                }
                return;
            }
        }

        // Outside lock:
        state.MarkSynchronized(eventId);
        if (getState != null)
        {
            getState.MarkSynchronized(getEventId);
            Scheduler.Dispatch(getResume!, value);
        }
        else if (directTakerResume != null)
        {
            Scheduler.Dispatch(directTakerResume);
        }

        Scheduler.Dispatch(resumePut);
    }

    public void PublishReceive(SyncState state, int eventId, Action<T> resumeGet)
    {
        Action? putResume = null;
        SyncState? putState = null;
        int putEventId = 0;
        T putValue = default!;
        bool matched = false;

        lock (_lock)
        {
            PutOp<T>? prev = null;
            PutOp<T>? curr = _giversHead;

            while (curr != null)
            {
                var next = curr.Next;

                if (curr.State != null && curr.IsSynchronized)
                {
                    if (prev == null) _giversHead = next;
                    else prev.Next = next;

                    if (curr == _giversTail) _giversTail = prev;

                    curr.Recycle();
                    curr = next;
                    continue;
                }

                if (curr.State != null && ReferenceEquals(curr.State, state))
                {
                    prev = curr;
                    curr = next;
                    continue;
                }

                if (state.TryClaim())
                {
                    if (curr.State != null)
                    {
                        if (curr.TrySync())
                        {
                            if (prev == null) _giversHead = next;
                            else prev.Next = next;

                            if (curr == _giversTail) _giversTail = prev;

                            putValue = curr.Value;
                            putResume = curr.ResumePut;
                            putState = curr.State;
                            putEventId = curr.EventId;
                            curr.Recycle();

                            matched = true;
                            break;
                        }

                        state.ResetClaim();
                        if (curr.IsSynchronized)
                        {
                            if (prev == null) _giversHead = next;
                            else prev.Next = next;

                            if (curr == _giversTail) _giversTail = prev;

                            curr.Recycle();
                            curr = next;
                            continue;
                        }
                    }
                    else
                    {
                        // Direct giver
                        if (prev == null) _giversHead = next;
                        else prev.Next = next;

                        if (curr == _giversTail) _giversTail = prev;

                        putValue = curr.Value;
                        putResume = curr.ResumePut;

                        matched = true;
                        break;
                    }
                }
                else
                {
                    return;
                }

                prev = curr;
                curr = next;
            }

            if (!matched)
            {
                if (state.IsSynchronized) return;

                var myOp = GetOp<T>.Rent(state, eventId, resumeGet);
                if (_takersTail == null)
                {
                    _takersHead = _takersTail = myOp;
                }
                else
                {
                    _takersTail.Next = myOp;
                    _takersTail = myOp;
                }
                return;
            }
        }

        // Outside lock:
        state.MarkSynchronized(eventId);
        if (putState != null) putState.MarkSynchronized(putEventId);

        Scheduler.Dispatch(putResume!);
        Scheduler.Dispatch(resumeGet, putValue);
    }

    // -----------------------------------------------------------------------
    // Direct Channel Operations (Non-Selective Fast Paths)
    // -----------------------------------------------------------------------

    public bool TryDirectReceive(out T value)
    {
        Action? putResume = null;
        SyncState? putState = null;
        int putEventId = 0;
        value = default!;

        lock (_lock)
        {
            PutOp<T>? prev = null;
            PutOp<T>? curr = _giversHead;

            while (curr != null)
            {
                var next = curr.Next;

                if (curr.State != null && curr.IsSynchronized)
                {
                    if (prev == null) _giversHead = next;
                    else prev.Next = next;

                    if (curr == _giversTail) _giversTail = prev;

                    curr.Recycle();
                    curr = next;
                    continue;
                }

                if (curr.State != null)
                {
                    if (curr.TrySync())
                    {
                        if (prev == null) _giversHead = next;
                        else prev.Next = next;

                        if (curr == _giversTail) _giversTail = prev;

                        value = curr.Value;
                        putResume = curr.ResumePut;
                        putState = curr.State;
                        putEventId = curr.EventId;
                        curr.Recycle();
                        break;
                    }

                    if (curr.IsSynchronized)
                    {
                        if (prev == null) _giversHead = next;
                        else prev.Next = next;

                        if (curr == _giversTail) _giversTail = prev;

                        curr.Recycle();
                        curr = next;
                        continue;
                    }
                }
                else
                {
                    // Direct giver
                    if (prev == null) _giversHead = next;
                    else prev.Next = next;

                    if (curr == _giversTail) _giversTail = prev;

                    value = curr.Value;
                    putResume = curr.ResumePut;
                    break;
                }

                prev = curr;
                curr = next;
            }
        }

        if (putResume != null)
        {
            if (putState != null) putState.MarkSynchronized(putEventId);
            Scheduler.Dispatch(putResume);
            return true;
        }

        return false;
    }

    public void ParkDirectReceive(GetOp<T> op)
    {
        Action? putResume = null;
        SyncState? putState = null;
        int putEventId = 0;
        bool matched = false;
        T val = default!;

        lock (_lock)
        {
            PutOp<T>? prev = null;
            PutOp<T>? curr = _giversHead;

            while (curr != null)
            {
                var next = curr.Next;

                if (curr.State != null && curr.IsSynchronized)
                {
                    if (prev == null) _giversHead = next;
                    else prev.Next = next;

                    if (curr == _giversTail) _giversTail = prev;

                    curr.Recycle();
                    curr = next;
                    continue;
                }

                if (curr.State != null)
                {
                    if (curr.TrySync())
                    {
                        if (prev == null) _giversHead = next;
                        else prev.Next = next;

                        if (curr == _giversTail) _giversTail = prev;

                        val = curr.Value;
                        putResume = curr.ResumePut;
                        putState = curr.State;
                        putEventId = curr.EventId;
                        curr.Recycle();

                        matched = true;
                        break;
                    }

                    if (curr.IsSynchronized)
                    {
                        if (prev == null) _giversHead = next;
                        else prev.Next = next;

                        if (curr == _giversTail) _giversTail = prev;

                        curr.Recycle();
                        curr = next;
                        continue;
                    }
                }
                else
                {
                    // Direct giver
                    if (prev == null) _giversHead = next;
                    else prev.Next = next;

                    if (curr == _giversTail) _giversTail = prev;

                    val = curr.Value;
                    putResume = curr.ResumePut;

                    matched = true;
                    break;
                }

                prev = curr;
                curr = next;
            }

            if (matched)
            {
                op.DirectValue = val;
            }
            else
            {
                if (_takersTail == null)
                {
                    _takersHead = _takersTail = op;
                }
                else
                {
                    _takersTail.Next = op;
                    _takersTail = op;
                }
                return;
            }
        }

        if (putState != null) putState.MarkSynchronized(putEventId);
        Scheduler.Dispatch(putResume!);
        Scheduler.Enqueue(op.DirectResume!);
    }

    public bool TryDirectSend(T value)
    {
        Action<T>? getResume = null;
        Action? directResume = null;
        SyncState? getState = null;
        int getEventId = 0;

        lock (_lock)
        {
            GetOp<T>? prev = null;
            GetOp<T>? curr = _takersHead;

            while (curr != null)
            {
                var next = curr.Next;

                if (curr.State != null && curr.IsSynchronized)
                {
                    if (prev == null) _takersHead = next;
                    else prev.Next = next;

                    if (curr == _takersTail) _takersTail = prev;

                    curr.Recycle();
                    curr = next;
                    continue;
                }

                if (curr.State != null)
                {
                    if (curr.TrySync())
                    {
                        if (prev == null) _takersHead = next;
                        else prev.Next = next;

                        if (curr == _takersTail) _takersTail = prev;

                        getResume = curr.ResumeGet;
                        getState = curr.State;
                        getEventId = curr.EventId;
                        curr.Recycle();
                        break;
                    }

                    if (curr.IsSynchronized)
                    {
                        if (prev == null) _takersHead = next;
                        else prev.Next = next;

                        if (curr == _takersTail) _takersTail = prev;

                        curr.Recycle();
                        curr = next;
                        continue;
                    }
                }
                else
                {
                    // Direct taker
                    if (prev == null) _takersHead = next;
                    else prev.Next = next;

                    if (curr == _takersTail) _takersTail = prev;

                    curr.DirectValue = value;
                    directResume = curr.DirectResume;
                    break;
                }

                prev = curr;
                curr = next;
            }
        }

        if (getState != null)
        {
            getState.MarkSynchronized(getEventId);
            Scheduler.Dispatch(getResume!, value);
            return true;
        }

        if (directResume != null)
        {
            Scheduler.Dispatch(directResume);
            return true;
        }

        return false;
    }

    public void ParkDirectSend(PutOp<T> op)
    {
        Action<T>? getResume = null;
        Action? directResume = null;
        SyncState? getState = null;
        int getEventId = 0;
        bool matched = false;

        lock (_lock)
        {
            GetOp<T>? prev = null;
            GetOp<T>? curr = _takersHead;

            while (curr != null)
            {
                var next = curr.Next;

                if (curr.State != null && curr.IsSynchronized)
                {
                    if (prev == null) _takersHead = next;
                    else prev.Next = next;

                    if (curr == _takersTail) _takersTail = prev;

                    curr.Recycle();
                    curr = next;
                    continue;
                }

                if (curr.State != null)
                {
                    if (curr.TrySync())
                    {
                        if (prev == null) _takersHead = next;
                        else prev.Next = next;

                        if (curr == _takersTail) _takersTail = prev;

                        getResume = curr.ResumeGet;
                        getState = curr.State;
                        getEventId = curr.EventId;
                        curr.Recycle();

                        matched = true;
                        break;
                    }

                    if (curr.IsSynchronized)
                    {
                        if (prev == null) _takersHead = next;
                        else prev.Next = next;

                        if (curr == _takersTail) _takersTail = prev;

                        curr.Recycle();
                        curr = next;
                        continue;
                    }
                }
                else
                {
                    // Direct taker
                    if (prev == null) _takersHead = next;
                    else prev.Next = next;

                    if (curr == _takersTail) _takersTail = prev;

                    curr.DirectValue = op.Value;
                    directResume = curr.DirectResume;
                    matched = true;
                    break;
                }

                prev = curr;
                curr = next;
            }

            if (!matched)
            {
                if (_giversTail == null)
                {
                    _giversHead = _giversTail = op;
                }
                else
                {
                    _giversTail.Next = op;
                    _giversTail = op;
                }
                return;
            }
        }

        if (getState != null)
        {
            getState.MarkSynchronized(getEventId);
            Scheduler.Dispatch(getResume!, op.Value);
        }
        else if (directResume != null)
        {
            Scheduler.Dispatch(directResume);
        }

        Scheduler.Enqueue(op.ResumePut);
    }
}

public readonly struct ChannelReceiveAwaiter<T> : ICriticalNotifyCompletion
{
    private readonly Channel<T> _channel;
    private readonly T _result;
    private readonly bool _isCompleted;
    private readonly GetOp<T>? _op;

    public ChannelReceiveAwaiter(Channel<T> channel)
    {
        _channel = channel;
        if (channel.TryDirectReceive(out _result))
        {
            _isCompleted = true;
            _op = null;
        }
        else
        {
            _isCompleted = false;
            _op = GetOp<T>.RentDirect();
        }
    }

    public bool IsCompleted => _isCompleted;

    public T GetResult()
    {
        if (_op != null)
        {
            var res = _op.DirectValue;
            _op.Recycle();
            return res;
        }
        return _result;
    }

    public void OnCompleted(Action continuation) => UnsafeOnCompleted(continuation);

    public void UnsafeOnCompleted(Action continuation)
    {
        _op!.DirectResume = continuation;
        _channel.ParkDirectReceive(_op);
    }
}

public readonly struct ChannelSendAwaiter<T> : ICriticalNotifyCompletion
{
    private readonly Channel<T> _channel;
    private readonly bool _isCompleted;
    private readonly PutOp<T>? _op;

    public ChannelSendAwaiter(Channel<T> channel, T value)
    {
        _channel = channel;
        if (channel.TryDirectSend(value))
        {
            _isCompleted = true;
            _op = null;
        }
        else
        {
            _isCompleted = false;
            _op = PutOp<T>.Rent(null, 0, value, null!);
        }
    }

    public bool IsCompleted => _isCompleted;

    public void GetResult()
    {
        _op?.Recycle();
    }

    public void OnCompleted(Action continuation) => UnsafeOnCompleted(continuation);

    public void UnsafeOnCompleted(Action continuation)
    {
        _op!.ResumePut = continuation;
        _channel.ParkDirectSend(_op);
    }
}

public readonly struct ChannelReceiveOperation<T> : IEvent<T>
{
    private readonly Channel<T> _channel;

    public ChannelReceiveOperation(Channel<T> channel) => _channel = channel;

    public ChannelReceiveAwaiter<T> GetAwaiter() => new(_channel);

    public void Publish(SyncState state, int eventId, Action<T> onSync)
        => _channel.PublishReceive(state, eventId, onSync);
}

public readonly struct ChannelSendOperation<T> : IEvent<Unit>
{
    private readonly Channel<T> _channel;
    private readonly T _value;

    public ChannelSendOperation(Channel<T> channel, T value)
    {
        _channel = channel;
        _value = value;
    }

    public ChannelSendAwaiter<T> GetAwaiter() => new(_channel, _value);

    public void Publish(SyncState state, int eventId, Action<Unit> onSync)
        => _channel.PublishSend(state, eventId, _value, () => onSync(Unit.Value));
}
