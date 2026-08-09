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
using System.Threading;

namespace Bjoml;

/// <summary>
/// Something that can be told "one or more of your parked entries just died", so that
/// a committing sync block can drive cleanup of the channels its branches published to.
///
/// Non-generic on purpose: <see cref="SyncState"/> knows nothing about the element type
/// of the channels its branches touched.
/// </summary>
internal interface IDeadEntrySink
{
    void NoteDeadEntry();
}

public class Channel<T> : IEvent<T>, IDeadEntrySink
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

    /// <summary>
    /// Parked receive entries, counted WITHOUT cleaning first. Test-only.
    ///
    /// <see cref="PendingReceiveCount"/> calls <c>CleanTakers</c> before counting, so it
    /// can never observe a leak — reading it performs exactly the reclamation that a B7
    /// test is trying to prove happens on its own, and the test passes whether or not
    /// the bug is fixed. Proving entries do not accumulate requires the raw list.
    /// </summary>
    internal int RawPendingReceiveCount
    {
        get
        {
            lock (_lock)
            {
                int count = 0;
                for (var curr = _takersHead; curr != null; curr = curr.Next) count++;
                return count;
            }
        }
    }

    /// <summary>Parked send entries, counted without cleaning first. Test-only.</summary>
    internal int RawPendingSendCount
    {
        get
        {
            lock (_lock)
            {
                int count = 0;
                for (var curr = _giversHead; curr != null; curr = curr.Next) count++;
                return count;
            }
        }
    }

    /// <summary>
    /// A block that parked here has committed, so at least one of our entries is now
    /// dead. Unlink and recycle every synchronized entry (B7).
    ///
    /// Both directions are cleaned because registration records the CHANNEL, not which
    /// side the branch parked on; a choose may well have offered both. Both lists are
    /// short in practice, and the walk only happens on commit, never on the fast path.
    ///
    /// MUST NOT be called while holding this channel's lock: it is reached from
    /// <see cref="SyncState.MarkSynchronized"/>, which deliberately releases its own
    /// lock first. Re-entering here mid-traversal would corrupt the caller's iteration.
    /// </summary>
    void IDeadEntrySink.NoteDeadEntry()
    {
        // Cleaned immediately rather than amortised behind a dead-entry threshold.
        //
        // Thresholding was tried and rejected. It only bounds growth at the threshold
        // instead of driving it to zero — an abandoned channel keeps its loser forever,
        // and 500 losing branches left 20 stranded — and it bought just 173 -> 158 ns/op
        // on Select/Choose. That is a poor price for giving up the zero-stranded
        // guarantee, because the remaining cost is not this walk at all: it is the extra
        // lock TryRegisterSink takes on every park. See SyncState.
        lock (_lock)
        {
            CleanTakers();
            CleanGivers();
        }
    }

    /// <summary>Unlink and recycle synchronized takers. Returns the surviving count.</summary>
    private int CleanTakers()
    {
        int live = 0;
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
                live++;
            }
            curr = next;
        }
        return live;
    }

    /// <summary>Unlink and recycle synchronized givers. Returns the surviving count.</summary>
    private int CleanGivers()
    {
        int live = 0;
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
                live++;
            }
            curr = next;
        }
        return live;
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
                // Register before parking so the committing side can come back and
                // reclaim this op if our branch loses. A false return means the block
                // already committed, so parking now would strand an entry that nothing
                // is coming back for.
                if (!state.TryRegisterSink(this)) return;

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
                // See PublishSend: register before parking so committing can reclaim us.
                if (!state.TryRegisterSink(this)) return;

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
