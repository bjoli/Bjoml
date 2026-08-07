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

public class Channel<T>
{
    private readonly object _lock = new();
    private PutOp<T>? _giversHead;
    private PutOp<T>? _giversTail;
    private GetOp<T>? _takersHead;
    private GetOp<T>? _takersTail;

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
            if (curr.IsSynchronized)
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
            if (curr.IsSynchronized)
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

                if (curr.IsSynchronized)
                {
                    // Clean stale receiver
                    if (prev == null) _takersHead = next;
                    else prev.Next = next;

                    if (curr == _takersTail) _takersTail = prev;

                    curr.Recycle();
                    curr = next;
                    continue;
                }

                if (ReferenceEquals(curr.State, state))
                {
                    // Never pair a sync block with itself (self-choose)
                    prev = curr;
                    curr = next;
                    continue;
                }

                if (state.TryClaim())
                {
                    if (curr.TrySync())
                    {
                        // Matched! Unlink curr
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
                    // If the other receiver is now synchronized, unlink it
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
                    // We were fulfilled by someone else
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
        getState!.MarkSynchronized(getEventId);

        Scheduler.Dispatch(resumePut);
        Scheduler.Dispatch(getResume!, value);
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

                if (curr.IsSynchronized)
                {
                    if (prev == null) _giversHead = next;
                    else prev.Next = next;

                    if (curr == _giversTail) _giversTail = prev;

                    curr.Recycle();
                    curr = next;
                    continue;
                }

                if (ReferenceEquals(curr.State, state))
                {
                    prev = curr;
                    curr = next;
                    continue;
                }

                if (state.TryClaim())
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
        putState!.MarkSynchronized(putEventId);

        Scheduler.Dispatch(putResume!);
        Scheduler.Dispatch(resumeGet, putValue);
    }
}
