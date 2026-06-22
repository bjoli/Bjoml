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
using System.Collections.Concurrent;
using Microsoft.Extensions.ObjectPool;

namespace Bjoml;

public class Channel<T>
{
    // We use ConcurrentQueues to hold unmatched operations. A channel is fundamentally a rendezvous point.
    // If a Send arrives before a Receive, it is queued in _putq. If a Receive arrives first, it is queued in _getq.
    internal readonly ConcurrentQueue<PutOp<T>> _putq = new();
    internal readonly ConcurrentQueue<GetOp<T>> _getq = new();

    // Object pooling is crucial here. In highly concurrent scenarios (like the ring benchmark), 
    // allocating new Operation objects for every message would cause massive GC pressure.
    // By pooling, we achieve zero-allocation steady-state message passing.
    internal readonly ObjectPool<PutOp<T>> _putPool = ObjectPool.Create<PutOp<T>>();
    internal readonly ObjectPool<GetOp<T>> _getPool = ObjectPool.Create<GetOp<T>>();

    public void PublishSend(SyncState state, int eventId, T value, Action resumePut)
    {
        // FAST PATH: Try to match without enqueueing ourselves
        while (_getq.TryDequeue(out var getOp))
        {
            if (getOp.IsSynchronized) 
            {
                _getPool.Return(getOp);
                continue;
            }

            if (state.TryClaim())
            {
                if (getOp.TrySync())
                {
                    var getResume = getOp.ResumeGet;
                    state.MarkSynchronized(eventId);
                    getOp.State.MarkSynchronized(getOp.EventId);

                    var myResume = resumePut;
                    T capturedValue = value;

                    Scheduler.Enqueue(() => myResume());
                    Scheduler.Enqueue(() => getResume(capturedValue));
                    
                    _getPool.Return(getOp);
                    return;
                }
                else
                {
                    state.ResetClaim();
                    if (getOp.IsSynchronized)
                        _getPool.Return(getOp);
                    else
                    {
                        _getq.Enqueue(getOp); 
                        System.Threading.Thread.Yield();
                    }
                }
            }
            else
            {
                if (getOp.IsSynchronized)
                    _getPool.Return(getOp);
                else
                    _getq.Enqueue(getOp);
                return;
            }
        }

        // SLOW PATH: We must enqueue ourselves and search again to prevent races
        var myOp = _putPool.Get();
        myOp.State = state;
        myOp.EventId = eventId;
        myOp.Value = value;
        myOp.ResumePut = resumePut;

        // We MUST enqueue our operation first before searching the opposing queue.
        // If both a sender and a receiver search before enqueueing, they would both see empty queues, 
        // enqueue themselves, and then wait forever. Enqueueing first ensures at least one will find the other.
        _putq.Enqueue(myOp);

        while (_getq.TryDequeue(out var getOp))
        {
            if (getOp.IsSynchronized) 
            {
                // This operation was already fulfilled by another thread. We discard it.
                _getPool.Return(getOp);
                continue;
            }

            // We must 'Claim' our own state first. This prevents another thread from fulfilling our operation 
            // while we are in the middle of fulfilling this getOp.
            if (state.TryClaim())
            {
                // We successfully claimed our state. Now we try to claim the receiver's state.
                if (getOp.TrySync())
                {
                    // Success! We have atomically paired the Send and Receive operations.
                    var getResume = getOp.ResumeGet;
                    
                    // Mark both states as permanently Synchronized. This also triggers any registered Negative Acknowledgements (NACKs)
                    // for other choices in a 'Cml.Choose' block that lost the race.
                    state.MarkSynchronized(eventId);
                    getOp.State.MarkSynchronized(getOp.EventId);

                    var myResume = resumePut;
                    T capturedValue = value;

                    // We dispatch the continuations to the ThreadPool. We do NOT run them inline because 
                    // inline execution could lead to unbounded stack growth or thread starvation if the continuations block.
                    Scheduler.Enqueue(() => myResume());
                    Scheduler.Enqueue(() => getResume(capturedValue));
                    
                    _getPool.Return(getOp);
                    return;
                }
                else
                {
                    // We failed to sync the receiver's state because another thread is currently inspecting it (Claimed)
                    // or already fulfilled it (Synchronized).
                    // We MUST release our claim so that other threads can interact with our operation.
                    state.ResetClaim();
                    if (getOp.IsSynchronized)
                        _getPool.Return(getOp);
                    else
                    {
                        // The receiver's state was Claimed but not yet Synchronized.
                        // We must put the getOp back in the queue so it isn't lost if the other thread backs off.
                        _getq.Enqueue(getOp); 
                        
                        // CRITICAL: We yield the thread here to prevent a Livelock. 
                        // If we didn't yield, two threads could endlessly dequeue each other's operations, 
                        // fail the TrySync (because both are Claimed), put them back, and repeat forever without making progress.
                        System.Threading.Thread.Yield();
                    }
                }
            }
            else
            {
                // We failed to claim our OWN state. This means another thread found our operation in the queue 
                // and is actively fulfilling it.
                // We MUST put the getOp back in the queue, because we are aborting our search and leaving it unmatched.
                if (getOp.IsSynchronized)
                    _getPool.Return(getOp);
                else
                    _getq.Enqueue(getOp);
                
                // We return immediately. The other thread will handle scheduling our continuation.
                return;
            }
        }
    }

    public void PublishReceive(SyncState state, int eventId, Action<T> resumeGet)
    {
        // FAST PATH: Try to match without enqueueing ourselves
        while (_putq.TryDequeue(out var putOp))
        {
            if (putOp.IsSynchronized) 
            {
                _putPool.Return(putOp);
                continue;
            }

            if (state.TryClaim())
            {
                if (putOp.TrySync())
                {
                    T capturedValue = putOp.Value;
                    var putResume = putOp.ResumePut;

                    state.MarkSynchronized(eventId);
                    putOp.State.MarkSynchronized(putOp.EventId);

                    var myResume = resumeGet;

                    Scheduler.Enqueue(() => putResume());
                    Scheduler.Enqueue(() => myResume(capturedValue));

                    _putPool.Return(putOp);
                    return;
                }
                else
                {
                    state.ResetClaim();
                    if (putOp.IsSynchronized)
                        _putPool.Return(putOp);
                    else
                    {
                        _putq.Enqueue(putOp);
                        System.Threading.Thread.Yield();
                    }
                }
            }
            else
            {
                if (putOp.IsSynchronized)
                    _putPool.Return(putOp);
                else
                    _putq.Enqueue(putOp);
                return;
            }
        }

        // SLOW PATH: Enqueue and search again
        var myOp = _getPool.Get();
        myOp.State = state;
        myOp.EventId = eventId;
        myOp.ResumeGet = resumeGet;

        // Enqueue first to prevent the race condition where sender and receiver miss each other.
        _getq.Enqueue(myOp);

        while (_putq.TryDequeue(out var putOp))
        {
            if (putOp.IsSynchronized) 
            {
                _putPool.Return(putOp);
                continue;
            }

            if (state.TryClaim())
            {
                if (putOp.TrySync())
                {
                    T capturedValue = putOp.Value;
                    var putResume = putOp.ResumePut;

                    // Trigger NACKs for aborted choices in both the sender's and receiver's Sync groups.
                    state.MarkSynchronized(eventId);
                    putOp.State.MarkSynchronized(putOp.EventId);

                    var myResume = resumeGet;

                    Scheduler.Enqueue(() => putResume());
                    Scheduler.Enqueue(() => myResume(capturedValue));

                    _putPool.Return(putOp);
                    return;
                }
                else
                {
                    state.ResetClaim();
                    if (putOp.IsSynchronized)
                        _putPool.Return(putOp);
                    else
                    {
                        _putq.Enqueue(putOp);
                        
                        // Yield to prevent livelock under heavy contention.
                        System.Threading.Thread.Yield();
                    }
                }
            }
            else
            {
                if (putOp.IsSynchronized)
                    _putPool.Return(putOp);
                else
                    _putq.Enqueue(putOp);
                return;
            }
        }
    }
}