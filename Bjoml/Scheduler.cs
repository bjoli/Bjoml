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

namespace Bjoml;

using System;
using System.Threading;
using System.Collections.Concurrent;
using Microsoft.Extensions.ObjectPool;

public static class Scheduler
{
    private static Worker[] _workers;
    private static int _nextWorker = 0;

    [ThreadStatic]
    internal static Worker? CurrentWorker;

    [ThreadStatic]
    internal static int InlineDepth;

    public static void Start(int numWorkers = 0)
    {
        if (numWorkers <= 0) numWorkers = Environment.ProcessorCount;
        _workers = new Worker[numWorkers];
        for (int i = 0; i < numWorkers; i++)
        {
            _workers[i] = new Worker(i);
        }
    }

    public static void Enqueue(Action work)
    {
        var worker = CurrentWorker;
        if (worker != null)
        {
            worker.Enqueue(work);
        }
        else
        {
            if (_workers == null) Start(); // Lazy init if missed
            int index = Interlocked.Increment(ref _nextWorker) % _workers.Length;
            if (index < 0) index += _workers.Length;
            _workers[index].Enqueue(work);
        }
    }

    public static void Dispatch(Action action)
    {
        if (InlineDepth < 50)
        {
            InlineDepth++;
            try { action(); }
            finally { InlineDepth--; }
        }
        else
        {
            Enqueue(action);
        }
    }

    public static void Dispatch<T>(Action<T> action, T state)
    {
        if (InlineDepth < 50)
        {
            InlineDepth++;
            try { action(state); }
            finally { InlineDepth--; }
        }
        else
        {
            Enqueue(() => action(state));
        }
    }
}

internal class Worker
{
    private readonly BlockingCollection<Action> _queue = new BlockingCollection<Action>(new ConcurrentQueue<Action>());
    private readonly Thread _thread;

    public Worker(int id)
    {
        _thread = new Thread(RunLoop)
        {
            Name = $"BjoML-Worker-{id}",
            IsBackground = true
        };
        _thread.Start();
    }

    public void Enqueue(Action work) => _queue.Add(work);

    private void RunLoop()
    {
        Scheduler.CurrentWorker = this;
        foreach (var action in _queue.GetConsumingEnumerable())
        {
            try
            {
                Scheduler.InlineDepth = 0;
                action();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Worker unhandled exception: {ex}");
            }
        }
    }
}

public class SyncState
{
    // The core lock-free state machine for event synchronization.
    // W = Waiting. The event is pending and can be claimed by any matching thread.
    // C = Claimed. A matching thread has found this event and is currently inspecting it to pair it. 
    //              It prevents other threads from interfering during the pairing process.
    // S = Synchronized. The event has successfully been paired and is permanently completed.
    public const int W = 0; 
    public const int C = 1; 
    public const int S = 2; 

    private int _value = W;
    private int _eventIdCounter = 0;
    
    // NACK callbacks mapped by EventId.
    // Why keep a dictionary? In a 'Choose' block, multiple events share the same SyncState.
    // Only one event can win. When the winner claims the state, we must fire the NACKs for ALL OTHER events.
    private readonly System.Collections.Generic.Dictionary<int, Action> _nacks = new();

    public int GenerateEventId() => Interlocked.Increment(ref _eventIdCounter);

    // TryClaim transitions W -> C. If it fails, another thread is either actively inspecting (C) or already finished (S).
    public bool TryClaim() => Interlocked.CompareExchange(ref _value, C, W) == W;
    
    // TrySync transitions W -> S. This is called by a thread that has already Claimed its OWN state, 
    // and is now atomically finalizing the match with the opposing thread's state.
    public bool TrySync() => Interlocked.CompareExchange(ref _value, S, W) == W;
    
    public void MarkSynchronized(int winningEventId)
    {
        Volatile.Write(ref _value, S);
        Action[] toFire = null;
        
        // We lock around NACK resolution because 'RegisterNack' might be called concurrently 
        // by another branch of the 'Choose' block that is still evaluating.
        lock (_nacks)
        {
            if (_nacks.Count > 0)
            {
                var list = new System.Collections.Generic.List<Action>();
                foreach (var kvp in _nacks)
                {
                    // We DO NOT fire the NACK for the winning event, because its branch succeeded!
                    if (kvp.Key != winningEventId) list.Add(kvp.Value);
                }
                toFire = list.ToArray();
                _nacks.Clear();
            }
        }
        
        if (toFire != null)
        {
            // NACKs are fired asynchronously on the ThreadPool to avoid deadlocking 
            // or throwing exceptions inline during the critical synchronization phase.
            foreach (var a in toFire) Scheduler.Enqueue(a);
        }
    }

    public void ResetClaim() => Volatile.Write(ref _value, W);
    public bool IsSynchronized => Volatile.Read(ref _value) == S;

    public void RegisterNack(int eventId, Action nack)
    {
        lock (_nacks)
        {
            if (IsSynchronized)
            {
                // Race condition handled: If the SyncState was already synchronized (because another branch won 
                // BEFORE we even finished publishing this branch), we must fire this NACK immediately.
                Scheduler.Enqueue(nack);
                return;
            }
            _nacks[eventId] = nack;
        }
    }
}