namespace Bjoml;

using System;
using System.Threading;
using System.Collections.Concurrent;
using Microsoft.Extensions.ObjectPool;

public static class Scheduler
{
    public static void Enqueue(Action work) => ThreadPool.QueueUserWorkItem(_ => work());
    public static void Start(int minWorkers = 0)
    {
        if (minWorkers > 0)
        {
            ThreadPool.GetMinThreads(out int currentMinWorker, out int currentMinIOC);
            ThreadPool.SetMinThreads(Math.Max(minWorkers, currentMinWorker), currentMinIOC);
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