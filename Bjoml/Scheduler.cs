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
    public const int W = 0; // Waiting
    public const int C = 1; // Claimed
    public const int S = 2; // Synchronized

    private int _value = W;
    private int _eventIdCounter = 0;
    private readonly System.Collections.Generic.Dictionary<int, Action> _nacks = new();

    public int GenerateEventId() => Interlocked.Increment(ref _eventIdCounter);

    public bool TryClaim() => Interlocked.CompareExchange(ref _value, C, W) == W;
    public bool TrySync() => Interlocked.CompareExchange(ref _value, S, W) == W;
    
    public void MarkSynchronized(int winningEventId)
    {
        Volatile.Write(ref _value, S);
        Action[] toFire = null;
        lock (_nacks)
        {
            if (_nacks.Count > 0)
            {
                var list = new System.Collections.Generic.List<Action>();
                foreach (var kvp in _nacks)
                {
                    if (kvp.Key != winningEventId) list.Add(kvp.Value);
                }
                toFire = list.ToArray();
                _nacks.Clear();
            }
        }
        if (toFire != null)
        {
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
                Scheduler.Enqueue(nack);
                return;
            }
            _nacks[eventId] = nack;
        }
    }
}