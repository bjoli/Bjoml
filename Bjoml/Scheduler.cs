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

    public bool TryClaim() => Interlocked.CompareExchange(ref _value, C, W) == W;
    public bool TrySync() => Interlocked.CompareExchange(ref _value, S, W) == W;
    public void MarkSynchronized() => Volatile.Write(ref _value, S);
    public void ResetClaim() => Volatile.Write(ref _value, W);
    public bool IsSynchronized => Volatile.Read(ref _value) == S;
}