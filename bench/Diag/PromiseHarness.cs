// Focused measurement of the promise complete->wake path.
//
// Every fiber join and every promise inside a choose goes through
// Promise.Complete signalling its waiters. The suspected costs, to be priced
// one change at a time:
//
//   - Scheduler.Enqueue(waiter.Signal): the method-group conversion allocates a
//     fresh 64 B Action per wake.
//   - OnCompleted(Action) wraps the caller's delegate in an ActionWaiter (~24 B)
//     just to fit the IPromiseWaiter shape.
//
// One cycle = new Promise, register a continuation, complete it, and the
// continuation decrements a countdown from a pool thread. The registration
// happens BEFORE completion, so the wake goes through the Complete path (the
// enqueue), not the already-completed inline path.

using System;
using System.Diagnostics;
using System.Threading;
using Bjoml;

namespace Bjoml.Diag;

public static class PromiseHarness
{
    public static void Run(int reps)
    {
        Console.WriteLine($".NET {Environment.Version}  ProcessorCount={Environment.ProcessorCount}  " +
                          $"ServerGC={System.Runtime.GCSettings.IsServerGC}");
        Console.WriteLine($"reps={reps}   (complete -> wake, per op)");
        Console.WriteLine();

        Once(100_000, out _);              // warm up

        var samples = new double[reps];
        double alloc = 0;
        for (int r = 0; r < reps; r++) samples[r] = Once(1_000_000, out alloc);

        var sorted = (double[])samples.Clone();
        Array.Sort(sorted);

        Console.WriteLine($"Promise wake   per rep: {string.Join(" ", Array.ConvertAll(samples, s => $"{s,6:F0}"))}");
        Console.WriteLine($"               median : {sorted[reps / 2]:F0} ns/op   ({alloc:F0} B/op)");
    }

    static int s_remaining;
    static readonly ManualResetEventSlim s_done = new(false);

    static double Once(int rounds, out double bytesPerOp)
    {
        s_remaining = rounds;
        s_done.Reset();

        // One continuation delegate for the whole run, so the harness itself
        // allocates nothing per op.
        Action k = static () =>
        {
            if (Interlocked.Decrement(ref s_remaining) == 0) s_done.Set();
        };

        long before = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();

        for (int i = 0; i < rounds; i++)
        {
            var p = new Promise<int>();
            p.GetAwaiter().OnCompleted(k);
            p.TrySetResult(i);
        }

        s_done.Wait();
        sw.Stop();

        bytesPerOp = (GC.GetTotalAllocatedBytes(precise: true) - before) / (double)rounds;
        return sw.Elapsed.TotalMilliseconds * 1e6 / rounds;
    }
}
