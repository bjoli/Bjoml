// Focused Select/Choose measurement.
//
// The full suite's Select row spreads 166-224 ns across runs, which cannot resolve
// the ~70 ns question this is meant to answer: whether the gap to Hopac (211 vs 141)
// is the two unconditional Monitor acquisitions that SyncState.MarkSynchronized
// performs per rendezvous, one for each side, to walk a nack list that is empty in
// this benchmark.
//
// So: one benchmark, many reps, median reported, nothing else running.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Bjoml;

namespace Bjoml.Diag;

public static class SelectHarness
{
    public static void Run(int reps)
    {
        Console.WriteLine($".NET {Environment.Version}  ProcessorCount={Environment.ProcessorCount}  " +
                          $"ServerGC={System.Runtime.GCSettings.IsServerGC}");
        Console.WriteLine($"reps={reps}");
        Console.WriteLine();

        RunOnce(200_000, out _);          // warm up JIT and the op free-lists

        var samples = new double[reps];
        double alloc = 0;
        for (int r = 0; r < reps; r++)
        {
            samples[r] = RunOnce(1_000_000, out alloc);
        }

        var sorted = (double[])samples.Clone();
        Array.Sort(sorted);

        Console.WriteLine($"Select/Choose  per rep: {string.Join(" ", Array.ConvertAll(samples, s => $"{s,6:F0}"))}");
        Console.WriteLine($"               median : {sorted[reps / 2]:F0} ns/op   ({alloc:F0} B/op)");
        Console.WriteLine();
        Console.WriteLine("Reference on this machine: Hopac 141 ns/op (528 B/op), Go 134 ns/op.");
    }

    static double RunOnce(int rounds, out double bytesPerOp)
    {
        var a = new Channel<int>();
        var b = new Channel<int>();

        var sender = Bjo.Spawn(() => Sender(a, b, rounds));

        // Hoisted, exactly as in bench/Bench and in the Hopac version.
        var choose = Cml.Choose(a, b);

        long before = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();

        Receive(choose, rounds).GetAwaiter().GetResult();
        sender.ToTask().GetAwaiter().GetResult();

        sw.Stop();
        bytesPerOp = (GC.GetTotalAllocatedBytes(precise: true) - before) / (double)rounds;

        return sw.Elapsed.TotalMilliseconds * 1e6 / rounds;
    }

    static async Task Receive(IEvent<int> choose, int rounds)
    {
        for (int i = 0; i < rounds; i++) await Cml.SyncAsync(choose);
    }

    static async Fiber Sender(Channel<int> a, Channel<int> b, int rounds)
    {
        for (int i = 0; i < rounds; i++)
        {
            if (i % 2 == 0) await a.Send(i);
            else await b.Send(i);
        }
    }
}
