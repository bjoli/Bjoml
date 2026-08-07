// BjoML counterpart to bench/go/main.go.
//
// Every benchmark is shaped identically to its Go twin: same counts, same channel
// topology, same work per unit. Allocation per operation is reported alongside time,
// because that is the diagnostic that actually explains a gap against goroutines.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Bjoml;

namespace Bjoml.Bench;

public static class Program
{
    public static async Task Main()
    {
        Scheduler.Start();

        Console.WriteLine($".NET {Environment.Version}, ProcessorCount={Environment.ProcessorCount}, " +
                          $"ServerGC={System.Runtime.GCSettings.IsServerGC}");
        Console.WriteLine();

        // Warm up the JIT so the first benchmark is not paying for everything.
        await Warmup();

        await SpawnStorm();
        await SpawnAndSend();
        await PingPong();
        await Ring();
        await SelectChoose();
        await FanOut();
    }

    static async Task Warmup()
    {
        for (int i = 0; i < 200; i++)
        {
            var ch = new Channel<int>();
            var p = Bjo.Spawn(() => Trivial(ch));
            await Cml.SyncAsyncVoid(new ChannelSendEvent<int>(ch, 1));
            await p.ToTask();
        }
    }

    static async Fiber Trivial(Channel<int> ch) => await ch.Receive();

    // ---------------------------------------------------------------------
    // 1. Spawn storm
    // ---------------------------------------------------------------------

    static long _counter;
    static int _remaining;
    static readonly ManualResetEventSlim _allDone = new(false);

    // Matches Go's `go func(){ atomic.AddInt64(...); wg.Done() }()` exactly: an
    // atomic bump and a completion signal, no rendezvous. An async method with no
    // await is legal and the compiler emits a synchronous fast path, which is the
    // honest equivalent of a goroutine that never blocks.
#pragma warning disable CS1998
    static async Fiber Bump()
    {
        Interlocked.Increment(ref _counter);
        if (Interlocked.Decrement(ref _remaining) == 0) _allDone.Set();
    }
#pragma warning restore CS1998

    static Task SpawnStorm()
    {
        const int n = 1_000_000;
        _counter = 0;
        _remaining = n;
        _allDone.Reset();

        // Hoisted so we allocate one delegate, not one per spawn — Go's closure is
        // likewise loop-invariant here because it captures nothing.
        Func<Fiber> body = () => Bump();

        long before = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();

        for (int i = 0; i < n; i++) Bjo.Spawn(body);
        _allDone.Wait();

        sw.Stop();
        long alloc = GC.GetTotalAllocatedBytes(precise: true) - before;

        Report("Spawn storm", n, sw, alloc, $"counter={Interlocked.Read(ref _counter)}");
        return Task.CompletedTask;
    }

    // ---------------------------------------------------------------------
    // 2. Spawn + one channel send each
    // ---------------------------------------------------------------------

    static async Fiber SendOne(Channel<int> ch, int v) => await ch.Send(v);

    static async Task SpawnAndSend()
    {
        const int n = 200_000;
        var ch = new Channel<int>();

        long before = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();

        for (int i = 0; i < n; i++)
        {
            int v = i;
            Bjo.Spawn(() => SendOne(ch, v));
        }

        long total = 0;
        for (int i = 0; i < n; i++) total += await Cml.SyncAsync(new ChannelReceiveEvent<int>(ch));

        sw.Stop();
        long alloc = GC.GetTotalAllocatedBytes(precise: true) - before;

        Report("Spawn+send", n, sw, alloc, $"total={total}");
    }

    // ---------------------------------------------------------------------
    // 3. Ping-pong
    // ---------------------------------------------------------------------

    static async Fiber Ponger(Channel<int> a, Channel<int> b, int rounds)
    {
        for (int i = 0; i < rounds; i++)
        {
            int v = await a.Receive();
            await b.Send(v * 2);
        }
    }

    static async Fiber Pinger(Channel<int> a, Channel<int> b, int rounds)
    {
        for (int i = 0; i < rounds; i++)
        {
            await a.Send(i);
            await b.Receive();
        }
    }

    static async Task PingPong()
    {
        const int rounds = 1_000_000;
        var a = new Channel<int>();
        var b = new Channel<int>();

        var pong = Bjo.Spawn(() => Ponger(a, b, rounds));

        long before = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();

        var ping = Bjo.Spawn(() => Pinger(a, b, rounds));
        await pong.ToTask();
        await ping.ToTask();

        sw.Stop();
        long alloc = GC.GetTotalAllocatedBytes(precise: true) - before;

        Report("Ping-pong", rounds, sw, alloc, "round trips");
    }

    // ---------------------------------------------------------------------
    // 4. Ring
    // ---------------------------------------------------------------------

    static async Fiber RingNode(Channel<int> inCh, Channel<int> outCh, bool isLast, int numTrips)
    {
        while (true)
        {
            int msg = await inCh.Receive();

            if (msg == -1)
            {
                if (!isLast) await outCh.Send(-1);
                return;
            }

            if (isLast)
            {
                msg++;
                if (msg >= numTrips)
                {
                    await outCh.Send(-1);
                    continue;
                }
            }

            await outCh.Send(msg);
        }
    }

    static async Task Ring()
    {
        const int numWorkers = 1000;
        const int numTrips = 1000;

        var channels = new Channel<int>[numWorkers];
        for (int i = 0; i < numWorkers; i++) channels[i] = new Channel<int>();

        var handles = new Promise<Unit>[numWorkers];
        for (int i = 0; i < numWorkers; i++)
        {
            var inCh = channels[i];
            var outCh = channels[(i + 1) % numWorkers];
            bool isLast = i == numWorkers - 1;
            handles[i] = Bjo.Spawn(() => RingNode(inCh, outCh, isLast, numTrips));
        }

        long before = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();

        await Cml.SyncAsyncVoid(new ChannelSendEvent<int>(channels[0], 0));
        foreach (var h in handles) await h.ToTask();

        sw.Stop();
        long alloc = GC.GetTotalAllocatedBytes(precise: true) - before;

        Report("Ring", numWorkers * numTrips, sw, alloc, "messages");
    }

    // ---------------------------------------------------------------------
    // 5. Select / Choose
    // ---------------------------------------------------------------------

    static async Fiber SelectSender(Channel<int> a, Channel<int> b, int rounds)
    {
        for (int i = 0; i < rounds; i++)
        {
            if (i % 2 == 0) await a.Send(i);
            else await b.Send(i);
        }
    }

    static async Task SelectChoose()
    {
        const int rounds = 1_000_000;
        var a = new Channel<int>();
        var b = new Channel<int>();

        var sender = Bjo.Spawn(() => SelectSender(a, b, rounds));

        long before = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();

        var choose = Cml.Choose(a, b);
        for (int i = 0; i < rounds; i++)
        {
            await Cml.SyncAsync(choose);
        }

        await sender.ToTask();
        sw.Stop();
        long alloc = GC.GetTotalAllocatedBytes(precise: true) - before;

        Report("Select/Choose", rounds, sw, alloc, "ops");
    }

    // ---------------------------------------------------------------------
    // 6. Fan-out
    // ---------------------------------------------------------------------

    static async Fiber Burn(int iterations)
    {
        BurnCore(iterations);
        await Cml.Always(0);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    static void BurnCore(int iterations)
    {
        double acc = 0;
        for (int i = 1; i <= iterations; i++) acc += 1.0 / i;
        _sink = acc;
    }

    static double _sink;

    static async Fiber FanOutParent(int numChildren, int iterations, Channel<int> done)
    {
        for (int i = 0; i < numChildren; i++)
        {
            var child = Bjo.Spawn(() => Burn(iterations));
            Bjo.Spawn(() => Notify(child, done));
        }

        for (int i = 0; i < numChildren; i++) await done.Receive();
    }

    static async Fiber Notify(Promise<Unit> child, Channel<int> done)
    {
        await child;
        await done.Send(1);
    }

    static async Task FanOut()
    {
        const int numChildren = 480;
        const int iterations = 3_000_000;

        var serialSw = Stopwatch.StartNew();
        BurnCore(iterations);
        serialSw.Stop();
        double serialTotalMs = serialSw.Elapsed.TotalMilliseconds * numChildren;

        var done = new Channel<int>();
        var sw = Stopwatch.StartNew();
        await Bjo.Spawn(() => FanOutParent(numChildren, iterations, done)).ToTask();
        sw.Stop();

        Console.WriteLine(
            $"Fan-out:          {numChildren} children in {sw.ElapsedMilliseconds} ms " +
            $"(serial reference {serialTotalMs:F0} ms, speedup {serialTotalMs / sw.Elapsed.TotalMilliseconds:F1}x)");
    }

    // ---------------------------------------------------------------------

    static void Report(string name, int n, Stopwatch sw, long allocBytes, string extra)
    {
        double nsPerOp = sw.Elapsed.TotalMilliseconds * 1_000_000.0 / n;
        Console.WriteLine(
            $"{name,-17} {n} in {sw.ElapsedMilliseconds} ms " +
            $"({nsPerOp:F0} ns/op, {allocBytes / (double)n:F0} B/op), {extra}");
    }
}
