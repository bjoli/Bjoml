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
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Bjoml;

namespace StressTest;

class Program
{
    static async Task Main(string[] args)
    {
        Scheduler.Start();

        Console.WriteLine("Starting Bjoml CML Stress Tests...\n");

        await RunRingBenchmark();
        Console.WriteLine();
        await RunSimpleRingBenchmark();
        Console.WriteLine();
        await RunProducerConsumerBenchmark();
        Console.WriteLine();
        await RunSimpleProducerConsumerBenchmark();
        Console.WriteLine();
        await RunFiberRingBenchmark();
        Console.WriteLine();
        await RunFiberSimpleRingBenchmark();
        Console.WriteLine();
        await RunFanOutBenchmark();
        Console.WriteLine();
        await RunCombinatorTest();

        Console.WriteLine("\nAll tests completed.");
    }

    static async Task RunRingBenchmark()
    {
        const int numWorkers = 1000;
        const int numTrips = 1000;

        Console.WriteLine($"--- Ring Benchmark: {numWorkers} workers, {numTrips} trips around the ring ---");
        
        var channels = new Channel<int>[numWorkers];
        for (int i = 0; i < numWorkers; i++)
        {
            channels[i] = new Channel<int>();
        }

        var tasks = new Task[numWorkers];

        // Create the workers
        for (int i = 0; i < numWorkers; i++)
        {
            int workerId = i;
            var inChannel = channels[workerId];
            var outChannel = channels[(workerId + 1) % numWorkers];

            tasks[i] = Task.Run(async () =>
            {
                while (true)
                {
                    int msg = await inChannel.GetMessage();
                    if (msg == -1) // Poison pill
                    {
                        if (workerId != numWorkers - 1) 
                        {
                            await outChannel.PutMessage(-1);
                        }
                        break;
                    }
                    
                    if (workerId == numWorkers - 1)
                    {
                        // Completed a trip
                        msg++;
                        if (msg >= numTrips)
                        {
                            // Reached the limit, send poison pill
                            await outChannel.PutMessage(-1);
                            continue;
                        }
                    }

                    await outChannel.PutMessage(msg);
                }
            });
        }

        var sw = Stopwatch.StartNew();
        
        // Inject the first message at worker 0
        await channels[0].PutMessage(0);

        await Task.WhenAll(tasks);

        sw.Stop();
        Console.WriteLine($"Ring Benchmark finished in {sw.ElapsedMilliseconds} ms. Passed {numWorkers * numTrips} messages.");
    }

    static async Task RunProducerConsumerBenchmark()
    {
        const int numProducers = 100;
        const int numConsumers = 100;
        const int messagesPerProducer = 5000;

        Console.WriteLine($"--- Fan-In / Fan-Out: {numProducers} Producers, {numConsumers} Consumers, {messagesPerProducer} msgs each ---");

        var sharedChannel = new Channel<int>();

        var producers = new Task[numProducers];
        var consumers = new Task[numConsumers];
        
        int totalReceived = 0;

        var sw = Stopwatch.StartNew();

        // Start Consumers
        for (int i = 0; i < numConsumers; i++)
        {
            consumers[i] = Task.Run(async () =>
            {
                while (true)
                {
                    int msg = await sharedChannel.GetMessage();
                    if (msg == -1) break;
                    System.Threading.Interlocked.Increment(ref totalReceived);
                }
            });
        }

        // Start Producers
        for (int i = 0; i < numProducers; i++)
        {
            producers[i] = Task.Run(async () =>
            {
                for (int j = 0; j < messagesPerProducer; j++)
                {
                    await sharedChannel.PutMessage(j);
                }
            });
        }

        await Task.WhenAll(producers);

        // Send poison pills
        for (int i = 0; i < numConsumers; i++)
        {
            await sharedChannel.PutMessage(-1);
        }

        await Task.WhenAll(consumers);

        sw.Stop();
        Console.WriteLine($"Producer/Consumer finished in {sw.ElapsedMilliseconds} ms. Total Messages Processed: {totalReceived}");
    }

    static async Task RunSimpleRingBenchmark()
    {
        const int numWorkers = 1000;
        const int numTrips = 1000;

        Console.WriteLine($"--- SimpleChannel Ring Benchmark: {numWorkers} workers, {numTrips} trips around the ring ---");
        
        var channels = new SimpleChannel<int>[numWorkers];
        for (int i = 0; i < numWorkers; i++)
        {
            channels[i] = new SimpleChannel<int>();
        }

        var tasks = new Task[numWorkers];

        for (int i = 0; i < numWorkers; i++)
        {
            int workerId = i;
            var inChannel = channels[workerId];
            var outChannel = channels[(workerId + 1) % numWorkers];

            tasks[i] = Task.Run(async () =>
            {
                while (true)
                {
                    int msg = await inChannel.GetMessage();
                    if (msg == -1) 
                    {
                        if (workerId != numWorkers - 1) 
                        {
                            await outChannel.PutMessage(-1);
                        }
                        break;
                    }
                    
                    if (workerId == numWorkers - 1)
                    {
                        msg++;
                        if (msg >= numTrips)
                        {
                            await outChannel.PutMessage(-1);
                            continue;
                        }
                    }

                    await outChannel.PutMessage(msg);
                }
            });
        }

        var sw = Stopwatch.StartNew();
        await channels[0].PutMessage(0);
        await Task.WhenAll(tasks);
        sw.Stop();
        Console.WriteLine($"SimpleChannel Ring Benchmark finished in {sw.ElapsedMilliseconds} ms. Passed {numWorkers * numTrips} messages.");
    }

    static async Task RunSimpleProducerConsumerBenchmark()
    {
        const int numProducers = 100;
        const int numConsumers = 100;
        const int messagesPerProducer = 5000;

        Console.WriteLine($"--- SimpleChannel Fan-In / Fan-Out: {numProducers} Producers, {numConsumers} Consumers, {messagesPerProducer} msgs each ---");

        var sharedChannel = new SimpleChannel<int>();
        var producers = new Task[numProducers];
        var consumers = new Task[numConsumers];
        
        int totalReceived = 0;

        var sw = Stopwatch.StartNew();

        for (int i = 0; i < numConsumers; i++)
        {
            consumers[i] = Task.Run(async () =>
            {
                while (true)
                {
                    int msg = await sharedChannel.GetMessage();
                    if (msg == -1) break;
                    System.Threading.Interlocked.Increment(ref totalReceived);
                }
            });
        }

        for (int i = 0; i < numProducers; i++)
        {
            producers[i] = Task.Run(async () =>
            {
                for (int j = 0; j < messagesPerProducer; j++)
                {
                    await sharedChannel.PutMessage(j);
                }
            });
        }

        await Task.WhenAll(producers);

        for (int i = 0; i < numConsumers; i++)
        {
            await sharedChannel.PutMessage(-1);
        }

        await Task.WhenAll(consumers);
        sw.Stop();
        Console.WriteLine($"SimpleChannel Producer/Consumer finished in {sw.ElapsedMilliseconds} ms. Total Messages Processed: {totalReceived}");
    }

    /// <summary>
    /// The same ring, but every node is a Fiber awaiting IEvent directly instead of a
    /// Task awaiting a pooled ValueTask source. This is the path the compiled language
    /// actually uses, so it is the number that matters.
    /// </summary>
    static async Task RunFiberRingBenchmark()
    {
        const int numWorkers = 1000;
        const int numTrips = 1000;

        Console.WriteLine($"--- Fiber Ring Benchmark: {numWorkers} workers, {numTrips} trips around the ring ---");

        var channels = new Channel<int>[numWorkers];
        for (int i = 0; i < numWorkers; i++) channels[i] = new Channel<int>();

        var handles = new Promise<Unit>[numWorkers];

        for (int i = 0; i < numWorkers; i++)
        {
            int workerId = i;
            var inChannel = channels[workerId];
            var outChannel = channels[(workerId + 1) % numWorkers];
            bool isLast = workerId == numWorkers - 1;

            handles[i] = Bjo.Spawn(() => RingNode(inChannel, outChannel, isLast, numTrips));
        }

        var sw = Stopwatch.StartNew();

        await Cml.SyncAsyncVoid(new ChannelSendEvent<int>(channels[0], 0));

        foreach (var h in handles) await h.ToTask();

        sw.Stop();
        Console.WriteLine($"Fiber Ring Benchmark finished in {sw.ElapsedMilliseconds} ms. Passed {numWorkers * numTrips} messages.");
    }

    static async Fiber RingNode(Channel<int> inChannel, Channel<int> outChannel, bool isLast, int numTrips)
    {
        while (true)
        {
            int msg = await inChannel.Receive();

            if (msg == -1)
            {
                if (!isLast) await outChannel.Send(-1);
                return;
            }

            if (isLast)
            {
                msg++;
                if (msg >= numTrips)
                {
                    await outChannel.Send(-1);
                    continue;
                }
            }

            await outChannel.Send(msg);
        }
    }

    static async Task RunFiberSimpleRingBenchmark()
    {
        const int numWorkers = 1000;
        const int numTrips = 1000;

        Console.WriteLine($"--- Fiber SimpleChannel Ring Benchmark: {numWorkers} workers, {numTrips} trips around the ring ---");

        var channels = new SimpleChannel<int>[numWorkers];
        for (int i = 0; i < numWorkers; i++) channels[i] = new SimpleChannel<int>();

        var handles = new Promise<Unit>[numWorkers];

        for (int i = 0; i < numWorkers; i++)
        {
            int workerId = i;
            var inChannel = channels[workerId];
            var outChannel = channels[(workerId + 1) % numWorkers];
            bool isLast = workerId == numWorkers - 1;

            handles[i] = Bjo.Spawn(() => SimpleRingNode(inChannel, outChannel, isLast, numTrips));
        }

        var sw = Stopwatch.StartNew();

        await channels[0].PutMessage(0);

        foreach (var h in handles) await h.ToTask();

        sw.Stop();
        Console.WriteLine($"Fiber SimpleChannel Ring Benchmark finished in {sw.ElapsedMilliseconds} ms. Passed {numWorkers * numTrips} messages.");
    }

    static async Fiber SimpleRingNode(SimpleChannel<int> inChannel, SimpleChannel<int> outChannel, bool isLast, int numTrips)
    {
        while (true)
        {
            int msg = await inChannel.GetMessage();

            if (msg == -1)
            {
                if (!isLast) await outChannel.PutMessage(-1);
                return;
            }

            if (isLast)
            {
                msg++;
                if (msg >= numTrips)
                {
                    await outChannel.PutMessage(-1);
                    continue;
                }
            }

            await outChannel.PutMessage(msg);
        }
    }

    /// <summary>
    /// The workload the old scheduler could not do: one fiber spawning many children.
    ///
    /// With per-worker queues and no stealing, a fiber running on worker 3 enqueued
    /// every child onto worker 3's own queue, so the whole fan-out ran on ONE core
    /// while the others sat blocked in GetConsumingEnumerable. On the .NET pool the
    /// children are stealable, so this should scale with core count.
    /// </summary>
    static async Task RunFanOutBenchmark()
    {
        const int numChildren = 480;
        const int iterationsPerChild = 3_000_000;

        Console.WriteLine($"--- Fan-Out: 1 fiber spawns {numChildren} children, {iterationsPerChild} iterations of CPU work each ---");

        // Serial reference, so the speedup claim is measured rather than assumed.
        var serialSw = Stopwatch.StartNew();
        BurnCore(iterationsPerChild);
        serialSw.Stop();
        double serialTotalMs = serialSw.Elapsed.TotalMilliseconds * numChildren;

        var sw = Stopwatch.StartNew();
        await Bjo.Spawn(() => FanOutParent(numChildren, iterationsPerChild)).ToTask();
        sw.Stop();

        Console.WriteLine(
            $"Fan-Out finished in {sw.ElapsedMilliseconds} ms on {Environment.ProcessorCount} logical cores " +
            $"(serial reference {serialTotalMs:F0} ms, speedup {serialTotalMs / sw.Elapsed.TotalMilliseconds:F1}x).");
    }

    static async Fiber FanOutParent(int numChildren, int iterations)
    {
        var children = new Promise<Unit>[numChildren];

        // Spawned from INSIDE a fiber, which is the case the old scheduler pinned.
        for (int i = 0; i < numChildren; i++)
            children[i] = Bjo.Spawn(() => Burn(iterations));

        var doneChannel = new Channel<int>();

        for (int i = 0; i < numChildren; i++)
        {
            var child = children[i];
            Bjo.Spawn(() => Notify(child, doneChannel));
        }

        for (int i = 0; i < numChildren; i++)
            await doneChannel.Receive();
    }

    static async Fiber Notify(Promise<Unit> child, Channel<int> done)
    {
        await child;
        await done.Send(1);
    }

    static async Fiber Burn(int iterations)
    {
        // Deliberately synchronous CPU work with no suspension, so the only thing
        // being measured is whether the scheduler spread the children across cores.
        BurnCore(iterations);
        await Cml.Always(0);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    static void BurnCore(int iterations)
    {
        double acc = 0;
        for (int i = 1; i <= iterations; i++) acc += 1.0 / i;
        if (acc < 0) throw new Exception("unreachable");
    }

    static async Task RunCombinatorTest()
    {
        Console.WriteLine("--- CML Combinators Test (Choose, Wrap, WithNack) ---");
        var chan1 = new Channel<string>();
        var chan2 = new Channel<string>();

        // We will do a choose between receiving from chan1 or chan2.
        // We will wrap both to uppercase.
        // We will add a NACK to chan1 so if we receive from chan2 instead, chan1's NACK will fire.

        bool nackFired = false;

        var ev1 = Cml.WithNack(nackChan => 
        {
            // Background task to listen for the NACK
            Task.Run(async () => 
            {
                await Cml.SyncAsync(nackChan);
                nackFired = true;
            });

            return Cml.Wrap(new ChannelReceiveEvent<string>(chan1), s => s.ToUpper() + " (from chan1)");
        });

        var ev2 = Cml.Wrap(new ChannelReceiveEvent<string>(chan2), s => s.ToUpper() + " (from chan2)");

        var choice = Cml.Choose(ev1, ev2);

        // Send to chan2 to ensure ev2 wins
        var sendTask = chan2.PutMessage("hello");
        
        var result = await Cml.SyncAsync(choice);
        await sendTask;

        Console.WriteLine($"Result: {result}");
        
        // Wait briefly to allow NACK to fire on the ThreadPool
        await Task.Delay(100);
        Console.WriteLine($"Nack fired for chan1? {nackFired}");
    }
}
