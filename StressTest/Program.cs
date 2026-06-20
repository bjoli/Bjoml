using System;
using System.Diagnostics;
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
        await RunProducerConsumerBenchmark();

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
                        if (msg % 100 == 0) Console.WriteLine($"Completed {msg} trips...");
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
}
