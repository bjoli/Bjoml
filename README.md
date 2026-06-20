# BjoML

*Note: This README was written by an AI assistant.*

BjoML is a high-performance, lock-free implementation of Reppy's Concurrent ML (CML) primitive abstractions in C#. 

**Important:** This library is **not intended to be used directly from C# by human developers**. It is specifically designed as a compilation target for a custom language that compiles down to C# state machines (bjoroutines). It is mostly "vibe-coded" to achieve extreme lock-free throughput for automatically generated code, but based on some of my own code from earlier. Only very small chunks of my original code remain, and performance is about an order of magnitude better. 

## Performance and the `Task` Wrapper Limitation

BjoML's core event synchronization pipeline (`Cml.Sync(...)`) is highly optimized, pooling its operation objects to achieve zero garbage collection allocations in its steady state. 

However, standard C# `async/await` and `Task` objects carry significant overhead (heap allocation, ExecutionContext capturing, state machine boxing). If you wrap BjoML channels in `TaskCompletionSource` and use them as standard C# `awaitable` Tasks, you will hit the bottleneck of the .NET ThreadPool infrastructure rather quickly. Even with this heavy `Task` wrapper, the library can still pass roughly ~770,000 messages per second on a Ryzen9 7900x.

When your language compiler targets `Cml.Sync` directly—bypassing `Task` wrappers and generating pure struct-based state machines with direct continuations—the throughput will easily exceed 1.5 to 2 million messages per second.

## Projects

- [Example](Bjoml/Example) - This project demonstrates the actual **compilation target**. It shows what a compiler targeting this library should emit: manual, `Task`-free `IAsyncStateMachine` structs that call `Cml.Sync()` directly with inline continuations to avoid C# infrastructure overhead.
- [StressTest](Bjoml/StressTest) - This project benchmarks the system using standard C# `async/await` wrappers (wrapped in comfort) to easily test load, ring passing, fan-in/fan-out, and the CML combinatorial abstractions (`Choose`, `Wrap`, `WithNack`).


## License

Since I learned of cml from Andy Wingo, and his fantastic guile-fibers, this is licensed under the same license. This is not a derived work, except for possibly the channels which I had a look at long before actually writing anything myself. Most of the algorithms come directly from John Reppy's papers. 

Gnu LGPL v3, to the extent I can actually copyright this. 