# BjoML

*Note: This README was mostly written by an AI assistant.*

BjoML is a high-performance, mostly  lock-free implementation of Reppy's Concurrent ML (CML) primitive abstractions in C#. 

**Important:** This library is **not intended to be used directly from C# by human developers**. It is specifically designed as a compilation target for a custom language that compiles down to C# state machines (bjoroutines). It is mostly "vibe-coded", but based on some of my own code from earlier. Only very small chunks of my original code remain, and performance is about an order of magnitude better. If you try to use this, note that AsyncLocals will not work, at least not in future releases. 

## The Fiber API

A compiled bjoroutine returns `Fiber` / `Fiber<T>`, driven by a custom
`AsyncMethodBuilder`. `(sync ev)` compiles to `await ev`, and nothing else in the
language suspends:

```csharp
static async Fiber Echo(Channel<int> inCh, Channel<int> outCh)
{
    while (true)
    {
        int x = await inCh.Receive();   // (sync (channel-get in))
        await outCh.Send(x + 1);        // (sync (channel-put out ...))
    }
}
```

`Bjo.Spawn` returns a `Promise<T>`, **not** a `Fiber<T>`. A fiber is a compiler
artifact; a promise is first-class and composable with `choose`, which a `Task` can
never be:

```csharp
var r = await Cml.Choose(
    Cml.Wrap(p.Join(),        _ => "done"),
    Cml.Wrap(abort.Receive(), _ => "aborted"));
```

The builder carries the language's dynamic environment in `FiberContext.Current`, an
opaque `[ThreadStatic] object?` that BjoML only ever saves and restores — a pointer
swap. It is re-captured at every suspension and reinstated on resume, with the
borrowed thread's own context restored in a `finally`.

**ExecutionContext is never flowed anywhere in the runtime** — every enqueue is an
`UnsafeQueueUserWorkItem`, every await an unsafe await. So `AsyncLocal`, `Activity`
spans and similar C# ambient state do **not** survive a BjoML await. Put anything
ambient you need into your own context object.

See [docs/design.md](docs/design.md) for the full design and the list of known issues.

## Performance

The scheduler runs on the .NET thread pool via
`ThreadPool.UnsafeQueueUserWorkItem(IThreadPoolWorkItem, preferLocal: true)`, so work
keeps producer-side locality but is stealable.

On a Ryzen 9 5900X (12C/24T), 1 000 000 messages round a 1000-node ring:

| Path | Time |
|---|---|
| Fiber ring (`await IEvent`) | 331 ms |
| ValueTask ring (C# interop path) | 442 ms |

A 480-child fan-out spawned from inside a single fiber runs **12–13x faster than
serial**. Under the previous dedicated-worker scheduler it would have serialised onto
one core, because `Enqueue` always targeted the current worker's own queue and idle
workers could not steal.

Full numbers, methodology and a before/after comparison are in
[BENCHMARKS.md](BENCHMARKS.md).

## Projects

- [Example](Example) - a small client/server demonstrating the library end to end.
- [StressTest](StressTest) - ring, fan-in/fan-out, fiber ring, fan-out scaling and
  the CML combinators (`Choose`, `Wrap`, `WithNack`).
- [Tests](Tests) - regression tests for each fixed correctness bug. Every test has
  been verified to fail when its fix is reverted. Run with
  `dotnet run -c Release --project Tests`.

## TODO

**B7: stale operations accumulate in channel queues.** A losing `choose` branch
leaves its operation in the channel queue, reclaimed only if some later operation
happens to dequeue it. A channel offered in a `choose` but never communicated on
grows without bound, and the operation pool never gets those objects back. Fixing it
needs O(1) removable queue entries (an intrusive linked list) plus `SyncState`
tracking published ops so it can unlink losers. There is a characterisation test that
will fail, deliberately, once this is fixed.

## License

Since I learned of cml from Andy Wingo, and his fantastic guile-fibers, this is licensed under the same license. This is not a derived work, except for possibly the channels which I had a look at long before actually writing anything myself. Most of the algorithms come directly from John Reppy's papers. 

Gnu LGPL v3, to the extent I can actually copyright this. 
