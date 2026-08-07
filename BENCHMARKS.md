# BjoML StressTest benchmarks

Run with:

```
dotnet run -c Release --project StressTest
```

Machine: AMD Ryzen 9 5900X (12C/24T), .NET 10.0.104, Linux 6.18 (Fedora 43).

---

## Baseline — before the IThreadPoolWorkItem / Fiber migration

Commit state: original `Scheduler.cs` with dedicated `Worker` threads +
`BlockingCollection<Action>`, `ProposedChanges/` excluded from compilation.

4 consecutive runs, Release build, milliseconds:

| Benchmark | Messages | run 1 | run 2 | run 3 | run 4 | median |
|---|---|---|---|---|---|---|
| CML Ring (1000 workers x 1000 trips) | 1,000,000 | 403 | 408 | 398 | 402 | **402** |
| SimpleChannel Ring (1000 x 1000) | 1,000,000 | 195 | 196 | 199 | 203 | **198** |
| CML Fan-In/Fan-Out (100 prod x 100 cons x 5000) | 500,000 | 126 | 128 | 128 | 125 | **127** |
| SimpleChannel Fan-In/Fan-Out (100 x 100 x 5000) | 500,000 | 242 | 257 | 217 | 188 | **230** |

Correctness checks in the same run:

- Combinators test result: `HELLO (from chan2)` — correct.
- `Nack fired for chan1?` → `True` — correct.

Notes on noise:

- The three CML numbers are stable to within ~3%.
- `SimpleChannel Fan-In/Fan-Out` is the noisy one (188–257 ms, ~30% spread); it
  is lock-based and sensitive to scheduling. Treat a change there as significant
  only if it moves the median by more than ~30%.

---

## After the IThreadPoolWorkItem / Fiber migration

Dedicated `Worker` threads and `BlockingCollection` replaced by
`ThreadPool.UnsafeQueueUserWorkItem(IThreadPoolWorkItem, preferLocal: true)`, plus
the `Fiber` / `AsyncMethodBuilder` layer. 4 consecutive runs, Release, ms:

| Benchmark | run 1 | run 2 | run 3 | run 4 | median | baseline | change |
|---|---|---|---|---|---|---|---|
| CML Ring (ValueTask path) | 448 | 437 | 484 | 422 | **442** | 402 | +10% slower |
| SimpleChannel Ring | 126 | 142 | 139 | 125 | **132** | 198 | −33% faster |
| CML Fan-In/Fan-Out | 125 | 128 | 126 | 137 | **127** | 127 | unchanged |
| SimpleChannel Fan-In/Fan-Out | 208 | 208 | 216 | 203 | **208** | 230 | −10% (within noise) |
| **Fiber Ring** (new) | 326 | 314 | 336 | 351 | **331** | — | new |
| **Fan-Out** (new) | 121 | 120 | 123 | 119 | **121** | — | new |

### Analysis

**The fiber path is the one that matters, and it is faster than the old baseline.**
`Fiber Ring` (331 ms) does the same 1 000 000 message ring as `CML Ring`, but with
each node an `async Fiber` awaiting `IEvent` directly rather than a `Task` awaiting a
pooled `ValueTask` source. It beats both the new ValueTask ring (442 ms) and the
*old* baseline ring (402 ms), because awaiting an event skips
`CmlValueTaskSource` entirely — no pooled source, no `ValueTask` wrapper, and the
inline-rendezvous case allocates no continuation at all.

**The ValueTask ring regressed ~10%, and this is the expected trade.** The ring is a
single hot chain: exactly one message is in flight, so the old design's
"always enqueue to my own worker's queue" was optimal for it — the whole ring
effectively ran on one thread with perfect cache locality and no contention. The .NET
pool deliberately makes that work *stealable*, so the chain migrates between threads
and pays cache-miss costs. We bought fan-out scalability with ring locality. Note the
ring only touches the scheduler at all when the inline-dispatch budget
(`Scheduler.MaxInlineDepth`, 50) is exhausted; the rest runs inline either way.

**Fan-Out is what the old scheduler could not do at all.** 480 children spawned from
inside a single fiber, 3 000 000 iterations of CPU work each: 121 ms against a
measured serial reference of ~1 550 ms, a **12.3–13.6x speedup** on 12 physical /
24 logical cores. Under the old scheduler `Scheduler.Enqueue` from a worker always
targeted *that same worker's* queue with no stealing, so all 480 children would have
serialised onto one core while the rest sat blocked in `GetConsumingEnumerable()`.
This is bug B6 in the review, and it is the reason for the migration.

**SimpleChannel Ring improved 33%** — it has no CML machinery, so it mostly measures
raw handoff, and the pool's local queues beat a `BlockingCollection` per worker.

Both fan-in/fan-out numbers are unchanged within noise; they were already spread
across many threads, so neither scheduler was the bottleneck.

---

## Experiment: is `MaxInlineDepth` leaving performance on the table?

**Answer: no. More inlining is worse. Do not raise it, and do not build a trampoline
without re-running this.**

Instrumenting `Scheduler.Dispatch` during the fiber ring shows **2 002 000 dispatches
of which only 39 254 (1.96%) bounce to the pool** — almost exactly 1/51, which is what
a depth-50 limit predicts on a linear chain. 98% of continuations already run inline.

Sweeping the limit (2 runs each; counters perturb absolute timings slightly, so read
the trend, not the absolutes):

| `MaxInlineDepth` | bounced | Fiber Ring | CML Ring (ValueTask) |
|---|---|---|---|
| 50 (shipped) | 1.96% | 364, 369 ms | 483, 475 ms |
| 200 | 0.50% | 351, 356 ms | 566, 555 ms |
| 1000 | 0.00% | 454, 429 ms | 688, 647 ms |
| 5000 | 0.00% | 437, 457 ms | 671, 663 ms |

Eliminating the bounce **entirely** (depth ≥ 1000 — the whole 1000-node ring trip in
one recursive chain) makes the fiber ring ~20% *slower* and the ValueTask ring ~40%
slower. The cost of a deep stack — cache and TLB pressure, stack page growth, nested
`try/finally` frames — overwhelms the saving from skipping a queue push/pop long
before the bounces run out.

50 → 200 is within run-to-run noise for the fiber ring (its spread at depth 50 across
all sessions is 314–369 ms) while clearly hurting the ValueTask ring, which pays more
stack per hop because its `ValueTask` continuations also run inline
(`RunContinuationsAsynchronously` is false).

Two conclusions:

- The bounce is not a bottleneck, so the obvious "trampoline" idea — park the
  continuation in a thread-local list at depth 50, unwind, and drain it on the same
  thread, giving 0% bounce with a bounded stack — is chasing at most a few percent.
  That is a poor trade against its real cost: work parked in a thread-local list is
  **not stealable**, which reintroduces B6 in miniature, and it needs careful drain-on-
  exception handling or work is silently stranded on an idle thread.
- `preferLocal: true` is already doing most of the job the trampoline would do. The
  local queue is LIFO, so a bounced continuation is usually popped straight back by
  the same thread; the enqueue is not a thread migration in the common case.

---

# Spawn throughput vs Go

Machine: AMD Ryzen 9 7900 (12C/24T), .NET 10.0.7 (ServerGC), Go 1.26.5, Linux 7.0.

`bench/Bench` and `bench/go` are shaped identically: same counts, same topology,
same work per unit. `bench/Diag` is the subtractive harness used to attribute cost.

## The gap, as measured

Medians of 3 runs, ns/op:

| Benchmark | BjoML before | Go | note |
|---|---|---|---|
| Spawn storm | 321 | 201 | **the real gap, 1.6x** |
| Ping-pong | 236 | 162 | |
| Ring | 128 | 90 | |
| Select/Choose | 214 | 134 | Go's `select`; not composable |
| Fan-out | 113 ms | 120 ms | equal |
| Spawn+send | 661 | 1778 | **not evidence, see below** |

`Spawn+send` is 200 000 senders on ONE unbuffered channel. Go handles that
particularly badly — every blocked sender allocates a `sudog`, links onto
`hchan.sendq` under the channel mutex, and the receiver hands off to exactly one
sender at a time. It is a lock convoy, not a rendezvous measurement, and it should
not be read as a BjoML win.

## Which cost dominates: GC or the queue?

**The queue, overwhelmingly. GC is ~9%.**

The decisive experiment is subtractive: run the same 1e6-unit workload with one
ingredient removed at a time. All rows use one shared completion atomic, padded onto
its own cache line, so the benchmark's own bookkeeping is constant across rows.
Medians of 7 reps:

| row | default GC | GC *entirely* disabled (`DOTNET_GCgen0size=0x20000000`) |
|---|---|---|
| `Parallel.For` chunked, no queue at all | 11 | 10 |
| one pool work item per unit, **zero allocation** | 185 | 204 |
| one pool work item per **32** units | 31 | 30 |
| `Bjo.Spawn` | 216 | 196 |

Three readings, and they all say the same thing:

1. **GC is ~9%.** With gen0 raised to 512 MB the spawn storm does *zero* gen0
   collections and reports `GCpause = 0.0 ms`, and `Bjo.Spawn` improves only
   216 → 196 ns. The GC pause in the default configuration is ~12–38 ms out of
   ~216 ms.
2. **Allocation itself is ~free.** A pre-allocated, zero-allocation work item costs
   the *same* 185 ns as one allocated per unit. The .NET allocator is a pointer bump;
   80 bytes of gen0 garbage is not what is expensive.
3. **How the queue operations are DISTRIBUTED is ~85% of the cost.** Same allocation,
   same atomics, same work — batching 32 units per queue operation takes 185 → 31 ns.

   Note carefully what this is *not* saying. The obvious reading is "fewer queue
   operations = faster", and that reading is wrong: the batch rows above split all
   the way down to leaves of ONE item, so they perform roughly the same number of
   queue operations as the unbatched row, and they are still 6x faster. What changes
   is *who* performs them. Unbatched, one thread pushes a million items into its own
   local queue and 23 threads contend to steal them one at a time under the victim
   queue's foreign lock. Batched, the tree spreads the pushes across every thread's
   own local queue, where push/pop is uncontended and nearly free.

   This distinction matters, because "fewer queue ops" motivates a coarse split floor
   and that turns out to be actively harmful — see the parallelism sweep below.

BjoML's own machinery is only ~30 ns of the 216 (`Bjo.Spawn` 216 vs raw pool 185).
The fiber layer was never the problem.

### Why the .NET pool costs ~185 ns per item here

One producer, 24 consumers, a million tiny items. Every enqueue touches the pool's
thread-request counter; every consumer steals **one item at a time** under the
victim queue's foreign lock. The pool also over-injects threads on a deep backlog of
tiny items — the harness observed 24 → 44 live worker threads on a 24-thread box.

This is precisely where Go's scheduler differs, and it is not a micro-optimisation
detail: Go moves runnable work in **batches** (a local runq spills 128 goroutines to
the global queue at once; a steal takes *half* the victim's queue). .NET moves one
work item per operation. Nothing about `preferLocal` changes this — `true` and
`false` measured identically (185 vs 185).

## The fix: batch the spawns, not the continuations

`SpawnBatch` parks freshly spawned fibers in a thread-local buffer and hands the pool
a *batch* work item every 64 fibers. `Scheduler.EnqueueSpawn` is deliberately
separate from `Scheduler.Enqueue`: a rendezvous continuation wants to run on the
thread that completed the match (locality, and there is only ever one of it), while a
spawn has no locality to preserve and arrives in bursts.

Three properties make it safe, and all three are load-bearing:

- **Burst detection.** The first 8 spawns of a run go straight to the pool, so
  "spawn a child and await it" is byte-for-byte the old path and cannot regress.
- **Flush on yield.** Every work-item boundary flushes, so a fiber that suspends or
  returns publishes whatever it queued.
- **Watchdog.** A 1 ms timer flushes buffers whose owning thread has stopped
  cooperating. This is *not* belt-and-braces: without it, a thread that spawns >8
  fibers and then blocks on a non-BjoML primitive strands them **forever**. Deleting
  the `EnsureWatchdog()` call turns 6 tests in `SpawnBatchTests` into hangs.

The batch does not run inline as a block — that would trade queue cost for lost
parallelism, which is bug B6 in a new hat.

## Does the split policy have to be a fixed heuristic? No, and it must not be

The first version of `SpawnBatchItem` split recursively down to a fixed floor of 8
and ran the leaf inline, justified by "splitting to 1 would reintroduce the per-item
queue cost". **That justification was wrong, and the constant was a 4.6x bug.**

A fixed floor decides how much parallelism a batch can reach *before any thread has
run*: a batch of N becomes ceil(N / floor) chunks and each chunk runs sequentially on
one thread. With floor 8, sixteen CPU-bound fibers become two chunks — two-way
parallelism on a 24-core box.

The throughput harness cannot see this. With a million trivial fibers there is so
much work that every policy fills every core. It only shows up when N is small but
above the burst threshold and each fiber is expensive, which is why the original
480-child fan-out benchmark missed it entirely, and why the original test — which
asserted only that more than one thread was touched — passed while it was happening.

`bench/Diag --mode fanout` sweeps N CPU-bound fibers against a calibrated serial
reference and measures achieved speedup (ideal = `min(N, cores)`):

| N | ideal | unbatched | floor=8 | floor=4 | floor=2 | floor=1 | adaptive |
|---|---|---|---|---|---|---|---|
| 12 | 12.0 | 14.9 | **6.8** | 7.0 | 7.5 | 10.9 | 13.0 |
| 16 | 16.0 | 19.8 | **4.3** | 8.8 | 11.9 | 17.5 | 17.3 |
| 20 | 20.0 | 19.7 | **6.9** | 11.1 | 14.6 | 20.5 | 20.0 |
| 24 | 24.0 | 19.5 | **6.3** | 11.1 | 14.2 | 20.5 | 24.8 |
| 48 | 24.0 | 25.6 | 14.6 | 18.5 | 22.3 | 22.6 | 21.9 |
| 256 | 24.0 | 29.2 | 24.5 | 27.4 | 28.3 | 28.7 | 28.9 |

And the throughput cross-check on 1e6 trivial fibers — the thing the floor was
supposed to protect:

| policy | ns/op | B/op | GC pause |
|---|---|---|---|
| unbatched | 289 | 80 | 5.6 ms |
| tree floor=8 | 34 | 92 | 2.3 ms |
| tree floor=2 | 33 | 104 | 0.9 ms |
| tree floor=1 | 35 | 120 | 42.4 ms |
| **adaptive** | **30** | **89** | **0.5 ms** |

**Every batched policy has identical throughput.** The floor was protecting nothing
and costing up to 4.6x in parallelism.

`SpawnBatchMode.Adaptive` is therefore the default. It publishes the batch as one
shared range drained by an atomic cursor: threads take chunks until it is empty, and
a thread that finds work remaining recruits one more thread (capped at one per core,
self-limiting once the cursor is exhausted). Chunk size follows guided
self-scheduling — `remaining / (2 * workers)`, clamped — so it takes big bites while
there is plenty left and small bites near the end, which is what stops the tail
landing on a single thread. Achieved parallelism is then simply however many threads
turned up: one thread drains it correctly, twenty-four split it twenty-four ways, and
no constant had to predict which.

It is also the cheapest policy on allocation (89 B/op) because it re-enqueues
*itself* rather than allocating a new split item per level.

Ranges handed out by the cursor are disjoint, so `RunRange` can null out array slots
without synchronisation.

The regression test is `"a batch of 16 CPU-bound fibers actually runs in parallel"`,
which asserts achieved speedup rather than thread count, and fails at ~4.3x if the
tree/floor=8 policy is restored.

Note the adaptive chunk arithmetic is inert at the shipped capacity on a large box:
`remaining / (2 * procs)` is `64 / 48 = 1` on 24 cores, so adaptive degenerates to a
shared cursor handing out one fiber at a time. That is why it allocates least and
performs best, but it also means the chunking logic is **not exercised by any of the
numbers above**. On a 4-core machine the same expression gives 8, and chunks of 8
reintroduce head-of-line blocking *within* a chunk: a claimed chunk cannot be taken
by another thread, so a fiber that blocks its thread holds up its chunk-mates. That
path is untested.

## Watchdog fences

The watchdog stops itself when a sweep finds nothing pending and re-arms on the next
buffered spawn. That handshake is a store/load pair:

```
Add:   store _count = 1            ...  load  s_watchdogState
Sweep: store s_watchdogState = 0   ...  load  _count
```

x86 permits StoreLoad reordering, and neither `Volatile.Write` (release) nor
`Volatile.Read` (acquire) prevents it. With plain volatile accesses both sides can
read stale values, both conclude the other party will handle it, and the buffered
fibers are dropped — permanently, if the owning thread never spawns again. This is
the deadlock the watchdog exists to prevent, reappearing inside the watchdog.

Both sides therefore use a full fence: `Interlocked.CompareExchange` in
`EnsureWatchdog` (which is why there is no `Volatile.Read` fast path in front of it)
and `Interlocked.Exchange` in `Sweep`, followed by a re-scan after the stop is
published. `Add` arms only on the empty-to-non-empty transition, so the fence is paid
once per batch rather than once per spawn.

**These fences are reasoned, not test-covered.** Removing the re-scan in `Sweep`
still passes the stop/re-arm stress test three runs out of three; the window is a few
instructions wide. Detecting it would need fault injection — a stall hook inside
`Sweep` between the scan and the stop — which is not built.

This is also why the earlier "do not build a trampoline" conclusion is *not*
contradicted. That analysis was about rendezvous **continuations**, where the
argument still holds: there is one continuation, it has locality worth keeping, and
parking it is pure loss. Spawns have the opposite economics.

## Result

`bench/Diag`, medians of 7 reps:

| | ns/op |
|---|---|
| `Bjo.Spawn` unbatched | 278 |
| `Bjo.Spawn` batched | **37** |

End-to-end, `bench/Bench` vs Go, medians of 3:

| Benchmark | before | after | Go | vs Go |
|---|---|---|---|---|
| **Spawn storm** | 321 | **92** | 201 | **2.2x faster** |
| Ping-pong | 236 | 206 | 162 | 1.3x slower |
| Ring | 128 | 102 | 90 | 1.1x slower |
| Select/Choose | 214 | 197 | 134 | 1.5x slower |
| Fan-out | 113 ms | 118 ms | 120 ms | equal |

Fan-out is unchanged (11.3x speedup on 12 physical cores), confirming the split
preserves parallelism. Cost: +12 B/op (80 → 92) for the batch arrays and split items,
and GC pause *drops* (12 ms → 0.4 ms) because fibers no longer pile up as an 80 MB
live backlog waiting to be dequeued one at a time.

## SimpleChannel: the like-for-like comparison against a Go `chan`

`Channel<T>` is a composable CML event — it pays for `choose`, `withNack` and the
`SyncState` protocol. A Go `chan` has no equivalent capability, so those rows flatter
Go. `SimpleChannel` is the honest comparison: plain unbuffered point-to-point
rendezvous, no composition.

| Benchmark | `Channel<T>` | `SimpleChannel` | Go `chan` |
|---|---|---|---|
| Ping-pong | 206 | **178** | 162 |
| Ring | 102 | 140 | 90 |
| Spawn+send | 630 | **491** | 1778 |

There is deliberately no `SimpleChannel` row for Select/Choose: a `SimpleChannel`
cannot be an argument to `choose`. That is exactly the capability `Channel<T>` is
charging ~60 ns for.

`Ring simple` is the noisy one (69–144 ms across runs) and is *not* reliably faster
than the CML ring; the ring is a single hot chain where the CML path's inline
dispatch already avoids most scheduler traffic.

## Hopac comparison

`bench/hopac` is the same six benchmarks written against Hopac 0.5.1, shaped
identically. Hopac is the reference point that matters most: it is also CML, also on
.NET, also on a work-stealing scheduler, and it has had years of tuning. Go tells us
what a mature runtime with language support achieves; **Hopac tells us what the same
idea costs on the same platform**, which is what BjoML should actually be judged
against.

Medians of 3, ns/op, with bytes allocated per operation:

| Benchmark | BjoML | B/op | Hopac | B/op | Go |
|---|---|---|---|---|---|
| Spawn storm (from a foreign thread) | **110** | 89 | 2163 | 72 | — |
| Spawn storm (from inside the runtime) | **100** | 89 | 212 | 184 | 201 |
| Spawn+send | **590** | 377 | 1076 | 304 | 1778 |
| Ping-pong | 193 | **0** | 191 | 672 | 162 |
| Ring | **62** | **0** | 76 | 296 | 90 |
| Select/Choose | 211 | 40 | **141** | 528 | 134 |
| Fan-out | 113 ms | — | **112 ms** | — | 120 ms |

### A fairness bug in the earlier suite, now fixed

The spawn storm had been measuring different things in different languages. In Go,
`main` *is* a goroutine, so `go func()` pushes onto a local run queue and never
touches a shared structure. In the C# and F# versions the producer was an ordinary
thread, so every spawn crossed a shared queue. Those are not the same measurement,
and Go's number was quietly getting the easy path.

`Spawn storm inside` is the comparable row, and adding it changed the conclusion.

### Hopac has a 10x cliff on foreign-thread spawns; BjoML does not

**2163 ns/op from a foreign thread against 212 from inside a job.** Hopac's `queue`
from outside the runtime takes the global work stack's `SpinlockTTAS`, once per
spawn, while 24 workers spin on the same lock trying to pull work out. Hopac's own
source comments on that spinlock predict this exactly: "on every change of owner this
spinlock implementation requires Omega(n) cache line transfers ... and is thus
inherently unscalable". It is fine in normal Hopac use, because Hopac code spawns
from inside jobs onto an unsynchronised worker-local stack.

BjoML has no such cliff — 110 foreign against 100 inside — and the reason is
`SpawnBatch`. This is the strongest evidence yet for keeping it. It had been
described in this document as a workaround for the .NET thread pool; it is better
understood as **the fix for a cliff that the mature comparable runtime still has**.
An embedded language runtime is called from foreign threads constantly (the entry
point, `Task` continuations, host callbacks), so this is not a benchmark artifact.

Independent confirmation: the `trial/own-workers` branch reproduced Hopac's cliff
almost exactly. Hand-written workers with a global queue measured 299 ns/op for the
same storm without batching, and 109 with it.

### Where Hopac is better, and it is worth taking seriously

**Select/Choose: 141 vs 211.** Hopac is 33% faster at the operation that is the whole
point of CML, while allocating 528 B/op against BjoML's 40. So it is not winning by
being cheap — its `Alt` machinery is simply better than BjoML's `SyncState`. That is
the clearest optimisation target in the codebase.

### Where BjoML is better

**Allocation, by an order of magnitude.** Ping-pong 0 B/op against 672, ring 0 against
296, select 40 against 528. Hopac's `job` computation expression allocates
continuation objects per bind; BjoML's compiled async state machine is allocated once
per fiber and then reused across every suspension. This is the structural advantage
of building on `IAsyncStateMachine`, and it shows up as lower GC pressure rather than
lower latency — the two runtimes are within noise of each other on ping-pong wall
time despite the 672:0 allocation ratio.

**Spawning, roughly 2x**: 100 vs 212 inside, and 20x foreign.

**Ring, 62 vs 76**, with zero allocation.

### Caveats

- Hopac's `Spawn+send` is as noisy as Go's (695, 3334, 1076 across three runs) and
  for the same reason: 200 000 senders convoying on one unbuffered channel. Treat
  that row as non-evidence for all three runtimes.
- Hopac 0.5.1 predates .NET 10 and is running on a runtime it was never tuned for.
- Only the C# suite reports `SimpleChannel`; there is no Hopac equivalent because
  Hopac's `Ch` is always a composable `Alt`.

## What is left on the table

- **~30 ns of BjoML overhead per spawn** (216 vs 185 raw pool, pre-batching).
  `FiberCore<T>` is 80 B and carries four fields (`_runner`, `_spawnBody`,
  `_spawnState`, `_spawnInherited`) that are dead the moment the fiber starts.
- **Ping-pong and Ring** are handoff-latency bound, not spawn bound. Batching does
  nothing for them; they are already within 1.1–1.3x of Go.
- **Select/Choose at 1.5x** is the widest remaining gap and is a `SyncState`
  question (40 B/op allocated per sync), not a scheduler one.

