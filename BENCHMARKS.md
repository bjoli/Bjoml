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

