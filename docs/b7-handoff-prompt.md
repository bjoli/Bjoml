# Handoff prompt: fix B7 (stale operations in channel queues)

Copy everything below the line into a fresh session.

---

You are working on **BjoML**, a Concurrent ML runtime in C# at
`/home/linis/Programmering/Bjoml`. It is a compilation target for a Scheme-like
language ("Bjolang"); it is not meant to be used directly by humans from C#.

Your task is to fix **B7: stale operations accumulate without bound in channel
queues.** This is the last known structural bug. Everything else in the review has
been fixed and has regression tests.

Read `docs/design.md` first — it describes the runtime as it stands and lists B7 in
its "Known issues" section.

## Build, test, benchmark

```
dotnet build -c Release
dotnet run -c Release --project Tests        # 29 tests, all currently pass
dotnet run -c Release --project StressTest   # benchmarks
```

Machine of record for the numbers below: AMD Ryzen 9 5900X (12C/24T), .NET 10.0.104,
Linux. `BENCHMARKS.md` has the full before/after history.

## The bug

`Channel<T>` holds two `ConcurrentQueue`s: parked senders (`_putq`) and parked
receivers (`_getq`). An operation that cannot match immediately is enqueued holding a
reference to its sync block's `SyncState`.

In a `choose`, **every branch publishes its own operation, all sharing one
`SyncState`.** When one branch wins, that `SyncState` goes to Synchronized and every
other branch's queued operation is instantly dead — any thread that later tries to
pair it sees `IsSynchronized`, or fails `TrySync`.

Nothing removes them. They are reclaimed only lazily, when some *later* operation
happens to dequeue one and notice it is dead. So reclamation is driven by future
traffic on that channel, while the garbage is produced by activity on a *different*
channel. A channel that goes quiet never drains.

**Trigger:** a losing `choose` branch on a channel that then goes quiet. A plain
non-`choose` send/receive never leaks — it either matches or stays legitimately live.
Only `choose` (and `withNack`, which is choose-shaped) creates abandonable branches.

Real shapes: `choose(request, shutdown)` in a server loop; a `choose` over many peers
where only a few are chatty; channel-based timeouts that usually don't fire.

Measured: one dead operation per losing branch, permanently. 500 iterations of
`choose(busy, idle)` leaves ≥500 stranded operations on `idle`.

Note `ChooseEvent` already stops publishing once the state is synchronized, so
`choose(Always, recv)` never publishes `recv`. But `choose(recv, Always)` publishes
`recv` first and *does* strand it. Ordering changes the symptom; it is not a fix.

## What it costs

1. Unbounded memory growth, proportional to iterations.
2. Retention beyond the operation object: each dead op holds its continuation
   delegate → the `EventAwaiter` → **the result value the winning branch delivered**.
   If you pass messages, each stranded op pins one message. (The fiber state machine
   is *not* retained; the awaiter's continuation slot is replaced by a static
   sentinel on completion.)
3. It defeats the object pool entirely. Ops are never returned, so `OpPool<T>`
   allocates forever and the advertised zero-allocation steady state silently stops
   holding.
4. Latency: if the quiet channel does eventually get traffic, the first operation
   walks and discards the entire backlog — an O(n) stall.

## The fix

Two parts. You need both; neither works alone.

**Part 1 — queue entries removable in O(1).** `ConcurrentQueue` cannot remove an
interior element without dequeuing everything ahead of it and re-enqueuing (O(n) and
it reorders). Replace the two queues with an intrusive doubly-linked list: each
`Operation` becomes a node with prev/next plus a back-reference to the list holding
it, so unlinking is pointer surgery. Do not attempt a lock-free doubly-linked list;
guard each channel with a lock. This is what Reppy's implementation and Guile-fibers
both do — it is the standard answer, not a novel design.

**Part 2 — `SyncState` must know what it published.** It currently has no idea which
operations exist on its behalf. Accumulate them as branches publish. Then
`MarkSynchronized` — which already runs exactly once at commit, and already walks a
list to fire nacks — also walks the operation list and unlinks every one, winner
included. Reclamation becomes "immediately, by the committing thread" instead of
"eventually, if someone happens to look".

### Races the fix must handle

- **Publish concurrent with commit.** A branch may still be publishing when another
  branch commits — a `Promise` waiter on another thread can commit at any moment. So
  registering an operation with the `SyncState` must be synchronized, and an
  operation registered *after* the state is already Synchronized must be unlinked by
  the registering thread itself, because the commit already walked the list and will
  not come back. `SyncState.RegisterNack` already handles exactly this race shape —
  copy its structure.
- **Idempotent unlink** against an operation another thread concurrently dequeued and
  matched. The node needs a linked/unlinked flag checked under the channel's lock.
- **Single ownership of pool return.** Both a matching thread and a committing thread
  can now finish with the same operation. Exactly one must return it to `OpPool<T>`,
  or you get double-return and aliasing.

### Why the cheap alternatives don't work

The same class of leak existed for `Promise` waiters and was fixed cheaply — an
`IPromiseWaiter.IsAbandoned` test plus pruning whenever a new waiter registers (see
`Promise.cs`). That works because all registrations funnel through one object, giving
a natural moment to sweep.

It does not transfer to channels, for the reason that makes B7 nasty: the
pathological channel receives no further registrations, so a registration-triggered
sweep never runs. Any threshold or amortised sweep driven from the channel side has
the same hole. Driving a sweep from the committing side requires knowing which
channels to sweep — which is Part 2 — and once you have Part 2, a sweep is strictly
worse than unlinking the node you already hold a handle on.

## Architecture facts you must not break

These are non-obvious and several are load-bearing.

- **`SyncState` protocol.** `W`aiting → `C`laimed → `S`ynchronized.
  `TryClaim` is W→C on your own block. `TrySync` is W→S on the *opposing* block.
  `TryCommit` is W→C→S and **spins past a transient C** — required so a promise
  landing on another thread is not silently dropped (bug B4). `MarkSynchronized`
  requires the caller to already own the state; it stomps otherwise (B8).
- **Event ids are a tree, not a flat set.** `choose` mints children under the
  incoming id, `wrap`/`guard` pass it through, `withNack` attaches to an interior
  node, and a nack fires only if its id does not cover the winner (B1). Root is 0.
- **The self-match guard (B3).** A sync block must never pair with itself, or
  `choose(send ch, recv ch)` livelocks a thread forever with no exception. Ops
  belonging to our own `SyncState` are skipped, collected, and re-queued **in a
  `finally` on every exit path including early returns**. If you restructure the
  matching loops, preserve this exactly — dropping a deferred op silently deletes a
  live branch and the block hangs.
- **Inline dispatch.** `Scheduler.Dispatch` runs continuations on the completing
  thread while `InlineDepth < MaxInlineDepth` (50). That constant is the only bound
  on stack growth for a chain of rendezvous. Do not remove it.
- **ExecutionContext is never captured or restored anywhere.** Unsafe enqueue,
  `AwaitUnsafeOnCompleted`, `ConfigureAwait(false)` + `UnsafeOnCompleted` instead of
  `ContinueWith`, `FlowExecutionContext` stripped in `CmlValueTaskSource`. This is
  deliberate and asserted by tests. Keep it that way.
- **Operations are pooled** per element type (`OpPool<T>`), implementing
  `IResettable`. Read everything off an op before returning it.
- **`EventAwaiter` publishes the sync in its constructor** and only then reports
  `IsCompleted`. A rendezvous landing in that window completes the await without ever
  creating a continuation, so **the fiber never suspends at all** and runs on inline.
  This is correct fast-path behaviour and it has consequences for tests (below).

## Verification

**All 29 existing tests must still pass.** They cover B1–B4, the fiber context shim,
and the no-ExecutionContext guarantee. Every one has been verified to fail when its
fix is reverted — do not weaken them.

**Invert the characterisation test.** `CmlTests.StaleOpsAccumulate` currently asserts
the *buggy* behaviour on purpose (≥1 stranded op per losing branch) and will fail the
moment you fix B7. That failure is the signal. Rewrite it to assert the queue stays
bounded — ideally that it returns to zero after the losing branches commit — and add
a case where the quiet channel is *never* touched again.

**Verify your fix by sabotage,** the way the rest of the suite was validated: revert
your change, confirm the new test fails, restore it, confirm it passes. A regression
test that has never failed proves nothing.

**Benchmark before and after.** Current medians to beat or match:

| Benchmark | Current |
|---|---|
| Fiber Ring (1000 nodes × 1000 trips) | 331 ms |
| CML Ring (ValueTask interop path) | 442 ms |
| SimpleChannel Ring | 132 ms |
| CML Fan-In/Fan-Out | 127 ms |
| SimpleChannel Fan-In/Fan-Out | 208 ms |
| Fan-Out (480 children) | 121 ms, 12–13x vs serial |

Expect a swing in **both** directions, and measure rather than guess. Hypothesis, not
fact: a per-channel lock is plausibly *cheaper* than the current CAS-retry-yield loop
under fan-in/fan-out contention, and plausibly *worse* on the uncontended ring, where
it adds a lock acquire/release per operation to a path that currently has neither.

**If the ring regresses badly, stop and report rather than shipping it.** This is
explicitly the user's preference. A correct-but-slow channel core is a decision for
them to make, not for you to make silently.

## Bonus available from this refactor

With an intrusive list you traverse and unlink in place instead of the current
dequeue / inspect / re-enqueue-at-tail dance. That removes the FIFO violation
currently documented in `docs/design.md` ("channel ordering is not guaranteed"), and
most of the `Thread.Yield()` livelock avoidance stops being necessary, because you
are no longer putting contended operations back at the wrong end. If you get FIFO
back, update the docs — the language manual currently disclaims ordering.

## Traps that already cost time in this codebase

- **Do not use a handshake over a second channel to prove a fiber has suspended.**
  The matching loop dispatches the *partner's* continuation inline from inside the
  awaiter's constructor, so the partner observes the rendezvous before the fiber has
  advanced to its next await. Worse, the fiber may then never suspend at all. Tests
  use `Channel<T>.PendingReceiveCount` (internal, exposed via `InternalsVisibleTo`)
  plus a tolerance for the fiber completing inline. See
  `FiberTests.AwaitSuspension` and the comment on `AsyncLocalDoesNotFlow`.
- **A test that routes through BjoML's own awaiters cannot detect ExecutionContext
  flow**, because those awaiters never capture EC regardless of what the builder
  does. Testing the builder requires an awaiter that respects the distinction, e.g.
  `await Task.Yield()`.

## Definition of done

- B7 fixed: dead operations are unlinked at commit, including on channels that are
  never touched again.
- Characterisation test inverted and passing; new coverage for the never-touched-
  again case.
- All 29 existing tests still pass, unweakened.
- Sabotage-verified.
- `BENCHMARKS.md` updated with a new section and honest analysis of any regression.
- `docs/design.md` updated: move B7 out of "Known issues", and revise the channel
  ordering note if FIFO is now guaranteed.
