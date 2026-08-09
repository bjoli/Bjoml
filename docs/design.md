# BjoML runtime design

BjoML is a Concurrent ML implementation for a Scheme-like language that compiles to
C#. This document describes the runtime after the `IThreadPoolWorkItem` + `Fiber`
migration, and records what is deliberately *not* done yet.

---

## 1. Scheduling

All work goes through
`ThreadPool.UnsafeQueueUserWorkItem(IThreadPoolWorkItem, preferLocal: true)`.

There are no dedicated worker threads. The previous design gave each worker a
`BlockingCollection` and had `Enqueue` always target the *current* worker's queue,
which meant a fiber spawning N children pinned all N to one core with no way for idle
workers to steal them. The .NET pool already implements local queues with stealing,
so `preferLocal: true` keeps the producer-side locality while leaving the work
stealable.

Measured effect: a 480-child fan-out now runs 12–13x faster than serial on 12
physical cores. See `BENCHMARKS.md`. The cost is ~10% on the single-hot-chain ring
benchmark, where the old "never steal" behaviour was accidentally optimal.

### Inline dispatch

`Scheduler.Dispatch` runs a continuation on the completing thread when
`InlineDepth < Scheduler.MaxInlineDepth` (50), otherwise it queues. This is the fast
path and the common case: the thread that completes a rendezvous simply keeps going.

**`MaxInlineDepth` is load-bearing.** It is the only bound on stack growth for a
chain of continuations that each complete another rendezvous.

### ExecutionContext is never flowed

Nothing in the runtime captures or restores an `ExecutionContext`:

| Path | Mechanism |
|---|---|
| scheduler enqueue | `UnsafeQueueUserWorkItem` |
| fiber suspension | builder calls `awaiter.UnsafeOnCompleted` |
| `Task` bridging | `ConfigureAwait(false)` + `UnsafeOnCompleted`, never `ContinueWith` |
| `ValueTask` interop | `CmlValueTaskSource` strips `FlowExecutionContext` |

This is what makes it safe to enqueue work unsafely everywhere. The hosted language
carries its own dynamic environment (below), so flowing EC would cost an allocation
per await *and* layer C#'s ambient state on top of the fiber's own.

**Consequence:** `AsyncLocal` values set by C# libraries — `Activity`/OpenTelemetry
spans, some logging scopes — do not survive a BjoML await. If you want tracing, put
the span inside your own context object. This is asserted by the
`AsyncLocal does not survive a C# Task await` regression test.

---

## 2. The dynamic environment shim

`FiberContext.Current` is a single `[ThreadStatic] object?`. BjoML knows nothing
about the payload; the language owns it, so installing a context is one pointer
store.

The shim lives in the *builder*, because that is the only place that sees every
suspension point of a fiber:

- **at suspend** — `FiberCore.GetMoveNextAction` re-captures `FiberContext.Current`
  into the state machine box. Re-capturing every time (not just at box creation) is
  what makes a `(parameterize ...)` that spans an await work.
- **at resume** — `FiberStateMachineBox.Run` saves the ambient context, installs the
  fiber's, calls `MoveNext`, and restores in a `finally`.

The save/restore is not optional. Continuations run inline on whichever thread
completed the rendezvous, and that thread may be several frames deep inside a
*different* fiber. A resuming fiber borrows the thread and must hand it back exactly
as it found it.

`(parameterize ...)` compiles to `FiberContext.Push(...)` / `Dispose`.

**Limitation:** the context is only reinstated at *fiber* suspension points. A bare
callback handed to the scheduler from outside a fiber — a nack action, a promise
waiter — runs with whatever context the borrowed thread had. Such callbacks must
therefore never run user code; they should only wake a fiber. `TaskInterop.Cancellable`
respects this (its nack callback only cancels a token).

---

## 3. Fibers, promises and events

| Type | What it is | Language surface |
|---|---|---|
| `Fiber` / `Fiber<T>` | return type of a compiled bjoroutine; a compiler artifact | not first-class |
| `Promise<T>` | write-once cell that is **also** a persistent `IEvent` | first-class; what `spawn` returns |
| `IEvent<T>` | a CML event | first-class; what `sync` takes |

`Bjo.Spawn` returns a `Promise<T>`, not a `Fiber<T>`. That is the whole point: a
promise is composable with `choose`, which a `Task` can never be.

```csharp
var r = await Cml.Choose(
    Cml.Wrap(p.Join(),        x => "done"),
    Cml.Wrap(abort.Receive(), x => "aborted"));
```

`(sync ev)` compiles to `await ev` via `EventAwaitExtensions.GetAwaiter`. Nothing
else in the language suspends, which is what makes CML reasoning work: between two
syncs a fiber is atomic with respect to every other fiber.

### Errors are values

Events must never throw. An exception raised in an event continuation escapes into a
channel's matching loop and is swallowed by the scheduler's catch-all, leaving the
sync block hung forever. So failures travel as `Result<T>` and are only converted
back to exceptions inside a fiber, in an awaiter's `GetResult()`, where the state
machine turns them into `SetException`.

| Location | Throwing here is |
|---|---|
| inside a bjoroutine body | correct |
| inside an awaiter's `GetResult()` | correct |
| inside an `IEvent` continuation | **wrong** |
| inside a `Wrap` mapper | **wrong** (mappers run in the continuation) |
| inside a nack action | **wrong** |

### Task interop

`TaskInterop.Cancellable` is the only form that should be exposed for use in
`choose`. A plain `Task` is already running and cannot be withdrawn, so if a
sibling branch wins, an uncancellable task keeps burning a socket. `Cancellable`
wires a `withNack` to a `CancellationTokenSource` so losing the choose actually tears
the work down.

Both `Promise` and `Cancellable` events are **persistent**: once completed, syncing
again succeeds immediately with the same value. A completed promise in a `choose`
loop therefore wins every iteration and starves its siblings — the same trap as
`Cml.Always`. Document this for language users.

---

## 4. Bugs fixed in this pass

Each has a regression test in `Tests/`, and each test has been verified to fail when
the corresponding fix is reverted.

- **B1** — nack fired when the winner was a nested `choose` under the same
  `withNack`. Event ids are now a *tree*: `choose` mints children under the incoming
  id, `wrap`/`guard` pass it through, and a nack fires only if its id does not cover
  the winner.
- **B2** — two `withNack`s could share an id and the inner one silently overwrote the
  outer one's nack. Nacks are now a list, not a dictionary keyed by id.
- **B3** — a sync block could match *itself*, livelocking a whole thread forever with
  no exception and no diagnostic. `(choose (send ch v) (recv ch))` is legal CML and
  hung the process. Ops belonging to our own `SyncState` are now skipped and
  re-queued in a `finally` on every exit path.
- **B4** — `TryClaim` treated a transient `C` as "already lost", silently dropping
  completions that arrive from another thread. Added `SyncState.TryCommit`, which
  spins past `C` (safe: `C` is never held across user code).
- **B5** — `Scheduler.Start()` raced and could NRE on a partially-published array.
  Gone with the dedicated workers.
- **B6** — no work stealing. Gone with the dedicated workers.
- **B7** — a losing `choose` branch left its `PutOp`/`GetOp` parked in the channel,
  reclaimed only if some *later* operation happened to walk past it. A channel offered
  in a `choose` that then went quiet grew without bound: measured at exactly one
  stranded op per losing branch, 500 out of 500. See below.
- **B8** — `MarkSynchronized` was a public method that would stomp another thread's
  claim. Precondition now documented; self-committing events go through `TryCommit`.
- **B9** — the `ValueTask` path flowed `ExecutionContext`. Flag now stripped.
- **B10** — root event id is now `0` and reserved; dead `Operation` helpers removed;
  operation pools are per-`T` statics rather than per-`Channel` instances;
  `withNack` no longer allocates a `Channel<Unit>` per publish.

Two bugs in the *proposed* code were also fixed: `TaskInterop.Cancellable` could not
compile (generic inference through an async lambda) and leaked its
`CancellationTokenSource` whenever the branch **won**; and `PromiseEvent` registered
waiters that were never removed when their branch lost.

### B7 in detail — cleanup must be driven from the committing side

The half of the fix that reads as a rewrite was already done, for unrelated reasons:
the channel had by then moved from `ConcurrentQueue` to intrusive lists under a
per-channel lock, and already had `CleanTakers`/`CleanGivers` to unlink and recycle
synchronized entries. Those were only ever called from the test-only `Pending*Count`
properties, so nothing in production ever ran them.

What was missing was the trigger. The channels that leak are precisely the *idle*
ones, so no scheme driven by channel activity can ever reach them — there is no
subsequent activity. The committing block is the only party that knows those branches
just died, so it has to drive the cleanup:

- `SyncState.TryRegisterSink` records each channel a block parks in, and returns false
  if the block has already committed, in which case the caller must not park. This
  replaces a bare `IsSynchronized` check and closes the race in it: test and
  registration now happen under one lock.
- `MarkSynchronized` calls `NoteDeadEntry()` on each registered channel.

Three things are load-bearing:

- **Store the channel, not the operation.** A matcher can unlink and recycle an op at
  any moment, so a stored op reference may already belong to somebody else by the time
  we look at it. A redundant clean is a short walk; a missed one leaks forever.
- **Clean outside the state lock.** Parking takes the channel lock and then the state
  lock (via `TryRegisterSink`). Holding the state lock while reaching for a channel
  lock would invert that order and deadlock, so `MarkSynchronized` collects the sinks
  under its lock, releases, and only then cleans. The one nesting in the system stays
  channel → state, with no cycle.
- **One field, not three.** `SyncState` is allocated per `Cml.Sync`, so each reference
  field added to it lands straight in the per-op allocation figure. Separate slots plus
  an overflow list measured +16 and +24 B/op on Select/Choose; packing into a single
  `object?` that holds either the sink or a `List` costs +8. The rare overflow list
  never showed up at all — it is the field count that is expensive.

A note on the old test. `characterise: losing branches accumulate in an idle channel`
could never have detected this: it read `PendingReceiveCount`, which calls
`CleanTakers()` *before* counting, so reading it performed the very reclamation the
test was meant to prove happened on its own. It passed with the bug present. The
replacement tests read `RawPendingReceiveCount`, which does not clean, and have been
verified to fail with the fix reverted — 500 of 500 stranded, 200 of 200 for the
`withNack` shape.

---

## 5. Known issues

### Channel ordering is not guaranteed

A contended operation is re-queued at the **tail**, so FIFO is not preserved and a
sender can be starved under sustained contention. Worth stating in the language
manual so the runtime is not held to FIFO later.

### `ValueTask` discard leak

`CmlValueTaskSource` is returned to its pool by `GetResult`. A `ValueTask` that is
created and never awaited is never recycled. In the language, `(put! ch v)` in
statement position looks exactly like a discard, so the compiler must always await
it — or better, use the event/awaiter path, which has no pooled source at all.

### Blocking bridge

`Bjo.RunToCompletion` blocks the calling thread. Do not call it from a thread-pool
thread: the fiber needs pool threads to make progress and you are holding one hostage.
